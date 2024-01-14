#include "sql/parallel_query/worker.h"

#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/mysqld.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/sql_lex.h"

// A helper function of worker thread enter entry

namespace pq {
 constexpr uint message_queue_ring_size = 65536;

/**
  Class has a instance of THD, needs call destroy if allocated in MEM_ROOT
*/
class LocalWorker : public Worker {
 public:
  LocalWorker(uint d, THD *thd,  PartialPlan *plan,
              mysql_mutex_t *state_lock, mysql_cond_t *state_cond);
  /// The receiver row channel created inside of worker, @param comm_event
  /// is for receiver waiting.
  bool Init(comm::Event *comm_event) override;
  bool Start() override;
  void Terminate() override;
  Diagnostics_area *stmt_da(bool finished_collect, ha_rows *found_rows,
                            ha_rows *examined_rows) override;
  std::string *QueryPlanTimingData() override {
    auto *data = m_executor.QueryPlanTimingData();
    // No data if the worker got killed in plan preparing stage or starts
    // failed.
    assert(data || m_terminate_requested || IsStartFailed());
    return data;
  }
  void CollectStatusVars(THD *target_thd) override;

  void ThreadMainEntry();

 protected:
   State state(bool need_state_lock = true) const override;
  /// Set worker state to @param state and broadcast "state cond" if
  /// State::Finished.
  void SetState(State state) override;

 private:
  /// Cleanup resources in this class allocated in m_thd. Those must be released
  /// before its mem_root clears.
  void CleanupThdResources();

  THD *m_leader_thd;
  THD m_thd;  // Current worker's THD

  /// Communication facilities with leader
  comm::RowChannel *m_sender_channel{nullptr};
  comm::RowExchange m_sender_exchange;

  PartialExecutor m_executor;

  State m_state{State::None};
  mysql_mutex_t *m_state_lock;
  mysql_cond_t *m_state_cond;
  bool m_terminate_requested{false};
};

Worker::~Worker() { destroy(m_receiver_channel); }

LocalWorker::LocalWorker(uint id, THD *thd, PartialPlan *plan,
                         mysql_mutex_t *state_lock, mysql_cond_t *state_cond)
    : Worker(id),
      m_leader_thd(thd),
      m_thd(true, m_leader_thd),
      m_executor(&m_thd, plan, [this]() { SetState(State::Cleaning); }),
      m_state_lock(state_lock),
      m_state_cond(state_cond) {}

bool LocalWorker::Init(comm::Event *comm_event) {
#if defined(ENABLED_DEBUG_SYNC)
  debug_sync_set_eval_id(&m_thd, m_id);
  debug_sync_clone_actions(&m_thd, m_leader_thd);
#endif

  comm::RowChannel::Type row_channel_type{comm::RowChannel::Type::MEM};

  DBUG_EXECUTE_IF("pq_use_tcp_row_channel",
                  { row_channel_type = comm::RowChannel::Type::TCP; });
  DBUG_EXECUTE_IF("pq_use_mixed_row_channel", {
    row_channel_type = m_id % 2 == 0 ? comm::RowChannel::Type::MEM
                                     : comm::RowChannel::Type::TCP;
  });

  // Parameter nullptr means allocating message queue in leader's mem_root,
  // This assures leader can read message queue when workers exit.
  if (row_channel_type == comm::RowChannel::Type::MEM) {
    if (!(m_receiver_channel =
              comm::CreateMemRowChannel(m_leader_thd->mem_root, nullptr)) ||
        m_receiver_channel->Init(m_leader_thd, comm_event, true) ||
        !(m_sender_channel =
              comm::CreateMemRowChannel(m_thd.mem_root, m_receiver_channel)) ||
        m_sender_channel->Init(&m_thd, m_sender_exchange.event(), false))
      return true;
  } else {
    assert(row_channel_type == comm::RowChannel::Type::TCP);
    int sock = -1;
    if (!(m_receiver_channel =
              comm::CreateTcpRowChannel(m_leader_thd->mem_root, &sock)) ||
        m_receiver_channel->Init(m_leader_thd, comm_event, true) ||
        !(m_sender_channel =
              comm::CreateTcpRowChannel(m_thd.mem_root, &sock)) ||
        m_sender_channel->Init(&m_thd, m_sender_exchange.event(), false))
      return true;
  }

  if (m_sender_exchange.Init(m_leader_thd->mem_root, 1,
                             [this](uint) { return m_sender_channel; }))
    return true;

  // Only shared memory message queue needs this.
  if (row_channel_type == comm::RowChannel::Type::MEM) {
    comm::SetPeerEventForMemChannel(m_receiver_channel,
                                    m_sender_exchange.event());
    comm::SetPeerEventForMemChannel(m_sender_channel, comm_event);
  }

  return m_executor.Init(m_sender_channel, &m_sender_exchange,
                         m_leader_thd->lex->is_explain_analyze);
}

static void *launch_worker_thread_handle(void *arg) {
  pq::LocalWorker *worker = static_cast<pq::LocalWorker *>(arg);
  worker->ThreadMainEntry();
  return nullptr;
}

bool LocalWorker::Start() {
  my_thread_handle th;
  SetState(State::Starting);
  int res = mysql_thread_create(PSI_INSTRUMENT_ME, &th, &connection_attrib,
                                launch_worker_thread_handle, (void *)this);
  if (res != 0) {
    MyOsError(res, ER_STARTING_PARALLEL_QUERY_THREAD, MYF(0));
    SetState(State::StartFailed);
    return true;
  }

  return false;
}

Worker::State LocalWorker::state(bool need_state_lock) const {
  if (need_state_lock) mysql_mutex_lock(m_state_lock);
  mysql_mutex_assert_owner(m_state_lock);
  auto cur_state = m_state;
  if (need_state_lock) mysql_mutex_unlock(m_state_lock);

  return cur_state;
}

void LocalWorker::SetState(State state) {
  mysql_mutex_lock(m_state_lock);
  m_state = state;
  if (state == State::Finished) mysql_cond_broadcast(m_state_cond);
  mysql_mutex_unlock(m_state_lock);
}

void LocalWorker::CollectStatusVars(THD *target_thd) {
  add_to_status(&target_thd->status_var, &m_thd.status_var);
}

void LocalWorker::Terminate() {
  THD *thd = &m_thd;

  if (m_terminate_requested || thd->killed) return;

  mysql_mutex_lock(m_state_lock);
  bool need_send_kill = IsRunning(false) && m_state != State::Cleaning;
  mysql_mutex_unlock(m_state_lock);
  if (!need_send_kill) return;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_QUERY);
  m_terminate_requested = true;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

void LocalWorker::ThreadMainEntry() {
  SetState(State::Started);
  THD *thd = &m_thd;

  THD_CHECK_SENTRY(thd);
  my_thread_init();

  // XXX HAVE_PSI_THREAD_INTERFACE process

  thd->thread_stack = (char *)&thd;  // remember where our stack is

  thd->set_new_thread_id();
  thd->store_globals();
  // XXX Note, should after store_globals() calling because
  // THR_mysys is allocated by set_my_thread_var_id() called by in it.
  DBUG_RESTORE_CSSTACK(dbug_cs_stack_clone);

  THD *lthd = m_leader_thd;
  PartialExecutorContext context{lthd->tx_isolation,
                                 &lthd->start_time,
                                 lthd->db(),
                                 NULL_CSTR,
                                 lthd->query_id,
                                 lthd->security_context(),
                                 lthd->first_successful_insert_id_in_prev_stmt,
                                 lthd->lex->sql_command,
                                 lthd->lex->is_explain_analyze,
                                 [lthd](const std::string &key) {
                                   mysql_mutex_lock(&lthd->LOCK_thd_data);
                                   user_var_entry *entry =
                                       find_or_nullptr(lthd->user_vars, key);
                                   mysql_mutex_unlock(&lthd->LOCK_thd_data);
                                   return entry;
                                 }};
  mysql_mutex_lock(&lthd->LOCK_thd_query);
  context.query = lthd->query();
  mysql_mutex_unlock(&lthd->LOCK_thd_query);

  m_executor.InitExecThd(&context, lthd);

  // Clone snapshot always return false, currently
#if 0
  ha_clone_consistent_snapshot(&m_thd, lthd);
#endif

  THD_STAGE_INFO(thd, stage_starting);

  m_executor.ExecuteQuery(&context);

  CleanupThdResources();
  thd->mem_root->Clear();

  THD_CHECK_SENTRY(thd);

  my_thread_end();

  SetState(State::Finished);

  my_thread_exit(nullptr);
}

void LocalWorker::CleanupThdResources() {
  m_sender_exchange.Reset();
  destroy(m_sender_channel);
  m_sender_channel = nullptr;
}

Diagnostics_area *LocalWorker::stmt_da(bool finished_collect,
                                       ha_rows *found_rows,
                                       ha_rows *examined_rows) {
  THD* thd = &m_thd;
  if (!finished_collect) return thd->get_stmt_da();

  assert(!IsRunning(true));

  *found_rows = thd->previous_found_rows;
  *examined_rows = thd->get_examined_row_count();

  if (m_state != State::Finished) return nullptr;

  Diagnostics_area *da = thd->get_stmt_da();

  // The INTERRUPT is sent by leader to end workers
  // XXX should we copy other sql conditions to leader?
  if (m_terminate_requested && da->is_error() &&
      da->mysql_errno() == ER_QUERY_INTERRUPTED)
    return nullptr;

  if (!da->is_error() && da->current_statement_cond_count() == 0)
    return nullptr;
  return da;
}

Worker *CreateLocalWorker(uint id, THD *thd, PartialPlan *plan,
                          mysql_mutex_t *state_lock, mysql_cond_t *state_cond) {
  auto *worker =
      new (thd->mem_root) LocalWorker(id, thd, plan, state_lock, state_cond);
  return worker;
}
}  // namespace pq
