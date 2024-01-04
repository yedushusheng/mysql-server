#include "sql/parallel_query/local_worker.h"

#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/mysqld.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/worker.h"
#include "sql/sql_lex.h"

namespace pq {
/**
  Class has a instance of THD, needs call destroy if allocated in MEM_ROOT
*/
class LocalWorker : public Worker {
  /// state "Cleaning" is for ER_QUERY_INTERRUPTED reported by workers, some
  /// mtr test case uses this e.g. "bug30769515_QUERY_INTERRUPTED" in
  /// QUICK_GROUP_MIN_MAX_SELECT::get_next(). When a worker is in Cleaning
  /// state, Terminate() skips to send termination request to it.
  enum class State { None, Starting, Started, Cleaning, Finished };

 public:
  LocalWorker(uint id, comm::Event *state_event, THD *thd, PartialPlan *plan)
      : Worker(id, state_event),
        m_leader_thd(thd),
        m_thd(true, m_leader_thd),
        m_executor(&m_thd, plan, [this]() { SetState(State::Cleaning); }) {
    mysql_mutex_init(PSI_INSTRUMENT_ME, &m_state_mutex, MY_MUTEX_INIT_FAST);
  }
  ~LocalWorker() {
    destroy(m_sender_channel);
    mysql_mutex_destroy(&m_state_mutex);
  }

  /// The receiver row channel created inside of worker, @param comm_event
  /// is for receiver waiting.
  bool Init(comm::Event *comm_event) override;
  bool Start() override;
  void Terminate() override;
  Diagnostics_area *stmt_da(bool finished_collect, ha_rows *found_rows,
                            ha_rows *examined_rows) override;
  std::string QueryPlanTimingData() override {
    auto data = m_executor.GetTimingData();
    // No data if the worker got killed in plan preparing stage or starts
    // failed.
    assert(!data.empty() || m_terminate_requested);
    return data;
  }
  void CollectStatusVars(THD *target_thd) override;

  void ThreadMainEntry();

  bool IsRunning() override {
    State curstate = state();

    bool is_running =
        (curstate == State::Started || curstate == State::Cleaning ||
         curstate == State::Starting);

    return is_running;
  }

 private:
  static bool IsScheduleSysThread() {
    return static_cast<WorkerScheduleType>(worker_handling) ==
           WorkerScheduleType::SysThread;
  }

  State state() {
    mysql_mutex_lock(&m_state_mutex);
    auto curstate = m_state;
    mysql_mutex_unlock(&m_state_mutex);
    return curstate;
  }

  void SetState(State state) {
    mysql_mutex_lock(&m_state_mutex);
    m_state = state;
    mysql_mutex_unlock(&m_state_mutex);
  }

  /// Call this at the end of worker thread, Let leader continue by state event
  /// set, Note we must hold state mutex to prevent leader to destroy state
  /// event.
  void SetFinished() {
    mysql_mutex_lock(&m_state_mutex);
    m_state = State::Finished;
    m_state_event->Set();
    mysql_mutex_unlock(&m_state_mutex);
  }

  THD *m_leader_thd;
  THD m_thd;  // Current worker's THD

  /// Communication facilities with leader
  comm::RowChannel *m_sender_channel{nullptr};
  comm::RowExchange m_sender_exchange;

  PartialExecutor m_executor;

  // Worker thread state, should be protected by m_state_mutex.
  State m_state{State::None};
  mysql_mutex_t m_state_mutex;

  bool m_terminate_requested{false};
};

ulong worker_handling = default_worker_schedule_type;

bool LocalWorker::Init(comm::Event *comm_event) {
#if defined(ENABLED_DEBUG_SYNC)
  debug_sync_set_eval_id(&m_thd, m_id);
  debug_sync_clone_actions(&m_thd, m_leader_thd);
#endif

  comm::RowChannel::Type row_channel_type{comm::RowChannel::Type::MEM};

  DBUG_EXECUTE_IF("pq_use_tcp_row_channel",
                  { row_channel_type = comm::RowChannel::Type::TCP; });

  DBUG_EXECUTE_IF("pq_use_mem_mixed_tcp_row_channel", {
    row_channel_type = m_id % 2 == 0 ? comm::RowChannel::Type::MEM
                                     : comm::RowChannel::Type::TCP; });

  DBUG_EXECUTE_IF("pq_use_brpc_row_channel", {
    row_channel_type = comm::RowChannel::Type::BRPC_STREAM;
  });

  DBUG_EXECUTE_IF("pq_use_mem_mixed_brpc_row_channel", {
    row_channel_type = m_id % 2 == 0 ? comm::RowChannel::Type::MEM
                                     : comm::RowChannel::Type::BRPC_STREAM;
  });

  // Parameter nullptr means allocating message queue in leader's mem_root,
  // This assures leader can read message queue when workers exit.
  if (row_channel_type == comm::RowChannel::Type::MEM) {
    if (!(m_receiver_channel =
              comm::CreateMemRowChannel(m_leader_thd->mem_root, nullptr)) ||
        m_receiver_channel->Init(m_leader_thd, comm_event, true) ||
        !(m_sender_channel = comm::CreateMemRowChannel(m_leader_thd->mem_root,
                                                       m_receiver_channel)) ||
        m_sender_channel->Init(&m_thd, m_sender_exchange.event(), false))
      return true;
    comm::SetPeerEventForMemChannel(m_receiver_channel,
                                    m_sender_exchange.event());
    comm::SetPeerEventForMemChannel(m_sender_channel, comm_event);
  } else if (row_channel_type == comm::RowChannel::Type::TCP) {
    int sock = -1;
    if (!(m_receiver_channel =
              comm::CreateTcpRowChannel(m_leader_thd->mem_root, &sock)) ||
        m_receiver_channel->Init(m_leader_thd, comm_event, true) ||
        !(m_sender_channel =
              comm::CreateTcpRowChannel(m_leader_thd->mem_root, &sock)) ||
        m_sender_channel->Init(&m_thd, m_sender_exchange.event(), false))
      return true;
  } else if (row_channel_type == comm::RowChannel::Type::BRPC_STREAM) {
    if (comm::CreateBrpcStreamRowChannelPair(
            m_leader_thd->mem_root, &m_receiver_channel, &m_sender_channel) ||
        m_receiver_channel->Init(m_leader_thd, comm_event, true) ||
        m_sender_channel->Init(&m_thd, m_sender_exchange.event(), false)) {
      return true;
    }
  }

  if (m_sender_exchange.Init(m_leader_thd->mem_root, 1,
                             [this](uint) { return m_sender_channel; }))
    return true;

  return m_executor.Init(m_sender_channel, &m_sender_exchange);
}

static void *launch_worker_thread_handle(void *arg) {
  pq::LocalWorker *worker = static_cast<pq::LocalWorker *>(arg);
  worker->ThreadMainEntry();
  return nullptr;
}

bool LocalWorker::Start() {
  SetState(State::Starting);

  auto schedule_type = static_cast<WorkerScheduleType>(worker_handling);
  int res = 0;
  switch (schedule_type) {
    case WorkerScheduleType::bthread: {
      bthread_t th;
      res = bthread_start_background(&th, nullptr, launch_worker_thread_handle,
                                     (void *)this);
      break;
    }
    case WorkerScheduleType::SysThread: {
      my_thread_handle th;
      res = mysql_thread_create(PSI_INSTRUMENT_ME, &th, &connection_attrib,
                                launch_worker_thread_handle, (void *)this);
      break;
    }
    default:
      assert(false);
  }

  if (res != 0) MyOsError(res, ER_STARTING_PARALLEL_QUERY_THREAD, MYF(0));

  return res;
}

void LocalWorker::CollectStatusVars(THD *target_thd) {
  add_to_status(&target_thd->status_var, &m_thd.status_var);
}

void LocalWorker::Terminate() {
  THD *thd = &m_thd;

  if (m_terminate_requested || thd->killed) return;

  State curstate = state();
  // Don't send kill for State::Cleaning otherwise killed state sent by
  // worker itself would be ignored by leader.
  if (curstate != State::Starting && curstate != State::Started) return;

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

  thd->set_new_thread_id();

  // XXX HAVE_PSI_THREAD_INTERFACE process

  thd->thread_stack = (char *)&thd;  // remember where our stack is

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
                                 lthd->save_raw_record,
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
  ha_clone_consistent_snapshot(&m_thd, lthd);

  THD_STAGE_INFO(thd, stage_starting);

  m_executor.ExecuteQuery(&context);

  thd->mem_root->Clear();

  THD_CHECK_SENTRY(thd);

  my_thread_end();

  SetFinished();

  if (IsScheduleSysThread()) my_thread_exit(nullptr);
}

Diagnostics_area *LocalWorker::stmt_da(bool finished_collect,
                                       ha_rows *found_rows,
                                       ha_rows *examined_rows) {
  THD *thd = &m_thd;
  if (!finished_collect) return thd->get_stmt_da();

  assert(!IsRunning());

  *found_rows = thd->previous_found_rows;
  *examined_rows = thd->get_examined_row_count();

  if (state() != State::Finished) return nullptr;

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

Worker *CreateLocalWorker(uint id, comm::Event *state_event,
                          PartialPlan *plan) {
  THD *thd = plan->thd();
  return new (thd->mem_root) LocalWorker(id, state_event, thd, plan);
}
}  // namespace pq
