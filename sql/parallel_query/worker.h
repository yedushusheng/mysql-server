#ifndef PARALLEL_QUERY_WORKER_H
#define PARALLEL_QUERY_WORKER_H
#include "sql/parallel_query/row_exchange.h"
#include "sql/sql_class.h"

namespace pq {
class PartialPlan;

/**
  Class has a instance of THD, needs call destroy if allocated in MEM_ROOT
*/
class Worker {
 public:
  /// state "Cleaning" is for ER_QUERY_INTERRUPTED reported by workers, some
  /// mtr test case uses this e.g. "bug30769515_QUERY_INTERRUPTED" in
  /// QUICK_GROUP_MIN_MAX_SELECT::get_next(). When a worker is in Cleaning
  /// state, Terminate() skips to send termination request to it.
  enum class State { None, Starting, Started, Cleaning, Finished, StartFailed };
  Worker(THD *thd, uint worker_id, PartialPlan *plan, mysql_mutex_t *state_lock,
         mysql_cond_t *state_cond);
  THD *thd() { return &m_thd; }
  THD *leader_thd() const { return m_leader_thd; }
  bool Init(MessageQueueEvent *peer_event);
  int Start();
  bool IsStartFailed() const;
  bool IsRunning(bool need_state_lock);
  void Terminate();
  void ThreadMainEntry();
  bool PrepareQueryPlan();
  void ExecuteQuery();
  void EndQuery();
  bool is_error() { return m_thd.is_error(); }
  MessageQueue *message_queue() { return m_message_queue; }
  MessageQueueEvent *message_queue_event() { return m_row_exchange_writer.Event(); }
  Diagnostics_area *stmt_da(ha_rows *found_rows, ha_rows *examined_rows);
  std::string *QueryPlanTimingData() { return m_query_plan_timing_data.get(); }
#if !defined(NDEBUG)
  CSStackClone *dbug_cs_stack_clone;
#endif

 private:
  void InitExecThdFromLeader();
  /// Let row exchange reader side return, We need call this if there is a
  /// failure before lex->result is set.
  void NotifyAbort();
  bool AttachTablesParallelScan();
  /// Set worker state to @param state and broadcast "state cond" if
  /// State::Finished.
  void SetState(State state);
  THD *m_leader_thd;
  THD m_thd;  // Current worker's THD
  uint m_id;
  PartialPlan *m_query_plan;

  std::unique_ptr<std::string> m_query_plan_timing_data;

  /// Communication facilities with leader
  RowExchange m_row_exchange{1};
  MessageQueue *m_message_queue;
  RowExchangeWriter m_row_exchange_writer;
  State m_state{State::None};
  mysql_mutex_t *m_state_lock;
  mysql_cond_t *m_state_cond;
  bool m_terminate_requested{false};
};
}
#endif
