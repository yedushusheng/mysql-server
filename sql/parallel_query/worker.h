#ifndef PARALLEL_QUERY_WORKER_H
#define PARALLEL_QUERY_WORKER_H
#include <sys/types.h>
#include "sql/parallel_query/row_exchange.h"
#include "sql/sql_class.h"

class THD;
class Diagnostics_area;
namespace pq {
class MessageQueue;
class PartialPlan;

/**
  Class has a instance of THD, needs call destroy if allocated in MEM_ROOT
*/
class Worker {
 public:
  enum class State { None, Starting, Started, Finished, StartFailed };
  Worker(THD *thd, uint worker_id, PartialPlan *plan, mysql_mutex_t *state_lock,
         mysql_cond_t *state_cond);
  THD *thd() { return &m_thd; }
  THD *leader_thd() const { return m_leader_thd; }
  bool Init();
  int Start();
  bool IsRunning();
  void Terminate();
  void ThreadMainEntry();
  bool PrepareQueryPlan();
  void ExecuteQuery();
  void Cleanup();

  pq::MessageQueue *MessageQueue() const { return m_message_queue; }
  Diagnostics_area *stmt_da(ha_rows *found_rows, ha_rows *examined_rows);

 private:
  void InitExecThdFromLeader();
  THD *m_leader_thd;
  THD m_thd;  // Current worker's THD
  uint m_id;
  PartialPlan *m_query_plan;

  /// Communication facilities with leader
  RowExchange m_row_exchange{1, RowExchange::Type::SENDER};
  pq::MessageQueue *m_message_queue;
  RowExchangeWriter m_row_exchange_writer;

  State m_state{State::None};
  mysql_mutex_t *m_state_lock;
  mysql_cond_t *m_state_cond;
  bool m_terminate_requested{false};
};
}
#endif
