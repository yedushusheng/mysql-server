#ifndef PARALLEL_QUERY_WORKER_H
#define PARALLEL_QUERY_WORKER_H
#include "my_base.h"
#include "sql/sql_error.h"
class TABLE;

namespace pq {
class PartialPlan;
struct WorkerShareState;

namespace comm {
class Event;
class RowChannel;
}  // namespace comm

#ifndef NDEBUG
#define SET_DBUG_CS_STACK_CLONE(worker, cs_stack) \
  (worker)->dbug_cs_stack_clone = cs_stack
#else
#define SET_DBUG_CS_STACK_CLONE(worker, cs_stack)
#endif

/**
  The abstract class that the leader manages. Currently the only child
  class: LocalWorker that runs as local thread (or bthread). We can inherits
  from it to implement remote workers.
*/
class Worker {
 public:
  Worker(uint id, comm::Event *state_event)
      : m_id(id), m_state_event(state_event) {}
  virtual ~Worker();

 public:
  // Life-cycle management interfaces
  virtual bool Init(comm::Event *comm_event) = 0;
  virtual bool Start() = 0;
  virtual void Terminate() = 0;
  // State interfaces
  virtual bool IsRunning() = 0;
  virtual bool IsStartFailed() = 0;

  virtual Diagnostics_area *stmt_da(bool finished_collect, ha_rows *found_rows,
                                    ha_rows *examined_rows) = 0;
  virtual std::string *QueryPlanTimingData() = 0;
  virtual void CollectStatusVars(THD *target_thd) = 0;

  comm::RowChannel *receiver_channel() const { return m_receiver_channel; }

#ifndef NDEBUG
  bool is_error() { return stmt_da(false, nullptr, nullptr)->is_error(); }
  CSStackClone *dbug_cs_stack_clone = nullptr;
#endif

 protected:

  const uint m_id;
  /// Communication facilities for leader, leader use this channel to
  /// receive rows from workers. Note, because we only have two phase of
  /// query plan, so we can put receiver channel here, move this to suitable
  /// position.
  comm::RowChannel *m_receiver_channel{nullptr};
  comm::Event *m_state_event;
};

Worker *CreateLocalWorker(uint id, comm::Event *state_event, THD *thd,
                          PartialPlan *plan);
Worker *CreateMySQLClientWorker(uint id, comm::Event *state_event, THD *thd,
                                PartialPlan *partial_plan,
                                WorkerShareState *worker_share_state,
                                TABLE *collector_table);
}  // namespace pq
#endif
