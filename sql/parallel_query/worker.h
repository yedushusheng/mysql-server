#ifndef PARALLEL_QUERY_WORKER_H
#define PARALLEL_QUERY_WORKER_H
#include "sql/parallel_query/row_exchange.h"
#include "sql/sql_class.h"

namespace pq {
class PartialPlan;

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
  /// state "Cleaning" is for ER_QUERY_INTERRUPTED reported by workers, some
  /// mtr test case uses this e.g. "bug30769515_QUERY_INTERRUPTED" in
  /// QUICK_GROUP_MIN_MAX_SELECT::get_next(). When a worker is in Cleaning
  /// state, Terminate() skips to send termination request to it.
  enum class State { None, Starting, Started, Cleaning, Finished, StartFailed };
  Worker(uint id) : m_id(id) {}
  virtual ~Worker();

 public:
  // Life-cycle management interfaces
  virtual bool Init(comm::Event *comm_event) = 0;
  virtual bool Start() = 0;
  virtual void Terminate() = 0;

  // State interfaces
  bool IsRunning(bool need_state_lock) const {
    auto cur_state = state(need_state_lock);
    bool is_running =
        (cur_state == State::Started || cur_state == State::Cleaning ||
         cur_state == State::Starting);
    return is_running;
  }
  bool IsStartFailed() const { return state() == State::StartFailed; }

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
  // Internal state functions
  virtual State state(bool need_state_lock = true) const = 0;
  virtual void SetState(State state) = 0;
  const uint m_id;
  /// Communication facilities for leader, leader use this channel to
  /// receive rows from workers. Note, because we only have two phase of
  /// query plan, so we can put receiver channel here, move this to suitable
  /// position.
  comm::RowChannel *m_receiver_channel{nullptr};
};

Worker *CreateLocalWorker(uint id, THD *thd, PartialPlan *plan,
                          mysql_mutex_t *state_lock, mysql_cond_t *state_cond);
}
#endif
