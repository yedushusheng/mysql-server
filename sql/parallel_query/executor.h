#ifndef PARALLEL_QUERY_EXECUTOR_H
#define PARALLEL_QUERY_EXECUTOR_H
#include <sys/types.h>
#include <functional>
#include "my_base.h"
#include "my_dbug.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql/parallel_query/row_exchange.h"
#include "sql/row_iterator.h"

class Diagnostics_area;
class AccessPath;
class Filesort;
struct ORDER;

namespace pq {
class PartialPlan;
class Worker;

class Collector {
 public:
  Collector(uint num_workers, PartialPlan *partial_plan);
  ~Collector();
  TABLE *CollectorTable() const { return m_table; }
  bool CreateCollectorTable();
  void PrepareExecution(THD *thd);
  /// This function could be called multiple times, because it called in
  /// RowIterator::Init(), If an initial operation just need once, should put it
  /// to PrepareExecution()
  bool Init(THD *thd);
  void Reset();
  int Read(THD *thd);
  void End(THD *thd, ha_rows *found_rows);
  void Destroy(THD *thd);
  AccessPath *PartialRootAccessPath() const;
  PartialPlan *partial_plan() const { return m_partial_plan; }
  JOIN *PartialJoin() const;
  uint NumWorkers() const { return m_workers.size(); }
  bool CreateMergeSort(JOIN *join, ORDER *merge_order);
  Filesort *MergeSort() const { return m_merge_sort; }
  template <class Func>
  void ForEachWorker(Func &&func) {
    for (auto *worker : m_workers) func(worker);
  }

 private:
  bool LaunchWorkers(bool &has_failed_worker);
  void TerminateWorkers();
  bool HandleWorkerExited(uint windex);
  void CollectStatusFromWorkers(THD *thd);
  bool InitParallelScan();
  Diagnostics_area *combine_workers_stmt_da(THD *thd, ha_rows *found_rows);
#if !defined(NDEBUG)
  CSStackClone dbug_cs_stack_clone;
#endif
  PartialPlan *m_partial_plan;
  TABLE *m_table{nullptr};
  Filesort *m_merge_sort{nullptr};
  comm::RowExchange m_receiver_exchange;
  comm::RowExchangeReader *m_row_exchange_reader{nullptr};

  std::vector<Worker *> m_workers;

  mysql_mutex_t m_worker_state_lock;
  mysql_cond_t m_worker_state_cond;
  bool is_ended{false};
};
std::string ExplainTableParallelScan(JOIN *join, TABLE *table);
RowIterator *NewFakeTimingIterator(THD *thd, Collector *collector);
}  // namespace pq

class CollectorIterator final : public TableRowIterator {
 public:
  // "examined_rows", if not nullptr, is incremented for each successful Read().
  CollectorIterator(THD *thd, pq::Collector *collector, ha_rows *examined_rows);
  ~CollectorIterator() override;

  bool Init() override;
  int Read() override;

 private:
  pq::Collector *m_collector;
  ha_rows *const m_examined_rows;
};


#endif
