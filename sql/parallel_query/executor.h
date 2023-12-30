#ifndef PARALLEL_QUERY_EXECUTOR_H
#define PARALLEL_QUERY_EXECUTOR_H
#include <sys/types.h>
#include <functional>
#include "sql/parallel_query/row_exchange.h"
#include "sql/row_iterator.h"
class Diagnostics_area;
class AccessPath;
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
  bool Init(THD *thd);
  int Read(THD *thd, uchar *buf, ulong reclength);
  void End(THD *thd, ha_rows *found_rows);
  AccessPath *PartialRootAccessPath() const;
  JOIN *PartialJoin() const;
  uint NumWorkers() const { return m_workers.size(); }
  bool CreateMergeSort(JOIN *join, ORDER *merge_order);
  Filesort *MergeSort() const { return m_merge_sort; }

 private:
  bool CreateRowExchange(MEM_ROOT *mem_root);
  bool LaunchWorkers(bool &has_failed_worker);
  void TerminateWorkers();
  bool HandleWorkerExited(uint windex);
  Diagnostics_area *combine_workers_stmt_da(THD *thd, ha_rows *found_rows);

  PartialPlan *m_partial_plan;
  TABLE *m_table{nullptr};
  Filesort *m_merge_sort{nullptr};
  RowExchange *m_row_exchange{nullptr};
  RowExchangeReader *m_row_exchange_reader{nullptr};
  std::vector<Worker *> m_workers;
  mysql_mutex_t m_worker_state_lock;
  mysql_cond_t m_worker_state_cond;
};
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
  uchar *const m_record;
  ha_rows *const m_examined_rows;
};


#endif