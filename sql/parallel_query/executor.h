#ifndef PARALLEL_QUERY_EXECUTOR_H
#define PARALLEL_QUERY_EXECUTOR_H
#include <sys/types.h>
#include <functional>
#include "my_base.h"
#include "my_dbug.h"
#include "my_sqlcommand.h"
#include "sql/parallel_query/row_exchange.h"
#include "sql/row_iterator.h"
#include "sql/handler.h"
#include "sql/xa.h"

class Diagnostics_area;
class Filesort;
struct ORDER;
class user_var_entry;
class Security_context;

namespace pq {
class PartialPlan;
class Worker;

class Collector {
 public:
  using CollectResult = comm::RowExchange::Result;
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
  CollectResult Read(THD *thd);
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
  bool LaunchWorkers(THD *thd);
  void TerminateWorkers();
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

  comm::Event m_worker_state_event;
  bool is_ended{false};
};

struct PartialExecutorContext {
  enum_tx_isolation tx_isolation;
  struct timeval *start_time;
  LEX_CSTRING db;
  LEX_CSTRING query;
  query_id_t query_id;
  Security_context *security_context;
  ulonglong first_successful_insert_id_in_prev_stmt;

  enum_sql_command sql_command;
  bool is_explain_analyze;
  std::function<user_var_entry *(const std::string &)> find_user_var_entry;
};

class PartialExecutor {
 public:
  PartialExecutor(THD *thd, PartialPlan *query_plan,
                  std::function<void()> &&cleanup_func)
      : m_thd(thd), m_query_plan(query_plan), m_cleanup_func(cleanup_func) {}

  bool Init(comm::RowChannel *sender_channel,
            comm::RowExchange *sender_exchange, bool is_explain_analyze);

  void InitExecThd(PartialExecutorContext *ctx, THD *mdl_group_leader);
  void ExecuteQuery(PartialExecutorContext *context);

  std::string *QueryPlanTimingData() {
    if (m_query_plan_timing_data->size() == 0) return nullptr;

    return m_query_plan_timing_data.get();
  }

 private:
  bool PrepareQueryPlan(PartialExecutorContext *context);

  void EndQuery();
  bool AttachTablesParallelScan();
  /// Let row exchange reader side return, We need call this if there is a
  /// failure before lex->result is set.
  void NotifyAbort();

  THD *m_thd;  // Current worker's THD
  PartialPlan *m_query_plan;

  std::function<void()> m_cleanup_func;

  std::unique_ptr<std::string> m_query_plan_timing_data;

  /// Communication facilities with leader
  comm::RowChannel *m_sender_channel{nullptr};
  comm::RowExchange *m_sender_exchange{nullptr};
  comm::RowExchangeWriter m_row_exchange_writer;
};

std::string ExplainTableParallelScan(JOIN *join, TABLE *table);
RowIterator *NewFakeTimingIterator(THD *thd, Collector *collector);
}  // namespace pq

class CollectorIterator final : public RowIterator {
 public:
  // "examined_rows", if not nullptr, is incremented for each successful Read().
  CollectorIterator(THD *thd, pq::Collector *collector, ha_rows *examined_rows);
  ~CollectorIterator() override;

  bool Init() override;
  int Read() override;
  void SetNullRowFlag(bool) override {}
  void UnlockRow() override {}

 private:
  TABLE *table() { return m_collector->CollectorTable(); }
  pq::Collector *m_collector;
  ha_rows *const m_examined_rows;
};

#endif
