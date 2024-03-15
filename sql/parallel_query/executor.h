#ifndef PARALLEL_QUERY_EXECUTOR_H
#define PARALLEL_QUERY_EXECUTOR_H
#include <sys/types.h>
#include <functional>
#include "my_base.h"
#include "my_dbug.h"
#include "my_sqlcommand.h"
#include "sql/handler.h"
#include "sql/parallel_query/distribution.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/row_exchange.h"
#include "sql/row_iterator.h"
#include "sql/sql_lex.h"
#include "sql/xa.h"

class Diagnostics_area;
class Filesort;
class user_var_entry;
class Security_context;

namespace pq {
class Worker;

class Collector {
 public:
  using CollectResult = comm::RowExchange::Result;
  Collector(dist::Adapter *dist_adapter, dist::NodeArray *exec_nodes,
            PartialPlan *partial_plan, uint num_workers);
  ~Collector();
  ulong id() const { return m_id; }
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
  bool CreateMergeSort(JOIN *join, ORDER *merge_order, bool remove_duplicates);
  std::pair<std::string *, std::string *> WorkersTimingData() const;
  AccessPath *Explain(std::vector<std::string> &description);
  void SetPreevaluateSubqueries(CachedSubselects &cached_subselects) {
    m_preevaluate_subqueries = cached_subselects;
  }

 private:
  bool LaunchWorkers();
  void TerminateWorkers();
  void CollectStatusFromWorkers(THD *thd);
  void CollectTimingDataFromWorkers(MEM_ROOT *mem_root);
  void ResetWorkersTimingData() {
    if (!m_workers_timing_data) return;
    destroy(m_workers_timing_data);
    m_workers_timing_data = nullptr;
  }
  Diagnostics_area *combine_workers_stmt_da(THD *thd, ha_rows *found_rows);
  dist::Adapter *m_dist_adapter;
  dist::NodeArray *m_exec_nodes;
#if !defined(NDEBUG)
  CSStackClone dbug_cs_stack_clone;
#endif
  PartialPlan *m_partial_plan;
  TABLE *m_table{nullptr};
  // This is a shallow copy of cached subqueries of partial plan for evaluating
  // them.
  CachedSubselects m_preevaluate_subqueries;  
  Filesort *m_merge_sort{nullptr};
  comm::RowExchange m_receiver_exchange;
  comm::RowExchangeReader *m_row_exchange_reader{nullptr};

  std::vector<Worker *> m_workers;
  /// Save timing data of EXPLAIN ANALYZE for each workers, workers are
  /// destroyed when its parent JOIN is cleared for next execution. However we
  /// still need to read its timing data after JOIN is cleared if the query
  /// block is executed in a materialized subquery.
  Mem_root_array<std::string> *m_workers_timing_data{nullptr};

  comm::Event m_worker_state_event;
  bool is_ended{false};
  ulong m_id{reinterpret_cast<ulong>(this)};
};

struct PartialExecutorContext {
  enum_tx_isolation tx_isolation;
  struct timeval *start_time;
  LEX_CSTRING db;
  LEX_CSTRING query;
  query_id_t query_id;
  Security_context *security_context;
  ulonglong first_successful_insert_id_in_prev_stmt;
  bool save_raw_record;

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
            comm::RowExchange *sender_exchange);

  void InitExecThd(PartialExecutorContext *ctx, THD *mdl_group_leader);
  void ExecuteQuery(PartialExecutorContext *context);

  std::string GetTimingData() {
    return m_query_plan_timing_data == nullptr
               ? std::string()
               : std::move(*m_query_plan_timing_data);
  }

 private:
  bool PrepareQueryPlan(PartialExecutorContext *context);

  void EndQuery();
  bool AttachTablesParallelScan();

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

inline Query_expression *find_inner_expression_by_id(
    List<Query_expression> *inner_list, Query_expression *from_inner_first,
    ulong target_id) {
  Query_expression *target_inner = nullptr;
  if (inner_list) {
    // Use inner expression list first, this used by root query block of partial
    // plan.
    for (auto &inner : *inner_list) {
      if (inner.m_id == target_id) {
        target_inner = &inner;
        break;
      }
    }
    assert(target_inner != nullptr);
    return target_inner;
  }

  assert(from_inner_first);
  for (auto *inner = from_inner_first; inner;
       inner = inner->next_query_expression()) {
    if (inner->m_id == target_id) {
      target_inner = inner;
      break;
    }
  }

  assert(target_inner != nullptr);
  return target_inner;
}

/**
  Call @param func for each inner query expression of @param query_block, the
  @param query_block is cloned from @from.
*/
template <class Func>
bool for_each_inner_expressions(Query_block *query_block, Query_block *from,
                                Func &&func) {
  for (auto *inner_unit = query_block->first_inner_query_expression();
       inner_unit; inner_unit = inner_unit->next_query_expression()) {
    auto *from_inner_unit = find_inner_expression_by_id(
        query_block->m_inner_query_expressions_clone_from,
        from->first_inner_query_expression(), inner_unit->m_id);

    if (func(inner_unit, from_inner_unit)) return true;
  }

  return false;
}
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
