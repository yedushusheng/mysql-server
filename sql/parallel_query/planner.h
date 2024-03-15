#ifndef PARALLEL_QUERY_PLANNER_H
#define PARALLEL_QUERY_PLANNER_H
#include "mem_root_deque.h"
#include "my_base.h"
#include "sql/item.h"
#include "sql/parallel_query/distribution.h"

class QEP_TAB;
struct AccessPath;
class Item_subselect;
class Item_cached_subselect_result;
class TABLE_REF;

#define PARALLEL_QUERY_JOIN (1ULL << 0)
#define PARALLEL_QUERY_SELECT_COUNT (1ULL << 1)
#define PARALLEL_QUERY_SUBQUERY_PUSHDOWN (1ULL << 2)
#define PARALLEL_QUERY_FULL_GROUPING_PUSHDOWN (1ULL << 3)

// Including the switch in this set, makes its default 'on'
#define PARALLEL_QUERY_SWITCH_DEFAULT                  \
  (PARALLEL_QUERY_JOIN | PARALLEL_QUERY_SELECT_COUNT | \
   PARALLEL_QUERY_SUBQUERY_PUSHDOWN | PARALLEL_QUERY_FULL_GROUPING_PUSHDOWN)

namespace pq {
class Collector;
class PartialItemGenContext;

constexpr uint default_max_parallel_degree = 0;
constexpr uint default_max_parallel_workers = 10000;
extern uint max_parallel_workers;
struct Item_ref_resolve_info {
  Item_ref *ref;
  const Item_ref *orig_ref;
  Query_block *query_block;
};

class ItemRefCloneResolver : public Item_ref_clone_resolver {
 public:
  ItemRefCloneResolver(MEM_ROOT *mem_root);
  bool resolve(Item_clone_context *context, Item_ref *item,
               const Item_ref *from) override;
  bool final_resolve(Item_clone_context *context) override;

#ifndef NDEBUG
  Query_block *query_block_for_inline_clone_assert{nullptr};
#endif

 private:
  mem_root_deque<Item_ref_resolve_info> m_refs_to_resolve;
};

struct ParallelScanInfo {
  TABLE *table{nullptr};
  ulong suggested_ranges;
  parallel_scan_desc_t scan_desc;
};

using QueryExpressionList = List<Query_expression>;
using CachedSubselectList = List<Item_cached_subselect_result>;
/**
   Currently, partial plan executor only depends on access path (No JOIN is
   created).
*/
class PartialPlan {
 public:
  ~PartialPlan() { destroy(m_dist_plan); }
  Query_block *QueryBlock() const { return m_query_block; }
  void SetQueryBlock(Query_block *query_block) { m_query_block = query_block; }
  Query_expression *QueryExpression() const;
  JOIN *Join() const;
  THD *thd() const;
  ParallelScanInfo &GetParallelScanInfo() { return m_parallel_scan_info; }
  void SetTableParallelScan(TABLE *table, ulong suggested_ranges,
                            const parallel_scan_desc_t &psdesc);
  void SetParallelScanReverse();
  bool IsParallelScanTable(TABLE *table) {
    return m_parallel_scan_info.table == table;
  }
  bool HasParallelScan() const { return m_parallel_scan_info.table != nullptr; }
  void SetDistPlan(dist::PartialDistPlan *dist_plan) {
    m_dist_plan = dist_plan;
  }
  dist::PartialDistPlan *DistPlan() const { return m_dist_plan; }
  /// @param set *hide_path_tree to true if dist system does not want to
  /// show partial plan tree.
  std::string ExplainPlan(bool *hide_plan_tree) {
    return m_dist_plan->ExplainPlan(hide_plan_tree);
  }

  /// Init execution, called by CollectorIterator::Init(), currently we do

  /// parallel scan initialization in it.
  bool InitExecution(uint num_workers);

  void SetPushdownInnerQueryExpressions(QueryExpressionList &units) {
    m_pushdown_inner_query_expressions = units;
  }
  QueryExpressionList &PushdownInnerQueryExpressions() {
    return m_pushdown_inner_query_expressions;
  }
  void SetCachedSubqueries(CachedSubselectList &cached_subqueries) {
    m_cached_subqueries = cached_subqueries;
  }
  CachedSubselectList &CachedSubqueries() { return m_cached_subqueries; }
 private:
  Query_block *m_query_block;
  QueryExpressionList m_pushdown_inner_query_expressions;
  CachedSubselectList m_cached_subqueries;
  /// Parallel scan info, current only support one table, This should be in
  /// class dist::PartialDistPlan, but we use it everywhere.
  ParallelScanInfo m_parallel_scan_info;
  dist::PartialDistPlan *m_dist_plan{nullptr};
};

class FieldPushdownDesc {
 public:
  enum PushdownAction { Replace, Clone };
  FieldPushdownDesc(Item *item, PushdownAction action)
      : m_item(item), m_action(action) {}

  inline bool to_replace() const { return m_action == Replace; }
  inline bool to_clone() const { return m_action == Clone; }
  inline Item *item() const { return m_item; }

 private:
  Item *m_item;
  PushdownAction m_action;
};

class ParallelDegreeHolder {
 public:
  ParallelDegreeHolder() = default;
  ParallelDegreeHolder(const ParallelDegreeHolder &) = delete;
  ParallelDegreeHolder& operator=(const ParallelDegreeHolder&) = delete;

  ~ParallelDegreeHolder();
  uint degree() const { return m_degree; }
  uint acquire(uint degree, bool forbid_reduce);
  void set(uint degree) {
    m_degree = degree;
    m_acquired_from_global = false;
  }

 private:
  bool m_acquired_from_global{true};
  uint m_degree{0};
};

enum class AggregateStrategy { OnePhase, PushedOnePhase, TwoPhase };

using FieldsPushdownDesc = mem_root_deque<FieldPushdownDesc>;

class ParallelPlan {
 public:
  ParallelPlan(JOIN *join);
  ~ParallelPlan();
  JOIN *SourceJoin() const;
  JOIN *PartialJoin() const;
  bool Generate();
  void ResetCollector();
  void EndCollector(THD *thd, ha_rows *found_rows);
  bool GenerateAccessPath(Item_clone_context *clone_context);
  bool AcquireParallelDegree(uint degree, bool forbid_reduce) {
    return m_parallel_degree.acquire(degree, forbid_reduce) == 0;
  }
  void SetParallelDegree(uint degree) { return m_parallel_degree.set(degree); }
  uint ParallelDegree() const { return m_parallel_degree.degree(); }

  void SetTableParallelScan(TABLE *table, ulong suggested_ranges,
                            const parallel_scan_desc_t &psdesc) {
    m_partial_plan.SetTableParallelScan(table, suggested_ranges,
                                        psdesc);
  }
  void SetParallelScanReverse() { m_partial_plan.SetParallelScanReverse(); }
  bool IsParallelScanTable(TABLE *table) {
    return m_partial_plan.IsParallelScanTable(table);
  }

  bool NonpushedAggregate() const {
    return m_aggregate_strategy == AggregateStrategy::OnePhase;
  }

  bool FullPushedAggregate() const {
    return m_aggregate_strategy == AggregateStrategy::PushedOnePhase;
  }

  void SetDistAdapter(dist::Adapter *adapter) { m_dist_adapter = adapter; }
  dist::Adapter *DistAdapter() const { return m_dist_adapter; }

  dist::TableDistArray *SetTableDists(dist::TableDistArray &table_dists) {
    m_table_dists = std::move(table_dists);
    return &m_table_dists;
  }
  void SetExecNodes(dist::NodeArray *exec_nodes) { m_exec_nodes = exec_nodes; }
  bool CreateCollector(THD *thd);

  Collector *GetCollector() { return m_collector; }
  PartialPlan *GetPartialPlan() { return &m_partial_plan; }

 private:
  THD *thd() const;
  Query_block *SourceQueryBlock() const;
  Query_block *PartialQueryBlock() const { return m_partial_plan.QueryBlock(); }

  bool AddPartialLeafTables();
  bool ResolvePushdownFields(FieldsPushdownDesc *fields_pushdown_desc);
  bool GenPartialFields(Item_clone_context *context,
                        FieldsPushdownDesc *fields_pushdown_desc);
  bool GenFinalFields(FieldsPushdownDesc *fields_pushdown_desc);
  bool setup_partial_base_ref_items();
  bool GeneratePartialPlan(PartialItemGenContext **partial_clone_context,
                           FieldsPushdownDesc *fields_pushdown_desc);
  // Clone ORDER for group list and order by
  bool ClonePartialOrders();
  bool CloneSemiJoinExecList(Item_clone_context *context);
  void DestroyCollector(THD *thd);

  JOIN *m_join;

  // The new item fields create by parallel plan
  mem_root_deque<Item *> m_fields;

  AggregateStrategy m_aggregate_strategy{AggregateStrategy::TwoPhase};

  Collector *m_collector{nullptr};
  // The query plan template for workers, workers clone plan from this.
  PartialPlan m_partial_plan;
  ParallelDegreeHolder m_parallel_degree;
  // If there is LIMIT OFFSET and it is pushed to workers, collecting found
  // rows from workers when workers end.
  bool m_need_collect_found_rows{false};
  /// All primary table distribution descriptions
  /// TODO: this should be in pq::Optimizer later.
  dist::TableDistArray m_table_dists;
  dist::NodeArray *m_exec_nodes;
  dist::Adapter *m_dist_adapter;
};

struct QEP_execution_state {
  TABLE *table;
  bool temptable;
  TABLE_REF *table_ref;
};
bool GenerateParallelPlan(JOIN *join);
bool add_tables_to_query_block(THD *thd, Query_block *query_block,
                               TABLE_LIST *tables);
bool ResolveItemRefByInlineClone(Item_ref *item_ref, const Item_ref *from,
                                 Item_clone_context *context);
}  // namespace pq
#endif
