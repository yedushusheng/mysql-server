#ifndef PARALLEL_QUERY_PLANNER_H
#define PARALLEL_QUERY_PLANNER_H
#include "mem_root_deque.h"
#include "my_base.h"
#include "sql/item.h"
#include "sql/parallel_query/distribution.h"

class QEP_TAB;
struct AccessPath;

#define PARALLEL_QUERY_JOIN (1ULL << 0)
#define PARALLEL_QUERY_SELECT_COUNT (1ULL << 1)

// Including the switch in this set, makes its default 'on'
#define PARALLEL_QUERY_SWITCH_DEFAULT (PARALLEL_QUERY_JOIN)

namespace pq {
class Collector;
class PartialItemGenContext;

constexpr uint default_max_parallel_degree = 0;
constexpr uint default_max_parallel_workers = 10000;
extern uint max_parallel_workers;

class ItemRefCloneResolver : public Item_ref_clone_resolver {
 public:
  ItemRefCloneResolver(MEM_ROOT *mem_root, Query_block *query_block);
  bool resolve(Item_ref *item, const Item_ref *from) override;
  bool final_resolve(Item_clone_context *context) override;

#ifndef NDEBUG
  Query_block *query_block_for_inline_clone_assert{nullptr};
#endif

 private:
  mem_root_deque<std::pair<Item_ref *, const Item_ref *>> m_refs_to_resolve;
  Query_block *m_query_block;
};

struct ParallelScanInfo {
  TABLE *table{nullptr};
  ulong suggested_ranges;
  parallel_scan_desc_t scan_desc;
};

struct Semijoin_mat_info {
  Semijoin_mat_info(MEM_ROOT *mem_root, ulong table_id)
      : sj_inner_exprs(mem_root), table_id(table_id) {}
  mem_root_deque<Item *> sj_inner_exprs;
  ulong table_id;
};

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
  void SetParallelScanReverse() {
    m_parallel_scan_info.scan_desc.is_asc = false;
  }
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
  bool CollectSJMatInfoList(JOIN *source_join,
                            Item_clone_context *clone_context);
  bool CloneSJMatInnerExprsForTable(ulong table_id,
                                    mem_root_deque<Item *> *sjm_fields,
                                    Item_clone_context *clone_context);
  List<Semijoin_mat_info> *SJMatInfoList() { return &m_sjm_info_list; }

 private:
  Query_block *m_query_block;
  List<Semijoin_mat_info> m_sjm_info_list{};
  /// Parallel scan info, current only support one table, This should be in
  /// class dist::PartialDistPlan, but we use it everywhere.
  ParallelScanInfo m_parallel_scan_info;
  dist::PartialDistPlan *m_dist_plan{nullptr};
};

class FieldPushdownDesc {
 public:
  enum PushdownAction { Replace, Clone };
  FieldPushdownDesc(PushdownAction action)
      : pushdown_action(action) {}
  PushdownAction pushdown_action;
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
    m_partial_plan.SetTableParallelScan(table, suggested_ranges, psdesc);
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
  bool CreateCollector(THD *thd);
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

bool GenerateParallelPlan(JOIN *join);
bool add_tables_to_query_block(THD *thd, Query_block *query_block,
                               TABLE_LIST *tables);
}  // namespace pq
#endif
