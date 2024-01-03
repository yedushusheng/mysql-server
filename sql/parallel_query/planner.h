#ifndef PARALLEL_QUERY_PLANNER_H
#define PARALLEL_QUERY_PLANNER_H
#include "my_base.h"
#include "mem_root_deque.h"
#include "sql/item.h"

class QEP_TAB;
struct AccessPath;

namespace pq {
class Collector;
class AccessPathChangesStore;

constexpr uint default_max_parallel_degree = 0;

class ItemRefCloneResolver : public Item_ref_clone_resolver {
 public:
  ItemRefCloneResolver(MEM_ROOT *mem_root, Query_block *query_block);
  bool resolve(Item_ref *item, const Item_ref *from) override;
  void final_resolve() override;

 private:
  mem_root_deque<std::pair<Item_ref *, const Item_ref *>> m_refs_to_resolve;
  Query_block *m_query_block;
};

struct ParallelScanInfo {
  TABLE *table;
  parallel_scan_desc_t scan_desc;
};

/**
   Currently, partial plan executor only depends on access path (No JOIN is
   created).
*/
class PartialPlan {
 public:
  Query_block *QueryBlock() const { return m_query_block; }
  void SetQueryBlock(Query_block *query_block) { m_query_block = query_block; }
  Query_expression *QueryExpression() const;
  JOIN *Join() const;
  void SetTablesParallelScan(TABLE *table, const parallel_scan_desc_t &psdesc);
  ParallelScanInfo &TablesParallelScan() { return m_parallel_scan_info; }

 private:
  Query_block *m_query_block;
  ParallelScanInfo m_parallel_scan_info;
};

class SourcePlanChangedStore {
 public:
  SourcePlanChangedStore(JOIN *join) : m_join(join) {}
  ~SourcePlanChangedStore();

  bool save_base_ref_items();
  bool save_sum_funcs();
  bool save_fields_and_ref_items();
  AccessPathChangesStore *create_access_path_changes();

  /**
    Just need restore query block if parallel query finish otherwise JOIN needs
    to be restored.
  */
  void reset_restore_for_join();

 private:
  MEM_ROOT *mem_root();

  Item **m_base_ref_items{nullptr};
  Item_sum **m_sum_funcs{nullptr};
  size_t m_sum_funcs_length;
  mem_root_deque<Item *> *m_fields{nullptr};
  Ref_item_array *m_ref_items;
  mem_root_deque<Item *> *m_tmp_fields;
  AccessPathChangesStore *m_access_path_changes{nullptr};

  JOIN *m_join;
};

class FieldPushdownDesc {
 public:
  enum PushdownAction { Replace, Clone };
  FieldPushdownDesc(PushdownAction action)
      : pushdown_action(action) {}
  PushdownAction pushdown_action;
};

using FieldsPushdownDesc = mem_root_deque<FieldPushdownDesc>;

class ParallelPlan {
 public:
  ParallelPlan(JOIN *join, uint parallel_degree);
  ~ParallelPlan();
  bool Generate();
  void ResetCollector();
  void EndCollector(THD *thd, ha_rows *found_rows);
  bool GenerateAccessPath(Item_clone_context *clone_context);
  uint ParallelDegree() const { return m_parallel_degree; }

 private:
  THD *thd() const;
  Query_block *SourceQueryBlock() const;
  Query_block *PartialQueryBlock() const { return m_partial_plan.QueryBlock(); }
  JOIN *SourceJoin() const;
  JOIN *PartialJoin() const;

  AccessPath *CreateCollectorAccessPath(THD *thd);
  bool AddPartialLeafTables();
  bool ResolvePushdownFields(FieldsPushdownDesc *fields_pushdown_desc);
  bool GenPartialFields(Item_clone_context *context,
                        FieldsPushdownDesc *fields_pushdown_desc);
  bool GenFinalFields(FieldsPushdownDesc *fields_pushdown_desc);
  bool setup_partial_base_ref_items();
  bool GeneratePartialPlan(Item_clone_context **partial_clone_context,
                           FieldsPushdownDesc *fields_pushdown_desc);
  // Clone ORDER for group list and order by
  bool ClonePartialOrders();
  bool CreateCollector(THD *thd);
  void DestroyCollector(THD *thd);

  JOIN *m_join;
  mem_root_deque<Item *> m_fields;  // The new item fields create by parallel plan
  Collector *m_collector{nullptr};
  // The query plan template for workers, workers clone plan from this.
  PartialPlan m_partial_plan;
  SourcePlanChangedStore m_source_plan_changed;
  uint m_parallel_degree;
  // If there is LIMIT OFFSET and it is pushed to workers, collecting found
  // rows from workers when workers end.
  bool m_need_collect_found_rows{false};
};

bool GenerateParallelPlan(JOIN *join);
bool add_tables_to_query_block(THD *thd, Query_block *query_block,
                               TABLE_LIST *tables);
}  // namespace pq
#endif
