#ifndef PARALLEL_QUERY_PLANNER_H
#define PARALLEL_QUERY_PLANNER_H
#include "my_base.h"
#include "mem_root_deque.h"
#include "sql/item.h"

class QEP_TAB;
struct AccessPath;

namespace pq {
class Collector;

constexpr uint max_parallel_degree_limit = 128;
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

class PartialPlan {
 public:
  Query_block *QueryBlock() const { return m_query_block; }
  void SetQueryBlock(Query_block *query_block) { m_query_block = query_block; }
  Query_expression *QueryExpression() const;
  JOIN *Join() const;

 private:
  Query_block *m_query_block;
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
  ParallelPlan(MEM_ROOT *mem_root, Query_block *query_block);
  bool Generate(bool &fallback);
  void ResetCollector();
  void EndCollector(THD *thd, ha_rows *found_rows);
  void DestroyCollector(THD *thd);
  bool GenerateAccessPath(Item_clone_context *clone_context);

 private:
  THD *thd() const;
  Query_block *SourceQueryBlock() const { return m_source_query_block; }
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

  mem_root_deque<Item *> m_fields;  // The new item fields create by parallel plan
  Collector *m_collector{nullptr};
  // The query plan template for workers, workers clone plan from this.
  PartialPlan m_partial_plan;
  Query_block *m_source_query_block;
};

bool GenerateParallelPlan(JOIN *join);
bool add_tables_to_query_block(THD *thd, Query_block *query_block,
                               TABLE_LIST *tables);
}  // namespace pq
#endif
