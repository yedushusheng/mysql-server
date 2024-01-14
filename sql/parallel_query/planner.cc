#include "sql/parallel_query/planner.h"

#include "scope_guard.h"
#include "sql/error_handler.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/opt_range.h"
#include "sql/opt_trace.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/plan_deparser.h"
#include "sql/parallel_query/rewrite_access_path.h"
#include "sql/sql_optimizer.h"

namespace pq {
/// Maximum parallel workers of this instance is allowed
uint max_parallel_workers = default_max_parallel_workers;
/// current total parallel workers of this instance
std::atomic<uint> total_parallel_workers;
/// acquires @param count parallel workers, return actual workers acquired. The
/// return value could be less than @param count unless @param strict is true.
static uint AcquireParallelWorkers(uint count, bool strict) {
  uint cur_total_workers =
      total_parallel_workers.load(std::memory_order_relaxed);

  while (cur_total_workers < max_parallel_workers) {
    uint workers = max_parallel_workers - cur_total_workers;
    if (workers > count)
      workers = count;
    else if ((workers == 1 && count > 1) ||  // (1)
             (strict && workers < count)) {  // (2)
      // (1) Workers must be greater than 1 unless user specifies.
      // (2) require exact count workers
      return 0;
    }
    if (total_parallel_workers.compare_exchange_strong(
            cur_total_workers, cur_total_workers + workers))
      return workers;
  }

  return 0;
}

static void ReleaseParallelWorkers(uint count) {
  total_parallel_workers.fetch_sub(count, std::memory_order_release);
}

static bool ItemRefuseParallel(const Item *item, const char **cause) {
  assert(item);
  *cause = nullptr;
  if (item->parallel_safe() != Item_parallel_safe::Safe) return true;
  // Some Item_refs in refer to outer table's field, we don't support that yet.
  if (item->used_tables() & OUTER_REF_TABLE_BIT) {
    *cause = "item_refer_to_outer_query_field";
    return true;
  }
  return false;
}

constexpr uint32 parallel_disabled = PT_hint_parallel::parallel_disabled;
constexpr uint32 degree_unspecified = PT_hint_parallel::degree_unspecified;

static uint32 table_parallel_degree(THD *thd, TABLE_LIST *table,
                                    bool *specified_by_hint) {
  uint32 degree = degree_unspecified;
  auto *lex = thd->lex;
  *specified_by_hint = true;
  if (table->opt_hints_table && table->opt_hints_table->parallel_scan) {
    auto *table_hint = table->opt_hints_table->parallel_scan;
    degree = table_hint->parallel_degree();
    if (degree != PT_hint_parallel::degree_unspecified) return degree;
  }

  if (table->opt_hints_qb && table->opt_hints_qb->get_parallel_hint()) {
    auto *qb_hint = table->opt_hints_qb->get_parallel_hint();
    degree = qb_hint->parallel_degree();
    if (degree != PT_hint_parallel::degree_unspecified) return degree;
  }

  if (lex->opt_hints_global && lex->opt_hints_global->parallel_hint) {
    auto *global_hint = lex->opt_hints_global->parallel_hint;
    degree = global_hint->parallel_degree();
    if (degree != PT_hint_parallel::degree_unspecified) return degree;
  }

  *specified_by_hint = false;
  return thd->variables.tdsql_max_parallel_degree;
}

static bool IsAccessRangeSupported(QEP_TAB *qt) {
  assert(qt->type() == JT_RANGE);

  if (qt->dynamic_range()) return false;

  auto quick_type = qt->quick()->get_type();
  return quick_type == QUICK_SELECT_I::QS_TYPE_RANGE ||
         quick_type == QUICK_SELECT_I::QS_TYPE_RANGE_DESC ||
         quick_type == QUICK_SELECT_I::QS_TYPE_SKIP_SCAN ||
         quick_type == QUICK_SELECT_I::QS_TYPE_GROUP_MIN_MAX;
}

static const char *TableAccessTypeRefuseParallel(QEP_TAB *qt) {
  auto typ = qt->type();
  // JT_EQ_REF used by subselect or join between tables
  if (typ < JT_EQ_REF || typ > JT_REF_OR_NULL || typ == JT_FT)
    return "table_access_type_is_not_supported";

  if (qt->type() == JT_RANGE && !IsAccessRangeSupported(qt))
    return "unsupported_access_range_type";

  if (qt->type() == JT_REF &&
      qt->ref().parallel_safe(qt->table()) != Item_parallel_safe::Safe)
    return "has_unsafe_items_in_table_ref";

  return nullptr;
}

static void get_scan_range_for_ref(MEM_ROOT *mem_root, TABLE_REF *ref,
                                   uint *keynr,
                                   key_range **min_key, key_range **max_key,
                                   bool ref_or_null) {
  *keynr = ref->key;
  *min_key = new (mem_root) key_range();
  (*min_key)->key = (const uchar *)ref->key_buff;
  (*min_key)->length = ref->key_length;
  (*min_key)->keypart_map = make_prev_keypart_map(ref->key_parts);
  (*min_key)->flag = HA_READ_KEY_EXACT;
  if (!ref_or_null) {
    *max_key = *min_key;
    return;
  }
  // Construct null ref key for parallel scan, avoid change origin
  // data, copy it from REF's key_buff (min_key->key point to it)
  *max_key = new (mem_root) key_range();
  **max_key = **min_key;
  bool old_null_ref_key = *ref->null_ref_key;
  *ref->null_ref_key = true;
  (*max_key)->key = new (mem_root) uchar[(*min_key)->length];
  memcpy(const_cast<uchar *>((*max_key)->key), (*min_key)->key,
         (*min_key)->length);
  *ref->null_ref_key = old_null_ref_key;
}

static bool GetTableParallelScanInfo(QEP_TAB *tab, ulong parallel_degree,
                                     parallel_scan_desc_t *scan_desc,
                                     ulong *ranges) {
  auto *table = tab->table();
  auto *thd = tab->join()->thd;

  scan_desc->is_ref_or_null = false;
  // Field is_asc set is complicated, set it in access path rewriter by
  // SetTableParallelScanReverse().
  scan_desc->is_asc = true;
  scan_desc->key_used = UINT16_MAX;

  switch (tab->type()) {
    case JT_ALL:
      scan_desc->min_key = scan_desc->max_key = nullptr;
      scan_desc->keynr = table->s->primary_key;
      break;
    case JT_INDEX_SCAN:
      scan_desc->min_key = scan_desc->max_key = nullptr;
      scan_desc->keynr = tab->index();
      break;
    case JT_REF:
      [[fallthrough]];
    case JT_EQ_REF:
      get_scan_range_for_ref(thd->mem_root, &tab->ref(), &scan_desc->keynr,
                             &scan_desc->min_key, &scan_desc->max_key, false);
      break;
    case JT_REF_OR_NULL:
      get_scan_range_for_ref(thd->mem_root, &tab->ref(), &scan_desc->keynr,
                             &scan_desc->min_key, &scan_desc->max_key, true);
      scan_desc->is_ref_or_null = true;
      break;
    case JT_RANGE:
      scan_desc->keynr = tab->quick()->index;
      scan_desc->key_used = tab->quick()->scan_range_keyused(
          thd->mem_root, &scan_desc->min_key, &scan_desc->max_key);
      break;
    default:
      assert(false);
      break;
  }

  *ranges = 100 * parallel_degree;
  ha_rows nrows;
  int res;
  if ((res = table->file->estimate_parallel_scan_ranges(
           scan_desc->keynr, scan_desc->min_key, scan_desc->max_key,
           scan_desc->is_ref_or_null, ranges, &nrows)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

static uint32 ChooseParallelDegreeByScanRange(THD *thd, uint32 parallel_degree,
                                             ulong ranges) {
  // Choosing disabled by system variables
  if (ranges > 0) return ranges;

  if (thd->variables.tdsql_parallel_scan_ranges_threshold == 0 ||
      parallel_degree == 1)
    return parallel_degree;

  // parallel disabled by insufficient ranges
  if (ranges < thd->variables.tdsql_parallel_scan_ranges_threshold)
    return parallel_disabled;

  if (parallel_degree > ranges) parallel_degree = ranges;

  return parallel_degree;
}

static void ChooseParallelPlan(JOIN *join) {
  THD *thd = join->thd;
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object wrapper(trace);
  Opt_trace_object trace_choosing(trace, "considering");
  bool chosen = false;
  const char *cause = nullptr;
  auto trace_set_guard =
      create_scope_guard([&trace_choosing, &chosen, &cause, thd]() {
        assert(chosen || cause || thd->is_error());
        trace_choosing.add("chosen", chosen);
        if (!chosen)
          trace_choosing.add_alnum(
              "cause", cause ? cause : "error_when_parallel_plan_choosing");
      });

  if (max_parallel_workers == 0) {
    cause = "disabled_by_zero_of_max_parallel_workers";
    return;
  }
  // Only support SELECT command
  if (thd->lex->sql_command != SQLCOM_SELECT) {
    cause = "not_supported_sql_command";
    return;
  }

  // Block uncacheable subqueries, Note some converted subqueries may also
  // blocked.
  if (!join->query_block->is_cacheable()) {
    cause = "not_supported_unacheable_subquery";
    return;
  }
  if (join->query_block->forbid_parallel_by_upper_query_block) {
    cause = "item_referred_by_upper_query_block";
    return;
  }
  // Block plan of derived table that transformed from subquery. It has a
  // bug ORDER::item that points to ORDER::item_initial incorrectly.
  auto *derived_table = join->query_expression()->derived_table;
  if (derived_table && derived_table->m_was_scalar_subquery) {
    cause = "buggy_derived_plan_from_subquery";
    return;
  }

  // Block plan which does't support yet
  if (join->zero_result_cause) {
    cause = "plan_with_zero_result";
    return;
  }

  // Need special parallel scan support
  if (join->select_count) {
    cause = "plan_with_select_count";
    return;
  }

  // We just support single table
  if (join->const_tables > 0 || join->primary_tables != 1) {
    cause = "not_single_table_or_just_const_tables";
    return;
  }

  // Don't support window function and rollup yet
  if (!join->m_windows.is_empty() ||
      join->rollup_state != JOIN::RollupState::NONE) {
    cause = "include_window_function_or_rullup";
    return;
  }

  // We don't support IN sub query because we don't support clone of its refs in
  // items yet.
  auto *subselect = join->query_expression()->item;
  if (subselect && subselect->substype() == Item_subselect::IN_SUBS) {
    Item_in_subselect *subs = down_cast<Item_in_subselect *>(subselect);
    if (subs->strategy == Subquery_strategy::SUBQ_EXISTS) {
      cause = "IN_subquery_strategy_is_SUBQ_EXISTS";
      return;
    }
  }

  // Block parallel query based on table properties
  auto *qt = &join->qep_tab[0];
  auto *table_ref = qt->table_ref;

  if (table_ref->is_placeholder()) {
    cause = "table_is_not_real_user_table";
    return;
  }

  if (is_temporary_table(table_ref)) {
    cause = "temporary_table_is_not_supported";
    return;
  }

  if ((cause = TableAccessTypeRefuseParallel(qt))) return;

  bool specified_by_hint;
  uint32 parallel_degree =
      table_parallel_degree(thd, qt->table_ref, &specified_by_hint);
  if (parallel_degree == parallel_disabled) {
    cause = specified_by_hint ? "forbidden_by_parallel_hint"
                              : "max_parallel_degree_not_set";
    return;
  }

  auto *table = qt->table();
  if (!(table->file->ha_table_flags() & HA_CAN_PARALLEL_SCAN)) {
    cause = "table_does_not_support_parallel_scan";
    return;
  }

  // Estimate parallel scan ranges, it will be displayed in EXPLAIN
  ulong scan_ranges;
  parallel_scan_desc_t scan_desc;
  if (GetTableParallelScanInfo(qt, parallel_degree, &scan_desc, &scan_ranges))
    return;

  if (!specified_by_hint &&
      (parallel_degree = ChooseParallelDegreeByScanRange(
           thd, parallel_degree, scan_ranges)) == parallel_disabled) {
    cause = "insufficient_parallel_scan_ranges";
    return;
  }

  const char *item_refuse_cause;
  // Block parallel query based on item expressions
  for (Item *item : join->query_block->fields) {
    if (!ItemRefuseParallel(item, &item_refuse_cause)) continue;
    cause = item_refuse_cause ? item_refuse_cause
                              : "include_unsafe_items_in_fields";
    return;
  }
  if (qt->condition() &&
      ItemRefuseParallel(qt->condition(), &item_refuse_cause)) {
    cause =
        item_refuse_cause ? item_refuse_cause : "filter_has_unsafe_condition";
    return;
  }
  if (join->having_cond &&
      ItemRefuseParallel(join->having_cond, &item_refuse_cause)) {
    cause = item_refuse_cause ? item_refuse_cause : "having_is_unsafe";
    return;
  }

  if ((parallel_degree =
           AcquireParallelWorkers(parallel_degree, specified_by_hint)) == 0)
  {
    cause = "too_many_parallel_workers";
    return;
  }

  if (!(join->parallel_plan =
            new (thd->mem_root) ParallelPlan(join, parallel_degree))) {
    ReleaseParallelWorkers(parallel_degree);
    return;
  }
  join->parallel_plan->SetTableParallelScan(table, scan_ranges, scan_desc);
  chosen = true;
}

bool GenerateParallelPlan(JOIN *join) {
  THD *thd = join->thd;
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object trace_wrapper(trace);
  Opt_trace_object trace_parallel_plan(trace, "parallel_plan");
  Opt_trace_array trace_steps(trace, "steps");

  ChooseParallelPlan(join);

  ParallelPlan *parallel_plan = join->parallel_plan;

  if (!parallel_plan) return false;

  assert(!thd->is_error());
  Opt_trace_context *const trace_gen = &thd->opt_trace;
  Opt_trace_object trace_gen_wrapper(trace_gen);
  Opt_trace_object trace_generating(trace, "generating");
  Opt_trace_array trace_gen_steps(trace, "steps");

  return parallel_plan->Generate();
}

ItemRefCloneResolver::ItemRefCloneResolver(MEM_ROOT *mem_root,
                                           Query_block *query_block)
    : m_refs_to_resolve(mem_root), m_query_block(query_block) {}

bool ItemRefCloneResolver::resolve(Item_ref *item, const Item_ref *from) {
  assert(from->ref);
  item->context = &m_query_block->context;
  return m_refs_to_resolve.push_back({item, from});
}

void ItemRefCloneResolver::final_resolve() {
  auto &base_ref_items = m_query_block->base_ref_items;
  for (auto &ref : m_refs_to_resolve) {
    auto *item_ref = ref.first;
    auto *from = ref.second;
    for (uint i = 0; i < m_query_block->fields.size(); i++) {
      auto *item = base_ref_items[i];
      if (item->is_identical(*from->ref)) {
        item_ref->ref = &base_ref_items[i];
        break;
      }
    }

    assert(item_ref->ref);
  }
}

Query_expression *PartialPlan::QueryExpression() const {
  return m_query_block->master_query_expression();
}

JOIN *PartialPlan::Join() const { return m_query_block->join; }

void PartialPlan::SetTableParallelScan(TABLE *table, ulong suggested_ranges,
                                       const parallel_scan_desc_t &psdesc) {
  m_parallel_scan_info = {table, suggested_ranges, psdesc};
}

bool PartialPlan::DeparsePlan(THD *thd) {
  assert(!m_plan_deparser);
  if (!(m_plan_deparser = new PlanDeparser(thd, m_query_block)) ||
      m_plan_deparser->deparse(thd))
    return true;
  return false;
}

String *PartialPlan::DeparsedStatement() const {
  if (!m_plan_deparser) return nullptr;
  return m_plan_deparser->statement();
}

ParallelPlan::ParallelPlan(JOIN *join, uint32 parallel_degree)
    : m_join(join),
      m_fields(join->thd->mem_root),
      m_parallel_degree(parallel_degree) {}

ParallelPlan::~ParallelPlan() {
  ReleaseParallelWorkers(m_parallel_degree);
  DestroyCollector(thd());
}

JOIN *ParallelPlan::PartialJoin() const {
  return m_partial_plan.Join();
}

JOIN *ParallelPlan::SourceJoin() const { return m_join; }

Query_block *ParallelPlan::SourceQueryBlock() const {
  return m_join->query_block;
}

THD *ParallelPlan::thd() const { return m_join->thd; }

bool ParallelPlan::AddPartialLeafTables() {
  Query_block *source = m_join->query_block;
  Query_block *partial_query_block = PartialQueryBlock();
  TABLE_LIST **last_table = &partial_query_block->leaf_tables;

  for (TABLE_LIST *tl = source->leaf_tables; tl; tl = tl->next_leaf) {
    assert(!tl->table->const_table);
    TABLE_LIST *table = tl->clone(thd()->mem_root);
    if (!table) return true;
    assert(table->tableno() == tl->tableno());
    *last_table = table;
    last_table = &table->next_leaf;
  }
  return false;
}

static bool is_item_field_in_fields(Item_field *item_field,
                                    const mem_root_deque<Item *> &fields) {
  for (auto *item : fields) {
    if (item->type() != Item::FIELD_ITEM) continue;
    if (down_cast<Item_field *>(item)->field == item_field->field) return true;
  }
  return false;
}

class PartialItemGenContext : public Item_clone_context {
  using Item_clone_context::Item_clone_context;

 public:
  void rebind_field(Item_field *item_field,
                    const Item_field *from_field) override {
    // We moved origin table to partial plan, so table_ref is not changed
    item_field->table_ref = from_field->table_ref;
    item_field->field = from_field->field;
    item_field->set_result_field(item_field->field);
    item_field->field_index = from_field->field_index;
  }

  void rebind_hybrid_field(Item_sum_hybrid_field *item_hybrid,
                           const Item_sum_hybrid_field *from_item) override {
    item_hybrid->set_field(from_item->get_field());
  }
  bool resolve_view_ref(Item_view_ref *item,
                        const Item_view_ref *from) override {
    // The ref point to another query lex, so it's safe
    item->ref = from->ref;
    if (from->get_first_inner_table())
      item->set_first_inner_table(from->get_first_inner_table());

    return false;
  }
};

bool ParallelPlan::GenPartialFields(Item_clone_context *context,
                                    FieldsPushdownDesc *fields_pushdown_desc) {
  Query_block *source = SourceQueryBlock();
  Query_block *partial_query_block = PartialQueryBlock();
  JOIN *join = SourceJoin();

  // See get_best_group_min_max(), which only support 1 table with no rollup
  assert(join->primary_tables == 1 &&
         join->rollup_state == JOIN::RollupState::NONE);

  auto *qt = &join->qep_tab[0];
  bool sumfuncs_full_pushdown =
      qt->quick() &&
      qt->quick()->get_type() == QUICK_SELECT_I::QS_TYPE_GROUP_MIN_MAX;

  for (uint i = 0; i < source->fields.size(); i++) {
    Item *new_item, *item = source->fields[i];
    // if ONLY_FULL_GROUP_BY is turned off, the functions contains
    // aggregation may include some item fields. Sum functions could be
    // const_item(), see optimize_aggregated_query(), it calls make_const()
    // to make MIN(), MAX() as a const. XXX This should be not pushed down
    // if non-pushed down sum got supported.
    assert(item->parallel_safe() == Item_parallel_safe::Safe);
    bool is_sum_func = item->type() == Item::SUM_FUNC_ITEM;
    bool pushdown =
        !(item->has_aggregation() || item->const_item()) || is_sum_func;

    if (pushdown) {
      if (!(new_item = item->clone(context))) return true;
      if (is_sum_func && !sumfuncs_full_pushdown) {
        Item_sum *item_sum = down_cast<Item_sum *>(new_item);
        item_sum->set_sum_stage(Item_sum::TRANSITION_STAGE);
        // change item_name so let EXPLAIN doesn't show Sum(Sum(a)) for leader's
        // aggregation
        Item *arg = item_sum->get_arg(0);
        item_sum->item_name = arg->item_name;
      }
      if (partial_query_block->add_item_to_list(new_item)) return true;
      if (fields_pushdown_desc->push_back(!is_sum_func || sumfuncs_full_pushdown
                                              ? FieldPushdownDesc::Replace
                                              : FieldPushdownDesc::Clone))
        return true;
    } else {
      FieldPushdownDesc pushdown_desc(FieldPushdownDesc::Clone);
      mem_root_deque<Item_field *> field_list(thd()->mem_root);
      enum_walk walk{enum_walk::PREFIX | enum_walk::ELIMINATE_SUM};
      item->walk(&Item::collect_item_field_processor, walk,
                 pointer_cast<uchar *>(&field_list));
      for (auto *item_field : field_list) {
        if (is_item_field_in_fields(item_field, partial_query_block->fields))
          continue;
        if (!(new_item = item_field->clone(context)) ||
            partial_query_block->add_item_to_list(new_item))
          return true;
      }
      if (fields_pushdown_desc->push_back(pushdown_desc)) return true;
    }
  }

  return false;
}

bool ParallelPlan::setup_partial_base_ref_items() {
  Query_block *partial_query_block = PartialQueryBlock();
  uint len = partial_query_block->fields.size();
  Item **arr = static_cast<Item **>(thd()->alloc(sizeof(Item *) * len));
  if (!arr) return true;
  partial_query_block->base_ref_items = Ref_item_array(arr, len);
  PartialJoin()->refresh_base_slice();
  return false;
}

bool ParallelPlan::ClonePartialOrders() {
  auto *query_block = PartialQueryBlock();
  auto *join = query_block->join;
  auto *source_query_block = SourceQueryBlock();
  auto *source_join = source_query_block->join;

  // group list is cloned in access path parallelizer

  join->grouped = source_join->grouped;
  join->need_tmp_before_win = source_join->need_tmp_before_win;

  join->simple_group = source_join->simple_group;

  join->simple_order = source_join->simple_order;

  return false;
}

class OriginItemRewriteContext : public Item_clone_context {
 public:
  OriginItemRewriteContext(THD *thd, Query_block *query_block,
                           ItemRefCloneResolver *ref_resolver,
                           TABLE *collector_table,
                           mem_root_deque<Item *> &partial_fields)
      : Item_clone_context(thd, query_block, ref_resolver),
        m_collector_table(collector_table),
        m_partial_fields(partial_fields) {}

  void rebind_field(Item_field *item_field, const Item_field *from) override {
    if (from->type() == Item::DEFAULT_VALUE_ITEM) {
      // Here is unpushed down Default_value_item, It's safe for Default by
      // reading original table
      item_field->field_index = from->field_index;
      item_field->field = from->field;
      item_field->set_result_field(from->field);
      return;
    }
    // Find table field no for pushed down item sum
    for (uint i = 0; i < m_partial_fields.size(); i++) {
      auto *partial_field = m_partial_fields[i];
      if (item_field->is_identical(partial_field)) {
        item_field->field_index = i;
        item_field->field = m_collector_table->field[i];
        item_field->set_result_field(item_field->field);
        break;
      }
    }
  }

  void rebind_hybrid_field(Item_sum_hybrid_field *item_hybrid,
                           const Item_sum_hybrid_field *from_item) override {
    item_hybrid->set_field(from_item->get_field());
  }

  Item *replace_sum_arg(Item_sum *item_func, const Item *arg) override {
    // Find table field no for pushed down item sum
    for (uint i = 0; i < m_partial_fields.size(); i++) {
      auto *pitem = m_partial_fields[i];
      if (item_func->is_identical(pitem)) {
        Item_field *item_field = new Item_field(m_collector_table->field[i]);
        // Arg could be nullptr, e.g. PTI_count_sym
        if (arg) item_field->set_id(arg->id());
        return item_field;
      }
    }
    assert(0);
    return nullptr;
  }

  bool resolve_view_ref(Item_view_ref *item,
                        const Item_view_ref *from) override {
    // all Item_view_ref should be pushed down except const_item
    assert(from->const_item());
    item->ref = from->ref;
    if (from->get_first_inner_table())
      item->set_first_inner_table(from->get_first_inner_table());
    return false;
  }

 private:
  TABLE *m_collector_table;
  mem_root_deque<Item *> &m_partial_fields;
};

bool ParallelPlan::GenFinalFields(FieldsPushdownDesc *fields_pushdown_desc) {
  Query_block *source_query_block = SourceQueryBlock();
  JOIN *source_join = SourceJoin();
  Query_block *partial_query_block = PartialQueryBlock();
  THD *thd = source_join->thd;
  TABLE *collector_table = m_collector->CollectorTable();
  auto &partial_fields = partial_query_block->fields;
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "final_plan");

  ItemRefCloneResolver ref_resolver(thd->mem_root, source_query_block);
  OriginItemRewriteContext clone_context(thd, source_query_block, &ref_resolver,
                                         collector_table, partial_fields);

  // Process original fields
  for (uint i = 0; i < source_query_block->fields.size(); i++) {
    FieldPushdownDesc &desc = (*fields_pushdown_desc)[i];
    Item *item = source_query_block->fields[i];
    Item *new_item = nullptr;
    if (desc.pushdown_action == FieldPushdownDesc::Replace) {
      for (uint j = 0; j < partial_fields.size(); j++) {
        if (item->is_identical(partial_fields[j])) {
          Field *field = collector_table->field[j];
          if (!(new_item = new Item_field(field))) return true;
          new_item->set_id(item->id());
          new_item->hidden = item->hidden;
          break;
        }
      }
      assert(new_item);
    } else {
      assert(desc.pushdown_action == FieldPushdownDesc::Clone);
      if (!(new_item = item->clone(&clone_context))) return true;
      if (new_item->type() == Item::SUM_FUNC_ITEM) {
        Item_sum *item_sum = down_cast<Item_sum *>(new_item);
        item_sum->set_sum_stage(Item_sum::COMBINE_STAGE);
        for (auto **sum_func_ptr = source_join->sum_funcs; *sum_func_ptr;
             sum_func_ptr++) {
          if (!item_sum->is_identical(*sum_func_ptr)) continue;
          *sum_func_ptr = item_sum;
        }
      }
    }
    if (m_fields.push_back(new_item)) return true;

    // Change origin plan: update original base_ref_items with new items
    bool found [[maybe_unused]] = false;
    for (size_t j = 0; j < source_query_block->fields.size(); j++) {
      if (source_query_block->base_ref_items[j]->is_identical(new_item)) {
        source_query_block->base_ref_items[j] = new_item;
#ifndef NDEBUG
        found = true;
#endif
        break;
      }
    }
    assert(found);
  }

  // Make all cloned ref point to correct item.
  clone_context.final_resolve_refs();


  return false;
}

bool ParallelPlan::GeneratePartialPlan(
    Item_clone_context **partial_clone_context,
    FieldsPushdownDesc *fields_pushdown_desc) {
  JOIN *source_join = SourceJoin();
  Query_block *source_query_block = source_join->query_block;
  THD *thd = source_join->thd;
  MEM_ROOT *mem_root = thd->mem_root;
  Query_expression *unit;
  LEX *lex_saved = thd->lex;
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "partial_plan");

  auto lex_restore_guard =
      create_scope_guard([&thd, lex_saved]() { thd->lex = lex_saved; });
  if (!(thd->lex = new (mem_root) st_lex_local)) return true;
  lex_start(thd);
  thd->lex->sql_command = lex_saved->sql_command;
  thd->lex->set_ignore(lex_saved->is_ignore());
  unit = thd->lex->unit;
  unit->set_prepared();
  unit->set_optimized();
  Query_block *partial_query_block = thd->lex->query_block;
  m_partial_plan.SetQueryBlock(partial_query_block);
  partial_query_block->select_number = source_query_block->select_number;
  // Generate JOIN for partial query block
  JOIN *join;
  if (!(join = new (mem_root) JOIN(thd, partial_query_block))) return true;
  partial_query_block->join = join;
  join->partial_plan = &m_partial_plan;
  join->set_executed();
  join->primary_tables = source_join->primary_tables;
  join->const_tables = source_join->const_tables;
  join->tables = source_join->tables;

  if (AddPartialLeafTables()) return true;

  auto *ref_clone_resolver =
      new (mem_root) ItemRefCloneResolver(mem_root, partial_query_block);
  if (!(*partial_clone_context = new (mem_root) PartialItemGenContext(
            thd, partial_query_block, ref_clone_resolver)))
    return true;

  if (GenPartialFields(*partial_clone_context, fields_pushdown_desc))
    return true;

  // XXX Set JOIN::select_distinct and JOIN::select_count
  // XXX collect pushed down fields from fields, table_ref, conditions and
  // having

  join->implicit_grouping = source_join->implicit_grouping;

  // XXX Don't need where_cond, Explain need this?
  // Temporary solution for deparser
  join->where_cond = source_query_block->where_cond();

  // XXX group by and order by generating
  if (setup_partial_base_ref_items()) return true;

  if (ClonePartialOrders()) return true;

  count_field_types(partial_query_block, &join->tmp_table_param,
                    partial_query_block->fields, false, false);
  if (join->alloc_func_list() ||
      join->make_sum_func_list(partial_query_block->fields, false))
    return true;

  join->tmp_table_param.precomputed_group_by =
      source_join->tmp_table_param.precomputed_group_by;

  if (CreateCollector(thd)) return true;

  return false;
}

bool ParallelPlan::Generate() {
  JOIN *source_join = SourceJoin();
  THD *thd = source_join->thd;
  LEX *lex = thd->lex;
  MEM_ROOT *mem_root = thd->mem_root;
  Query_block *source_query_block = source_join->query_block;
  Item_clone_context *partial_clone_context;
  FieldsPushdownDesc fields_pushdown_desc(mem_root);

  if (GeneratePartialPlan(&partial_clone_context, &fields_pushdown_desc)) return true;

  if (GenFinalFields(&fields_pushdown_desc)) return true;

  source_join->fields = &m_fields;
  // tmp_fields[REF_SLICE_ACTIVE] is not used, here we use it for access path
  // rewrite.
  source_join->tmp_fields[REF_SLICE_ACTIVE] = m_fields;
  source_join->ref_items[REF_SLICE_ACTIVE] = source_query_block->base_ref_items;

  if (!source_join->ref_items[REF_SLICE_SAVED_BASE].is_null())
    source_join->copy_ref_item_slice(REF_SLICE_SAVED_BASE, REF_SLICE_ACTIVE);

  if (GenerateAccessPath(partial_clone_context)) return true;

  partial_clone_context->final_resolve_refs();

  if (!lex->is_explain() || lex->is_explain_analyze)
    m_collector->PrepareExecution(thd);

  return false;
}

bool ParallelPlan::CreateCollector(THD *thd) {
  assert(m_parallel_degree < max_parallel_degree_limit);
  if (!(m_collector = new (thd->mem_root)
            Collector(m_parallel_degree, &m_partial_plan)) ||
      m_collector->CreateCollectorTable()) {
    destroy(m_collector);
    m_collector = nullptr;
    return true;
  }

  return false;
}

void ParallelPlan::EndCollector(THD *thd, ha_rows *found_rows) {
  m_collector->End(thd, m_need_collect_found_rows ? found_rows : nullptr);
}

void ParallelPlan::ResetCollector() { m_collector->Reset(); }

void ParallelPlan::DestroyCollector(THD *thd) {
  if (!m_collector) return;
  m_collector->Destroy(thd);
  destroy(m_collector);
}

AccessPath *ParallelPlan::CreateCollectorAccessPath(THD *thd) {
  return NewParallelCollectorAccessPath(thd, m_collector,
                                        m_collector->CollectorTable(), true);
}

bool ParallelPlan::GenerateAccessPath(Item_clone_context *clone_context) {
  JOIN *source_join = SourceJoin();
  THD *thd = source_join->thd;
  AccessPath *partial_path;
  AccessPath *parallelized_path;
  AccessPathParallelizer rewriter(clone_context, source_join, &m_partial_plan);
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "access_path_rewriting");

  assert(source_join->root_access_path());
  rewriter.set_collector_access_path(CreateCollectorAccessPath(thd));
  if (thd->lex->is_explain_analyze) {
    rewriter.set_fake_timing_iterator(NewFakeTimingIterator(thd, m_collector));
  }
  if (!(parallelized_path =
            rewriter.parallelize_access_path(source_join->root_access_path())))
    return true;
  partial_path = rewriter.out_path();
  if (source_join->root_access_path() != parallelized_path) {
    CopyBasicProperties(*partial_path, rewriter.collector_access_path());
    source_join->set_root_access_path(parallelized_path);
  }

  if (rewriter.MergeSort() &&
      m_collector->CreateMergeSort(source_join, rewriter.MergeSort()))
    return true;

  if (rewriter.has_pushed_limit_offset()) m_need_collect_found_rows = true;

  PartialJoin()->set_root_access_path(partial_path);

  return false;
}

bool add_tables_to_query_block(THD *thd, Query_block *query_block,
                               TABLE_LIST *tables) {
  for (TABLE_LIST *tl = tables; tl; tl = tl->next_leaf) {
    assert(!tl->is_view_or_derived());

    ulong table_options = 0;
    if (!tl->table_name) table_options |= TL_OPTION_ALIAS;
    if (tl->updating) table_options |= TL_OPTION_UPDATING;
    // XXX Don't need ignore_leaves, index_hints, option?

    LEX_CSTRING db_name{tl->db, tl->db_length};
    LEX_CSTRING table_name{tl->table_name, tl->table_name_length};
    Table_ident table_ident(db_name, table_name);
    TABLE_LIST *added;
    // Here we don't want add_table_to_list() to check unique alias names
    // because tables may be merged from other query block in parallel plan.
    if (!(added = query_block->add_table_to_list(thd, &table_ident, tl->alias,
                                                 table_options, TL_IGNORE,
                                                 tl->mdl_request.type)))
      return true;
    added->set_lock(tl->lock_descriptor());
    added->grant = tl->grant;
  }

  return false;
}

}  // namespace pq

bool Query_expression::clone_from(THD *thd, Query_expression *from,
                                  Item_clone_context *context) {
  uncacheable = from->uncacheable;
  m_reject_multiple_rows = from->m_reject_multiple_rows;
  assert(from->is_prepared());
  set_prepared();

  assert(!from->is_executed());
  assert(!from->fake_query_block);
  assert(!from->first_query_block()->next_query_block());

  for (Query_block *block = first_query_block(),
                   *from_block = from->first_query_block();
       block && from_block; block = block->next_query_block(),
                   from_block = from_block->next_query_block()) {
    if (block->clone_from(thd, from_block, context)) return true;
  }

  types = first_query_block()->fields;

  if (!from->is_optimized()) return false;

  create_access_paths(thd);

  set_optimized();

  JOIN *join = first_query_block()->join;

  m_root_iterator = CreateIteratorFromAccessPath(
      thd, m_root_access_path, join, /*eligible_for_batch_mode=*/true);

  return false;
}

bool Query_block::clone_from(THD *thd, Query_block *from,
                             Item_clone_context *context) {
  uncacheable = from->uncacheable;
  select_number = from->select_number;

  // Item_field clone depends on this
  if (setup_tables(thd, get_table_list(), false)) return true;

  // Clone table attributes from @from tables
  for (TABLE_LIST *tl = leaf_tables, *ftl = from->leaf_tables;
       tl && ftl; tl = tl->next_leaf, ftl = ftl->next_leaf) {
    tl->m_id = ftl->m_id;
    TABLE *table = tl->table;
    TABLE *ftable = ftl->table;

    assert(table);

    // record of const table has be filled during query optimization.
    if (ftable->const_table) {
      table->const_table = true;
      memcpy(table->record[0], ftable->record[0], ftable->s->reclength);
    }
    if (ftable->is_nullable()) table->set_nullable();
    if (ftable->has_null_row()) table->set_null_row();

    // XXX Tranditional explain needs these
    table->covering_keys = ftable->covering_keys;
    table->reginfo.not_exists_optimize = ftable->reginfo.not_exists_optimize;
    if (ftable->part_info) {
      bitmap_clear_all(&table->part_info->read_partitions);
      bitmap_copy(&table->part_info->read_partitions,
                  &ftable->part_info->read_partitions);
    }
    // XXX The materialized derived table clone
  }

  // create_intermediate_table() reads this
  with_sum_func = from->with_sum_func;

  // XXX clone olap, m_windows
  JOIN *from_join = from->join;
  if (from_join && !(join = new (thd->mem_root) JOIN(thd, this))) return true;

  // clone fields
  uint n_elems = from->fields.size();
  Item **array =
      static_cast<Item **>(thd->stmt_arena->alloc(sizeof(Item *) * n_elems));
  if (!array) return true;
  base_ref_items = Ref_item_array(array, n_elems);
  uint i = 0;
  for (auto *item : from->fields) {
    Item *new_item = item->clone(context);
    if (!new_item || fields.push_back(new_item))
      return true;
    base_ref_items[i++] = new_item;

    select_list_tables |= new_item->used_tables();
  }

  cond_value = from->cond_value;
  having_value = from->having_value;
  // Clone join
  if (join && join->clone_from(from->join, context)) {
      assert(thd->is_error() || thd->killed);

      return true;
    }

  return false;
}

bool JOIN::clone_from(JOIN *from, Item_clone_context *context) {
  // TABLE_REF clone use this, see TABLE_REF::clone().
  const_table_map = from->const_table_map;

  zero_result_cause = from->zero_result_cause;
  select_count = from->select_count;

  assert(!from->plan_is_const() && !from->where_cond);
  where_cond = nullptr;

  assert(from->order.empty());

  group_list =
      from->group_list.clone({thd->mem_root, &query_block->base_ref_items,
                              from->fields->size(), nullptr});

  grouped = from->grouped;
  need_tmp_before_win = from->need_tmp_before_win;
  implicit_grouping = from->implicit_grouping;
  simple_group = from->simple_group;

  simple_order = from->simple_order;
  select_distinct = from->select_distinct;
  m_ordered_index_usage = from->m_ordered_index_usage;
  skip_sort_order = from->skip_sort_order;
  m_windows_sort = from->m_windows_sort;

  m_select_limit = from->m_select_limit;
  calc_found_rows = from->calc_found_rows;

  assert(m_windows.elements == 0);

  // XXX Other members of tmp_table_param
  tmp_table_param.precomputed_group_by =
      from->tmp_table_param.precomputed_group_by;
  tmp_table_param.force_copy_fields = from->tmp_table_param.force_copy_fields;
  tmp_table_param.func_count = from->tmp_table_param.func_count;
  tmp_table_param.allow_group_via_temp_table =
      from->tmp_table_param.allow_group_via_temp_table;
  tmp_table_param.sum_func_count = from->tmp_table_param.sum_func_count;

  assert(query_block->olap ==  UNSPECIFIED_OLAP_TYPE);
  if (alloc_func_list()) return true;

  set_optimized();
  tables_list = query_block->leaf_tables;
  if (alloc_indirection_slices()) return true;

  tmp_fields[0] = *fields;
  ref_items[REF_SLICE_ACTIVE] = query_block->base_ref_items;

  best_read = from->best_read;
  sort_cost = from->sort_cost;
  windowing_cost = from->windowing_cost;

  assert(zero_result_cause == nullptr && tables_list);

  calc_group_buffer(this, group_list.order);
  send_group_parts = tmp_table_param.group_parts;
  if (make_sum_func_list(*fields, true)) return true;

  assert(from->root_access_path());

  pq::PartialAccessPathRewriter aprewriter(context, from, this);

  if (aprewriter.clone_and_rewrite(from->root_access_path())) return true;
  m_root_access_path = aprewriter.out_path();

  if (thd->is_error()) return true;

  set_plan_state(PLAN_READY);

  return false;
}
