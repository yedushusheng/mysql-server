#include "sql/parallel_query/planner.h"

#include "scope_guard.h"
#include "sql/error_handler.h"
#include "sql/handler.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/opt_range.h"
#include "sql/opt_trace.h"
#include "sql/parallel_query/distribution.h"
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

static bool IsAccessRangeSupported(QEP_TAB *qt) {
  assert(qt->type() == JT_RANGE);

  auto quick_type = qt->quick()->get_type();
  return quick_type == QUICK_SELECT_I::QS_TYPE_RANGE ||
         quick_type == QUICK_SELECT_I::QS_TYPE_RANGE_DESC ||
         quick_type == QUICK_SELECT_I::QS_TYPE_SKIP_SCAN ||
         quick_type == QUICK_SELECT_I::QS_TYPE_GROUP_MIN_MAX;
}

static const char *TableAccessTypeRefuseParallel(QEP_TAB *qt) {
  auto typ = qt->type();
  assert(typ != JT_UNKNOWN);
  // JT_EQ_REF used by subselect or join between tables
  // Currently, we don't support JT_SYSTEM, JT_FT and JT_INDEX_MERGE.
  if (typ == JT_SYSTEM || typ == JT_FT || typ == JT_INDEX_MERGE)
    return "table_access_type_is_not_supported";

  if (qt->dynamic_range()) {
    assert(typ == JT_ALL || typ == JT_RANGE /* || typ == JT_INDEX_MERGE */);
    return "dynamic_range_access_is_not_supported";
  }

  if (qt->type() == JT_RANGE && !IsAccessRangeSupported(qt))
    return "unsupported_access_range_type";

  if (qt->type() >= JT_CONST && qt->type() <= JT_REF &&
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

void GetTableAccessInfo(QEP_TAB *tab, uint *keynr, key_range **min_key,
                        key_range **max_key, bool *is_ref_or_null,
                        uint16 *key_used) {
  auto *table = tab->table();
  auto *thd = tab->join()->thd;

  *is_ref_or_null = false;
  *key_used = UINT16_MAX;

  switch (tab->type()) {
    case JT_ALL:
      *min_key = *max_key = nullptr;
      *keynr = table->s->primary_key;
      break;
    case JT_INDEX_SCAN:
      *min_key = *max_key = nullptr;
      *keynr = tab->index();
      break;
    case JT_REF_OR_NULL:
      *is_ref_or_null = true;
      [[fallthrough]];
    case JT_CONST:
      [[fallthrough]];
    case JT_REF:
      [[fallthrough]];
    case JT_EQ_REF:
      get_scan_range_for_ref(thd->mem_root, &tab->ref(), keynr, min_key,
                             max_key, *is_ref_or_null);
      break;
    case JT_RANGE:
      *keynr = tab->quick()->index;
      *key_used =
          tab->quick()->scan_range_keyused(thd->mem_root, min_key, max_key);
      break;
    default:
      assert(false);
      break;
  }
}

static bool GetTableParallelScanInfo(QEP_TAB *tab, ulong parallel_degree,
                                     parallel_scan_desc_t *scan_desc,
                                     ulong *ranges) {
  auto *table = tab->table();

  // Field is_asc set is complicated, set it in access path rewriter.
  scan_desc->is_asc = true;

  GetTableAccessInfo(tab, &scan_desc->keynr, &scan_desc->min_key,
                     &scan_desc->max_key, &scan_desc->is_ref_or_null,
                     &scan_desc->key_used);

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

static uint32 ChooseParallelDegreeByScanRange(
    uint32 parallel_degree, ulong parallel_scan_ranges_threshold,
    ulong ranges) {
  // Choosing disabled by system variables
  if (parallel_scan_ranges_threshold == 0 || parallel_degree == 1)
    return parallel_degree;

  // parallel disabled by insufficient ranges
  if (ranges < parallel_scan_ranges_threshold) return parallel_disabled;

  if (parallel_degree > ranges) parallel_degree = ranges;

  return parallel_degree;
}

static bool ChooseParallelScanTable(ParallelPlan *parallel_plan,
                                    uint parallel_table_index,
                                    uint32 *parallel_degree,
                                    bool *specified_by_hint,
                                    const char **cause) {
  auto *join = parallel_plan->SourceJoin();
  THD *thd = join->thd;
  ulong scan_ranges;
  parallel_scan_desc_t scan_desc;

  // Only support first table do parallel scan.
  // TODO: we can support other tables for HASH JOIN even there is no parallel
  // rescan.
  auto *qt = &join->qep_tab[parallel_table_index];

  // semi-join inner tables could not do parallel scan because we could
  // not eliminate duplicates between workers. we need a repartition with
  // unique.
  if (qt->first_sj_inner() != NO_PLAN_IDX) {
    *cause = "first_table_is_semijoin_inner_table";
    return true;
  }

  // First table can not be inner table of outer join.
  assert(!qt->is_inner_table_of_outer_join());

  // LOOSE SCAN needs to scan to eliminate duplicates, currently we can not
  // support prefix in PARALLEL SCAN. It should be blocks by
  // `first_table_is_semijoin_inner_table`
  assert(!qt->do_loosescan() && !qt->do_firstmatch());

  double parallel_plan_cost_threshold;
  ulonglong parallel_scan_records_threshold;
  ulong parallel_scan_ranges_threshold;
  auto *dist_adapter = parallel_plan->DistAdapter();
  dist_adapter->GetThresholdsForParallelScan(thd, &parallel_plan_cost_threshold,
                                             &parallel_scan_records_threshold,
                                             &parallel_scan_ranges_threshold);

  *parallel_degree = dist_adapter->GetTableParallelDegree(thd, qt->table_ref,
                                                          specified_by_hint);
  if (*parallel_degree == parallel_disabled) {
    *cause = *specified_by_hint ? "forbidden_by_parallel_hint"
                               : "max_parallel_degree_not_set";
    return true;
  }

  if (!*specified_by_hint) {
    // These "soft" limits are applied when user doesn't hint to use parallel

    if (join->best_read < parallel_plan_cost_threshold) {
      *cause = "plan_cost_less_than_threshold";
      return true;
    }
    // This should be in table_parallel_degree() but currently we only support
    // one table.
    if (qt->position()->rows_fetched < parallel_scan_records_threshold) {
      *cause = "table_records_less_than_threshold";
      return true;
    }
  }

  auto *parallel_table = qt->table();
  if (!(parallel_table->file->ha_table_flags() & HA_CAN_PARALLEL_SCAN)) {
    *cause = "first_table_does_not_support_parallel_scan";
    return true;
  }

  // Estimate parallel scan ranges, it will be displayed in EXPLAIN
  if (GetTableParallelScanInfo(qt, *parallel_degree, &scan_desc, &scan_ranges))
    return true;

  if (!*specified_by_hint &&
      (*parallel_degree = ChooseParallelDegreeByScanRange(
           *parallel_degree, parallel_scan_records_threshold,
           scan_ranges)) == parallel_disabled) {
    *cause = "insufficient_parallel_scan_ranges";
    return true;
  }
  auto *table = join->qep_tab[parallel_table_index].table();
  parallel_plan->SetTableParallelScan(table, scan_ranges, scan_desc);

  return false;
}

static bool ChooseExecNodes(ParallelPlan *parallel_plan,
                            uint parallel_table_index, uint32 parallel_degree,
                            bool forbid_degree_reduce, const char **cause) {
  auto *join = parallel_plan->SourceJoin();
  THD *thd = join->thd;
  auto *dist_adapter = parallel_plan->DistAdapter();
  bool need_pscan = dist_adapter->NeedParallelScan();
  dist::TableDistArray table_dists(thd->mem_root);
  table_dists.reserve(join->primary_tables);

  assert((parallel_degree != parallel_disabled &&
          parallel_degree != degree_unspecified) ||
         !need_pscan);

  for (uint i = 0; i < join->primary_tables; ++i) {
    auto qep_tab = &join->qep_tab[i];
    auto *table = qep_tab->table();
    uint keynr;
    key_range *min_key, *max_key;
    bool is_ref_or_null;
    uint16 key_used;
    GetTableAccessInfo(qep_tab, &keynr, &min_key, &max_key, &is_ref_or_null,
                       &key_used);
    auto *table_dist_desc = dist_adapter->GetTableDist(
        thd, table, keynr, min_key, max_key, is_ref_or_null);
    if (!table_dist_desc || table_dists.push_back(table_dist_desc)) return true;
  }
  auto *saved_table_dists = parallel_plan->SetTableDists(table_dists);

  // For sharding, currently we only support single partition push down and we
  // handle it in IsCompatibleWith()
  if (!need_pscan && join->primary_tables > 1) {
    dist::TableDist *prev_desc = nullptr;
    for (auto *dist_desc : table_dists) {
      if (!prev_desc) continue;
      if (!dist_desc->IsCompatibleWith(prev_desc)) {
        *cause = "could_not_push_to_single_partition";
        return true;
      }
    }
  }

  // Currently, we just use parallel scan table to determine execution nodes.
  auto *dist_desc = saved_table_dists->at(parallel_table_index);
  auto *store_nodes = dist_desc->GetStoreNodes();
  dist::NodeArray *exec_nodes = store_nodes;
  // TODO: should we make sure current node is in exec_nodes?
  if (need_pscan && exec_nodes->size() > parallel_degree) {
    exec_nodes =
        new (thd->mem_root) dist::NodeArray(thd->mem_root, *exec_nodes);
    std::sort(exec_nodes->begin(), exec_nodes->end(),
              [](auto *a, auto *b) { return a->weight() > b->weight(); });
    exec_nodes->resize(parallel_degree);
  }

  parallel_plan->SetExecNodes(exec_nodes);
  if (need_pscan) {
    if ((parallel_plan->AcquireParallelDegree(parallel_degree,
                                              forbid_degree_reduce))) {
      *cause = "too_many_parallel_workers";
      return true;
    }
  } else
    parallel_plan->SetParallelDegree(exec_nodes->size());

  return false;
}

static void ChooseParallelPlan(JOIN *join) {
  THD *thd = join->thd;
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object wrapper(trace);
  Opt_trace_object trace_choosing(trace, "considering");
  const char *cause = nullptr;

  auto trace_set_guard = create_scope_guard([&trace_choosing, &cause, join]() {
    assert(cause || join->thd->is_error());
    trace_choosing.add("chosen", false);
    trace_choosing.add_alnum(
        "cause", cause ? cause : join->thd->get_stmt_da()->message_text());
    if (join->parallel_plan) {
      destroy(join->parallel_plan);
      join->parallel_plan = nullptr;
    }
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

  // We just support single table
  // TODO: remove this block when deparser supports JOIN.
  if (join->primary_tables != 1) {
    cause = "not_single_table";
    return;
  }

  if (join->const_tables > 0) {
    cause = "plan_with_const_tables";
    return;
  }

  if (!thd->parallel_query_switch_flag(PARALLEL_QUERY_JOIN) &&
      join->primary_tables != 1) {
    cause = "not_single_table";
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

  auto *dist_adapter = dist::CreateSpiderAdapter(thd->mem_root);
  if (!dist_adapter) return;

  // Block parallel query based on table properties
  for (uint i = 0; i < join->primary_tables; i++) {
    auto *qt = &join->qep_tab[i];
    auto *table_ref = qt->table_ref;

    if (table_ref->is_placeholder()) {
      cause = "table_is_not_real_user_table";
      return;
    }

    // Materialization semijoin is blocked by this
    if (is_temporary_table(table_ref)) {
      cause = "temporary_table_is_not_supported";
      return;
    }

    auto *table = qt->table();
    // Distribution system may refuse some table to parallel, e.g. a local table
    // in a sharding system and currently 3.0 does not enable parallel query to
    // system table.
    if ((cause = dist_adapter->TableRefuseParallel(table))) return;

    // Weed out plan execution depends on QEP_TAB, so block it.
    if (qt->get_sj_strategy() == SJ_OPT_DUPS_WEEDOUT) {
      cause = "duplicateweedout_semijoin_plan_is_not_supported";
      return;
    }

    if ((cause = TableAccessTypeRefuseParallel(qt))) return;
  }

  // Block parallel query based on item expressions

  const char *item_refuse_cause;
  for (Item *item : join->query_block->fields) {
    if (!ItemRefuseParallel(item, &item_refuse_cause)) continue;
    cause = item_refuse_cause ? item_refuse_cause
                              : "include_unsafe_items_in_fields";
    return;
  }

  for (uint i = 0; i < join->tables; ++i) {
    auto *qep_tab = &join->qep_tab[i];
    if (!qep_tab->table()) break;

    if (qep_tab->condition() &&
        ItemRefuseParallel(qep_tab->condition(), &item_refuse_cause)) {
      cause =
          item_refuse_cause ? item_refuse_cause : "filter_has_unsafe_condition";
      return;
    }
    Item *item = qep_tab->table()->file->pushed_idx_cond;
    if (item && ItemRefuseParallel(item, &item_refuse_cause)) {
      cause = item_refuse_cause ? item_refuse_cause
                                : "filter_pushed_idx_cond_has_unsafe_condition";
      return;
    }

    if (qep_tab->having &&
        ItemRefuseParallel(qep_tab->having, &item_refuse_cause)) {
      cause = item_refuse_cause ? item_refuse_cause
                                : "table_having_has_unsafe_condition";
      return;
    }
  }

  if (join->having_cond &&
      ItemRefuseParallel(join->having_cond, &item_refuse_cause)) {
    cause = item_refuse_cause ? item_refuse_cause : "having_is_unsafe";
    return;
  }

  if (!(join->parallel_plan = new (thd->mem_root) ParallelPlan(join))) return;
  auto *parallel_plan = join->parallel_plan;
  parallel_plan->SetDistAdapter(dist_adapter);

  constexpr int parallel_scan_table_index = 0;
  uint32 parallel_degree = degree_unspecified;
  bool specified_by_hint = false;
  if (dist_adapter->NeedParallelScan() &&
      ChooseParallelScanTable(parallel_plan, parallel_scan_table_index,
                              &parallel_degree, &specified_by_hint, &cause))
    return;

  if (ChooseExecNodes(parallel_plan, parallel_scan_table_index, parallel_degree,
                      specified_by_hint, &cause))
    return;

  trace_choosing.add("chosen", true);
  trace_set_guard.commit();
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

THD *PartialPlan::thd() const { return m_query_block->join->thd; }

void PartialPlan::SetTableParallelScan(TABLE *table, ulong suggested_ranges,
                                       const parallel_scan_desc_t &psdesc) {
  m_parallel_scan_info = {table, suggested_ranges, psdesc};
}

uint ParallelDegreeHolder::acquire(uint degree, bool forbid_reduce) {
  m_degree = AcquireParallelWorkers(degree, forbid_reduce);
  return m_degree;
}

ParallelDegreeHolder::~ParallelDegreeHolder() {
  if (m_degree == 0 || !m_acquired_from_global) return;

  ReleaseParallelWorkers(m_degree);
}

ParallelPlan::ParallelPlan(JOIN *join)
    : m_join(join), m_fields(join->thd->mem_root) {}

ParallelPlan::~ParallelPlan() {
  for (auto *table_dist : m_table_dists) destroy(table_dist);
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
  assert(join->rollup_state == JOIN::RollupState::NONE);

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

    assert(from->type() == Item::FIELD_ITEM);

    // Find table field by identifier for pushed down item
    uint same_field_index = 0;
    Item *same_field_item = nullptr;
    uint i = 0;
    for (auto *item : m_partial_fields) {
      if (item_field->is_identical(item)) {
        same_field_index = i;
        same_field_item = item;
        break;
      }

      /// See is_item_field_in_fields(), we omit duplicated fields for reducing
      /// pushed down fields. Here we don't break if one field found because we
      /// hope it can match with its identical field (with same id) if it is in
      /// fields.
      if (!same_field_item && item->type() == Item::FIELD_ITEM &&
          from->field == down_cast<Item_field *>(item)->field) {
        same_field_index = i;
        same_field_item = item;
      }

      ++i;
    }

    assert(same_field_item);

    item_field->field_index = same_field_index;
    item_field->field = m_collector_table->field[same_field_index];
    item_field->set_result_field(item_field->field);
    if (!item_field->is_identical(same_field_item))
      item_field->set_id(same_field_item->id());
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

    // Change origin plan: update original base_ref_items with new items. We
    // need use THD::change_item_tree() for multiple time execution of
    // prepare.
    bool found [[maybe_unused]] = false;
    for (size_t j = 0; j < source_query_block->fields.size(); j++) {
      if (source_query_block->base_ref_items[j]->is_identical(new_item)) {
        thd->change_item_tree(&source_query_block->base_ref_items[j], new_item);
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

  if (!m_dist_adapter->MakePartialDistPlan(&m_partial_plan, m_exec_nodes))
    return true;
  assert(m_exec_nodes->size() > 0 || m_dist_adapter->NeedParallelScan());

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
  assert(ParallelDegree() <= max_parallel_degree_limit);
  if (!(m_collector = new (thd->mem_root) Collector(
            m_dist_adapter, m_exec_nodes, &m_partial_plan, ParallelDegree())) ||
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

bool ParallelPlan::GenerateAccessPath(Item_clone_context *clone_context) {
  JOIN *source_join = SourceJoin();
  THD *thd = source_join->thd;
  AccessPath *partial_path;
  AccessPath *parallelized_path;
  AccessPathParallelizer rewriter(clone_context, source_join, &m_partial_plan);
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "access_path_rewriting");

  assert(source_join->root_access_path());

  if (thd->lex->is_explain_analyze)
    rewriter.set_fake_timing_iterator(NewFakeTimingIterator(thd, m_collector));

  if (!(parallelized_path = rewriter.parallelize_access_path(
            m_collector, source_join->root_access_path(), partial_path)))
    return true;
  assert(partial_path != nullptr);

  if (source_join->root_access_path() != parallelized_path)
    source_join->set_root_access_path(parallelized_path);

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

  if (!(m_root_access_path =
            aprewriter.clone_and_rewrite(from->root_access_path())))
      return true;

  if (thd->is_error()) return true;

  set_plan_state(PLAN_READY);

  return false;
}
