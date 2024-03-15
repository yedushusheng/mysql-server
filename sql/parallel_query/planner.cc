#include "sql/parallel_query/planner.h"

#include "scope_guard.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/nested_join.h"
#include "sql/opt_range.h"
#include "sql/opt_trace.h"
#include "sql/parallel_query/distribution.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/rewrite_access_path.h"
#include "sql/sql_optimizer.h"
#include "sql/tdsql/trans.h"

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

static bool ItemRefuseParallel(const Item *item, bool deny_outer_ref,
                               const char **cause) {
  assert(item);
  *cause = nullptr;
  if (item->parallel_safe() != Item_parallel_safe::Safe) return true;
  // Some Item_refs in refer to outer table's field, we don't support that yet.
  if (deny_outer_ref && (item->used_tables() & OUTER_REF_TABLE_BIT)) {
    *cause = "item_refer_to_outer_query_field";
    return true;
  }

  return false;
}
enum class PlanExprPlace { FIELDS, FILTER, HAVING, SJ_INNER_EXPRS };

/**
  Traverse every item of one query plan, include fields of Query_block and
  JOIN, calling func() for each one. If func() returns true, traversal is
  stopped early.

  func() must have signature func(Item *, PlanExprPlace), the last parameter is
   place of item.
*/
template <typename Func>
bool walk_plan_expressions(const Query_block *query_block, Func &&func) {
  for (Item *item : query_block->fields) {
    if (func(item, PlanExprPlace::FIELDS)) return true;
  }
  auto *join = query_block->join;
  if (!join) return false;

  if (join->having_cond && func(join->having_cond, PlanExprPlace::HAVING))
    return true;

  // No tables or Select tables optimized away. See zero_result_cause usages.
  if (!join->qep_tab) return false;

  for (uint i = 0; i < join->tables; ++i) {
    auto *qt = &join->qep_tab[i];

    if (qt->having && func(qt->having, PlanExprPlace::FILTER)) return true;

    if (qt->condition() && func(qt->condition(), PlanExprPlace::FILTER))
      return true;

    auto *table = qt->table();
    if (!table) continue;

    Item *pushed_cond = table->file->pushed_idx_cond;
    if (pushed_cond && func(pushed_cond, PlanExprPlace::FILTER)) return true;

    // Walk on TABLE_REF finally
    if (qt->type() < JT_CONST || qt->type() > JT_REF) continue;
    auto &ref = qt->ref();
    if (!ref.key_copy || !(table->key_info + ref.key)) continue;
    for (uint kpart = 0; kpart < ref.key_parts; ++kpart) {
      if (func(ref.items[kpart], PlanExprPlace::FILTER)) return true;
    }
  }

  // Walk semi-join materialization items too
  for (auto &sjm : join->sjm_exec_list) {
    for (auto *item : sjm.sj_nest->nested_join->sj_inner_exprs) {
      if (func(item, PlanExprPlace::SJ_INNER_EXPRS)) return true;
    }
  }

  return false;
}

static bool IsAccessRangeSupported(QEP_TAB *qt) {
  assert(qt->type() == JT_RANGE);

  auto quick_type = qt->quick()->get_type();
  return quick_type == QUICK_SELECT_I::QS_TYPE_RANGE ||
         quick_type == QUICK_SELECT_I::QS_TYPE_RANGE_DESC ||
         quick_type == QUICK_SELECT_I::QS_TYPE_SKIP_SCAN;
  // TODO: support GROUP_MIN_MAX;
}

static bool IsAccessSupportedForPartition(QEP_TAB *qt) {
  // Parallel queries on partition table cannot be REF_OR_NULL, SKIP_SCAN or
  // GROUP_MIN_MAX because these 3 access paths are no coherent scans.
  auto typ = qt->type();
  if (typ == JT_REF_OR_NULL) return false;
  if (typ == JT_RANGE) {
    auto quick_type = qt->quick()->get_type();
    if (quick_type == QUICK_SELECT_I::QS_TYPE_GROUP_MIN_MAX) {
      return false;
    }
  }
  return true;
}

///  Check supported JOIN_TYPE. NOTE, temporary table's type could be
/// JT_UNKNOWN, but it does not block parallel.
static const char *TableAccessTypeRefuseParallel(QEP_TAB *qt) {
  auto typ = qt->type();

  if (qt->table()->part_info && !IsAccessSupportedForPartition(qt)) {
    return "unsupported_access_for_partition_table";
  }

  // Currently, we don't support JT_SYSTEM, JT_FT and JT_INDEX_MERGE.
  if (typ == JT_SYSTEM || typ == JT_FT || typ == JT_INDEX_MERGE)
    return "table_access_type_is_not_supported";

  if (qt->dynamic_range()) {
    assert(typ == JT_ALL || typ == JT_RANGE /* || typ == JT_INDEX_MERGE */);
    return "dynamic_range_access_is_not_supported";
  }

  if (typ == JT_RANGE && !IsAccessRangeSupported(qt))
    return "unsupported_access_range_type";

  return nullptr;
}

static void get_scan_range_for_ref(MEM_ROOT *mem_root, TABLE_REF *ref,
                                   uint *keynr, key_range **min_key,
                                   key_range **max_key, bool ref_or_null) {
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

static void GetTableAccessInfo(QEP_TAB *tab, uint *keynr, key_range **min_key,
                        key_range **max_key, uint16 *key_used,
                        parallel_scan_flags *flags) {
  auto *thd = tab->join()->thd;

  *key_used = UINT16_MAX;
  switch (tab->type()) {
    case JT_ALL:
      *min_key = *max_key = nullptr;
      *keynr = MAX_KEY;
      break;
    case JT_INDEX_SCAN:
      *min_key = *max_key = nullptr;
      *keynr = tab->index();
      break;
    case JT_REF_OR_NULL:
      *flags |= parallel_scan_flags::ref_or_null;
      [[fallthrough]];
    case JT_CONST:
      [[fallthrough]];
    case JT_REF:
      [[fallthrough]];
    case JT_EQ_REF:
      if (tab->ref().depend_map != 0)
        *flags |= parallel_scan_flags::preassign_ranges;
      get_scan_range_for_ref(thd->mem_root, &tab->ref(), keynr, min_key,
                             max_key,
                             (*flags) & parallel_scan_flags::ref_or_null);
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

  // Field is_asc set is complicated, set it in access path re-writer.
  scan_desc->flags = parallel_scan_flags::none;
  GetTableAccessInfo(tab, &scan_desc->keynr, &scan_desc->min_key,
                     &scan_desc->max_key, &scan_desc->key_used,
                     &scan_desc->flags);
  *ranges = 100 * parallel_degree;
  int res;
  if ((res = table->file->estimate_parallel_scan_ranges(
           scan_desc->keynr, scan_desc->min_key, scan_desc->max_key,
           scan_desc->key_used, scan_desc->flags, 0, ranges)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  // It will be set to correct value before handle::init_parallel_scan() is
  // called.
  scan_desc->degree = 0;

  return false;
}

static uint32 ChooseParallelDegreeByScanRange(
    uint32 parallel_degree, ulong parallel_scan_ranges_threshold,
    ulong ranges, const char **cause) {
  // Choosing disabled by system variables
  if (parallel_scan_ranges_threshold == 0 || parallel_degree == 1)
    return parallel_degree;

  // parallel disabled by insufficient ranges
  if (ranges < parallel_scan_ranges_threshold) {
    *cause = "insufficient_parallel_scan_ranges";
    return parallel_disabled;
  }
  if (parallel_degree > ranges) parallel_degree = ranges;

  return parallel_degree;
}

static bool IsWeedoutRowidsTable(SJ_TMP_TABLE *sj_tmp_table, QEP_TAB *qt) {
  if (sj_tmp_table->is_confluent) return false;

  for (auto *sjt = sj_tmp_table->tabs; sjt != sj_tmp_table->tabs_end; sjt++) {
    if (qt == sjt->qep_tab) return true;
  }

  return false;
}

bool ParallelRefuseByThresholds(THD *thd, dist::Adapter *dist_adapter,
                                QEP_TAB *qep_tab, uint32 *parallel_degree,
                                parallel_scan_desc_t *scan_desc,
                                ulong *scan_ranges,
                                const char **cause) {
  auto *join = qep_tab->join();
  double parallel_plan_cost_threshold;
  ulonglong parallel_scan_records_threshold;
  ulong parallel_scan_ranges_threshold;
  dist_adapter->GetThresholdsForParallelScan(thd, &parallel_plan_cost_threshold,
                                             &parallel_scan_records_threshold,
                                             &parallel_scan_ranges_threshold);
  if (join->best_read < parallel_plan_cost_threshold) {
    *cause = "plan_cost_less_than_threshold";
    return true;
  }
  // This should be in table_parallel_degree() but currently we only support
  // one table.
  if (qep_tab->position()->rows_fetched < parallel_scan_records_threshold) {
    *cause = "table_records_less_than_threshold";
    return true;
  }

  // Estimate parallel scan ranges
  if (GetTableParallelScanInfo(qep_tab, *parallel_degree, scan_desc,
                               scan_ranges)) {
    *cause = "fail_to_get_parallel_scan_info_from_engine";
    return true;
  }

  auto degree = ChooseParallelDegreeByScanRange(
      *parallel_degree, parallel_scan_records_threshold, *scan_ranges, cause);
  if (degree == parallel_disabled) return true;

  *parallel_degree = degree;

  return false;
}

bool ChooseParallelScanTable(ParallelPlan *parallel_plan, QEP_TAB *&qep_tab,
                             uint32 *parallel_degree, bool *specified_by_hint,
                             const char **cause) {
  auto *join = parallel_plan->SourceJoin();
  THD *thd = join->thd;
  auto *dist_adapter = parallel_plan->DistAdapter();
  *parallel_degree = dist_adapter->DefaultParallelDegree(thd);

  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_array trace_tables(trace, "considered_parallel_scan_tables");

  // Here we can use join->tables if we implemented parallel scan for
  // internal tmp table storage engines.
  ulong chosen_scan_ranges = 0;
  parallel_scan_desc_t chosen_scan_desc;
  qep_tab = nullptr;
  SJ_TMP_TABLE *weedout_tmp_table = nullptr;
  for (uint i = 0; i < join->primary_tables; ++i) {
    auto *qt = &join->qep_tab[i];
    bool table_by_hint = false;
    auto table_degree =
        dist_adapter->TableParallelHint(thd, qt->table_ref, &table_by_hint);
    if (qt->flush_weedout_table) weedout_tmp_table = qt->flush_weedout_table;

    Opt_trace_object trace_qt(trace);
    const char *refuse_cause = nullptr;
    auto trace_set_guard =
        create_scope_guard([&trace_qt, &refuse_cause, &table_by_hint]() {
          if (!refuse_cause) {
            if (table_by_hint) trace_qt.add("force_by_parallel_hint", true);
            trace_qt.add("usable", true);
            return;
          }
          trace_qt.add("usable", false);
          trace_qt.add_alnum("cause", refuse_cause);
        });
    trace_qt.add_utf8_table(qt->table_ref);

    // Hard restriction of parallel scan

    // Normally, The table is semjoin inner table if first_sj_inner table is
    // valid, but it is not true for duplicates weedout.
    bool weedout_non_rowid_table =
        weedout_tmp_table && !IsWeedoutRowidsTable(weedout_tmp_table, qt);
    if (qt->check_weed_out_table) {
      assert(weedout_tmp_table);
      weedout_tmp_table = nullptr;
    }
    if (weedout_non_rowid_table ||
        (qt->get_sj_strategy() > SJ_OPT_DUPS_WEEDOUT &&
         qt->first_sj_inner() != NO_PLAN_IDX)) {
      refuse_cause = "inner_table_of_semijoin";
      continue;
    }

    if (!(qt->table()->file->ha_table_flags() & HA_CAN_PARALLEL_SCAN)) {
      refuse_cause = "engine_can_not_do_parallel_scan";
      continue;
    }

    if (qt->table()->correlate_src_table) {
      refuse_cause = "an_online_DDL_is_in_progress";
      continue;
    }

    if (qt->is_inner_table_of_outer_join()) {
      refuse_cause = "inner_table_of_outer_join";
      continue;
    }

    // LOOSE SCAN needs to scan to eliminate duplicates, currently we can not
    // support prefix in PARALLEL SCAN. It should be blocks by
    // `first_table_is_semijoin_inner_table`
    assert(!qt->do_loosescan() && !qt->do_firstmatch());

    if (table_degree == parallel_disabled) {
      refuse_cause = "disabled_by_parallel_hint";
      continue;
    }

    if (table_degree == degree_unspecified)
      table_degree = *parallel_degree;
    else
      *specified_by_hint = true;

    if (table_degree == parallel_disabled) {
      refuse_cause = "parallel_degree_is_unspecified";
      continue;
    }

    bool force_by_hint = table_by_hint || *specified_by_hint;
    parallel_scan_desc_t scan_desc;
    ulong scan_ranges;
    if (force_by_hint) {
      if (GetTableParallelScanInfo(qt, table_degree, &scan_desc, &scan_ranges))
        return true;

      if (table_by_hint) {
        qep_tab = qt;
        *parallel_degree = table_degree;

        chosen_scan_desc = scan_desc;
        chosen_scan_ranges = scan_ranges;
        break;
      }
    } else if (ParallelRefuseByThresholds(thd, dist_adapter, qt, &table_degree,
                                          &scan_desc, &scan_ranges,
                                          &refuse_cause))
      continue;

    *parallel_degree = table_degree;
    if (qep_tab) continue;  // the first one wins.

    qep_tab = qt;
    chosen_scan_desc = scan_desc;
    chosen_scan_ranges = scan_ranges;
  }

  if (!qep_tab) {
    *cause = "no_parallel_table_found";
    return true;
  }

  parallel_plan->SetTableParallelScan(qep_tab->table(), chosen_scan_ranges,
                                      chosen_scan_desc);
  return false;
}

static bool ChooseExecNodes(ParallelPlan *parallel_plan,
                            QEP_TAB *parallel_table, uint32 parallel_degree,
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
    uint16 key_used;
    parallel_scan_flags flags = parallel_scan_flags::none;
    GetTableAccessInfo(qep_tab, &keynr, &min_key, &max_key, &key_used, &flags);
    auto *table_dist_desc =
        dist_adapter->GetTableDist(thd, table, keynr, min_key, max_key, flags);
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
  auto *dist_desc = saved_table_dists->at(parallel_table - join->qep_tab);
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

static bool skip_no_used_field(Query_expression *unit, Item *item) {
  if (!unit->item) return false;
  return unit->item->engine_type() == Item_in_subselect::HASH_SJ_ENGINE &&
         item->created_by_in2exists();
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

  if (GetTdsqlTransId(thd) == 0) {
    cause = "not_supported_inactive_tdstore_transaction";
    return;
  }

  // SELECT command is checked inside tdsql::ReadWithoutTrans()
  if (!tdsql::ReadWithoutTrans(thd)) {
    cause = "not_supported_tdstore_transaction_with_participants";
    return;
  }

  // Block uncacheable subqueries, Note some converted subqueries may also
  // blocked.
  if (!join->query_block->is_cacheable()) {
    cause = "not_supported_uncacheable_subquery";
    return;
  }

  // Block plan which does't support yet
  if (join->zero_result_cause) {
    cause = "plan_with_zero_result";
    return;
  }

  if ((cause = join->query_block->parallel_safe(true))) return;

  // non-parallel scan depends on QEP_TAB deeply see GetStartEndKey() in
  // ha_rocksdb.cc
  if (join->select_count &&
      !thd->parallel_query_switch_flag(PARALLEL_QUERY_SELECT_COUNT)) {
    cause = "select_count_is_switched_off";
    return;
  }

  if (!thd->parallel_query_switch_flag(PARALLEL_QUERY_JOIN) &&
      join->primary_tables != 1) {
    cause = "join_is_switched_off";
    return;
  }

  auto *dist_adapter = dist::CreateTDStoreAdapter(thd->mem_root);
  if (!dist_adapter) return;

  // Loop on each table to block unsupported query
  for (uint i = 0; i < join->tables; i++) {
  
    // Distribution system may refuse some table to parallel, e.g. a local table
    // in a sharding system and currently 3.0 does not enable parallel query to
    // system table.
    auto *table = join->qep_tab[i].table();
    if (!table) continue;

    if ((cause = dist_adapter->TableRefuseParallel(table))) return;
  }

  // Make sure semi-join materialization items parallel safe, we use them to
  // create materialization temporary tables in partial plan, currently.
  for (auto &sjm : join->sjm_exec_list) {
    for (auto *item : sjm.sj_nest->nested_join->sj_inner_exprs) {
      if (!(ItemRefuseParallel(item, &item_refuse_cause))) continue;
      cause = item_refuse_cause ? item_refuse_cause : "unsafe_sj_inner_exprs";
      return;
    }
  }

  if (!(join->parallel_plan = new (thd->mem_root) ParallelPlan(join))) return;
  auto *parallel_plan = join->parallel_plan;
  parallel_plan->SetDistAdapter(dist_adapter);

  // First table do parallel scan if no hint is specified.
  QEP_TAB *parallel_table = join->qep_tab;
  uint32 parallel_degree = degree_unspecified;
  bool specified_by_hint = false;
  if (dist_adapter->NeedParallelScan() &&
      ChooseParallelScanTable(parallel_plan, parallel_table, &parallel_degree,
                              &specified_by_hint, &cause))
    return;

  if (ChooseExecNodes(parallel_plan, parallel_table, parallel_degree,
                      specified_by_hint, &cause))
    return;

  trace_choosing.add("chosen", true);
  trace_set_guard.commit();
}

bool GenerateParallelPlan(JOIN *join) {
  THD *thd = join->thd;

  ChooseParallelPlan(join);

  ParallelPlan *parallel_plan = join->parallel_plan;

  if (!parallel_plan) return false;

  assert(!thd->is_error());
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object trace_gen_wrapper(trace);
  Opt_trace_object trace_generating(trace, "generating");
  Opt_trace_array trace_gen_steps(trace, "steps");

  return parallel_plan->Generate();
}

ItemRefCloneResolver::ItemRefCloneResolver(MEM_ROOT *mem_root)
    : m_refs_to_resolve(mem_root) {}

bool ItemRefCloneResolver::resolve(Item_clone_context *context, Item_ref *item,
                                   const Item_ref *from) {
  assert(from->ref_pointer());
  return m_refs_to_resolve.push_back({item, from, context->query_block()});
}

#ifndef NDEBUG
// Assert for inline clone. an Item_ref fail to resolve, there are 2 cases
// currently, a. refer to a unpushed item; b. a merged derived leaves an
// item_ref point to its own base_ref_items.
static bool item_is_const_or_in_merged_deriveds(const Item *item,
                                                Query_block *query_block) {
  if (item->const_item()) return true;
  for (TABLE_LIST *tl = query_block->get_table_list(); tl;
       tl = tl->next_local) {
    if (!tl->is_view_or_derived() && !tl->is_merged()) continue;
    for (Field_translator *transl = tl->field_translation;
         transl < tl->field_translation_end; transl++) {
      if (transl->item == item) return true;
    }
  }

  return false;
}
#endif

bool ResolveItemRefByInlineClone(Item_ref *item_ref,
                                        const Item_ref *from,
                                        Item_clone_context *context) {
  Item **ref_item;
  if (!(ref_item = (Item **)new (context->mem_root()) Item **) ||
      !(*ref_item = from->ref_item()->clone(context)))
    return true;
  item_ref->set_ref_pointer(ref_item);
  return false;
}

bool ItemRefCloneResolver::final_resolve(Item_clone_context *context) {
  for (auto &ref_info : m_refs_to_resolve) {
    auto *item_ref = ref_info.ref;
    auto *from = ref_info.orig_ref;
    auto *query_block = ref_info.query_block;
    auto &base_ref_items = query_block->base_ref_items;
    for (uint i = 0; i < query_block->fields.size(); i++) {
      auto *item = base_ref_items[i];
      if (item->is_identical(from->ref_item())) {
        item_ref->set_ref_pointer(&base_ref_items[i]);
        break;
      }
    }
    if (!item_ref->ref_pointer()) {
      assert(!query_block_for_inline_clone_assert ||
             item_is_const_or_in_merged_deriveds(
                 from, query_block_for_inline_clone_assert));

      if (ResolveItemRefByInlineClone(item_ref, from, context)) return true;
    }
    assert(item_ref->ref_item());
  }

  return false;
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

void PartialPlan::SetParallelScanReverse() {
  m_parallel_scan_info.scan_desc.flags |= parallel_scan_flags::desc;
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

JOIN *ParallelPlan::PartialJoin() const { return m_partial_plan.Join(); }

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
 public:
  using Item_clone_context::Item_clone_context;

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
    // Here we should use deep clone here because we needs handle subqueries
    // pre-evaluation or pushdown if its ref_item includes any of them.
    if (ResolveItemRefByInlineClone(item, from, this)) return true;
    item->set_first_inner_table(from->get_first_inner_table());
    return false;
  }

  Item *get_replacement_item(const Item *item) override {
    if (item->type() != Item::SUBSELECT_ITEM) return nullptr;
    auto *subq = down_cast<const Item_subselect *>(item);
    if (!subq->is_uncacheable())
      return AddCachedSubquery(const_cast<Item_subselect *>(subq));

    // Other sub-queries should be pushed down unless wrong parallel plan is
    // generated.
    auto *sub_unit = down_cast<const Item_subselect *>(item)->unit;
    assert(sub_unit->first_query_block() && sub_unit->outer_query_block());
    // For consistency, also mark its evaluation policy.
    sub_unit->set_parallel_pushdown_evaluation();
    m_pushdown_subselects.push_back(sub_unit);
    return nullptr;
  }

  /**
    Call this function to finish partial plan generation

    Do following jobs:
    - Resolve all Item_ref items
    - collect cached sub-queries and mark all child query expressions in pushed
      down sub-queries.
  */
  bool EndPartialPlanGen() {
    // Resolve all Item_ref finally.
    if (final_resolve_refs()) return true;

    // Collect all cached sub-queries from push down sub-queries otherwise
    // partial plan may includes some sub-queries' parallel plan.
    for (auto &unit : m_pushdown_subselects) {
      for (auto *inner_select = unit.first_query_block(); inner_select;
           inner_select = inner_select->next_query_block()) {
        if (CollectCachedSubqueries(inner_select)) return true;
      }
    }

    return false;
  }
  CachedSubselectList &CachedSubselects() { return m_cached_subselects; }
  QueryExpressionList &PushdownSubselects() { return m_pushdown_subselects; }

 private:
  /**
    Save a cached sub-query if it is not in list yet and mark its evaluation
    policy in its query expression.
  */
  Item *AddCachedSubquery(Item_subselect *subselect) {
    for (auto &item : m_cached_subselects) {
      if (item.subselect() == subselect) return &item;
    }

    auto *cached_subselect =
        Item_cached_subselect_result::create_from_subselect(subselect);
    if (!cached_subselect || m_cached_subselects.push_back(cached_subselect))
      return nullptr;

    subselect->unit->set_parallel_pre_evaluation();

    return cached_subselect;
  }

  /**
    Walk in an item to collect sub-queries recursively and mark parallel
    evaluation policy for each query expressions.
  */
  bool walk_for_subqueries_from_item(Item *item_arg) {
    return WalkItem(item_arg, enum_walk::PREFIX, [this](Item *item) {
      if (item->type() != Item::SUBSELECT_ITEM) return false;
      auto *subq = down_cast<Item_subselect *>(item);
      if (subq->substype() != Item_subselect::SINGLEROW_SUBS) return false;
      if (!subq->is_uncacheable()) {
        if (!AddCachedSubquery(subq)) return true;
        return false;
      }

      subq->unit->set_parallel_pushdown_evaluation();

      for (auto *inner_select = subq->unit->first_query_block(); inner_select;
           inner_select = inner_select->next_query_block()) {
        if (CollectCachedSubqueries(inner_select)) return true;
      }

      return false;
    });
  }

  /**
    Collect cached (pre-evaluate) sub-queries in @param source_query_block
    includes its child plan tree. Also mark evaluation policy for each query
    expression.
  */
  bool CollectCachedSubqueries(Query_block *source_query_block) {
    return walk_plan_expressions(source_query_block, [this](Item *item, auto) {
      assert(item->parallel_safe() == Item_parallel_safe::Safe);
      return walk_for_subqueries_from_item(item);
    });
  }

  CachedSubselectList m_cached_subselects;
  QueryExpressionList m_pushdown_subselects;
};

static bool is_order_prefix_by_key_fields(ORDER *order, KEY *key,
                                          uint16_t &key_prefix) {
  ORDER *ord = order;
  unsigned keypart_idx = 0;
  unsigned end_parts = key->user_defined_key_parts;
  assert(key_prefix == UINT16_MAX);

  // Currently, only support the parallel scan key with HA_NOSAME flag.
  if (!(key->flags & HA_NOSAME)) return false;

  for (; keypart_idx < end_parts && ord; ++keypart_idx, ord = ord->next) {
    auto *item = (*ord->item)->real_item();
    if (item->type() != Item::FIELD_ITEM ||
        !key->key_part[keypart_idx].field->eq(
            down_cast<Item_field *>(item)->field))
      return false;
  }

  // No prefix at all. We only support all key parts as the key_prefix due to
  // current parallel scan limitation.
  if (keypart_idx == 0 || keypart_idx != end_parts) return false;
  // We also set key_used even if prefix includes all fields because underlying
  // storage engines has different implementation of parallel scan.
  key_prefix = keypart_idx - 1;
  return true;
}

static AggregateStrategy decide_aggregate_strategy(JOIN *join, TABLE *ps_table,
                                                   uint ps_keynr,
                                                   uint16_t &ps_key_used) {
  auto *group = join->group_list.actual_used();
  // It must be GROUP MIN MAX scan if ps_key_used is valid. XXX We can add
  // aggregate access path in access path rewriter if the full_grouping_pushdown
  // switch is off.
  if (ps_key_used != UINT16_MAX) {
    assert(join->primary_tables == 1 && join->qep_tab->type() == JT_RANGE &&
           join->qep_tab->quick()->get_type() ==
               QUICK_RANGE_SELECT::QS_TYPE_GROUP_MIN_MAX);
    // The group list can be empty when GROUP MIN MAX is used for AGG(distinct
    // ...), see get_best_group_min_max(). Here group_list of query block used
    // because the group list can be optimized out due to
    // equals_constant_in_where but we don't want leave agg functions to final
    // plan. We also can not set it to used_order of JOIN::group_list since it
    // should not appear to parallel plan.
    return group || join->query_block->group_list.size() > 0
               ? AggregateStrategy::PushedOnePhase
               : AggregateStrategy::TwoPhase;
  }

  THD *thd = join->thd;
  KEY *key = &ps_table->key_info[ps_keynr];
  if (thd->parallel_query_switch_flag(PARALLEL_QUERY_FULL_GROUPING_PUSHDOWN) &&
      group && ps_keynr != MAX_KEY &&
      is_order_prefix_by_key_fields(group, key, ps_key_used))
    return AggregateStrategy::PushedOnePhase;

  // Only OnePhase strategy can be used if there are aggregate functions.
  for (auto **sum_func_ptr = join->sum_funcs; *sum_func_ptr; sum_func_ptr++) {
    if ((*sum_func_ptr)->has_with_distinct())
      return AggregateStrategy::OnePhase;
  }

  return AggregateStrategy::TwoPhase;
}

static AggregateStrategy decide_aggregate_strategy(JOIN *join) {
  bool group_scan = false;
  if (join->primary_tables == 1) {
    auto *qt = &join->qep_tab[0];
    if (qt->type() == JT_RANGE &&
        qt->quick()->get_type() == QUICK_SELECT_I::QS_TYPE_GROUP_MIN_MAX)

      group_scan = true;
  }

  // Check if there are distinct aggregation functions, e.g.
  // COUNT(DISTINCT). There is a specify case: distinct aggregation
  // functions + group min max scan should use a two phase aggregation.
  for (auto **sum_func_ptr = join->sum_funcs; *sum_func_ptr; sum_func_ptr++) {
    if (!(*sum_func_ptr)->has_with_distinct()) continue;

    return group_scan ? AggregateStrategy::TwoPhase
                      : AggregateStrategy::OnePhase;
  }

  return group_scan ? AggregateStrategy::PushedOnePhase
                    : AggregateStrategy::TwoPhase;
}

bool ParallelPlan::GenPartialFields(Item_clone_context *context,
                                    FieldsPushdownDesc *fields_pushdown_desc) {
  Query_block *source = SourceQueryBlock();
  Query_block *partial_query_block = PartialQueryBlock();
  JOIN *join = SourceJoin();

  bool sumfuncs_full_pushdown =
      m_aggregate_strategy == AggregateStrategy::PushedOnePhase;

  for (uint i = 0; i < source->fields.size(); i++) {
    Item *new_item, *item = source->fields[i];
    if (skip_no_used_field(join->query_expression(), item)) continue;
    if (can_item_push_down(m_aggregate_strategy, item)) {
      if (!(new_item = item->clone(context))) return true;
      bool is_sum_func = item->type() == Item::SUM_FUNC_ITEM;
      if (is_sum_func && !sumfuncs_full_pushdown) {
        Item_sum *item_sum = down_cast<Item_sum *>(new_item);
        item_sum->set_sum_stage(Item_sum::TRANSITION_STAGE);
        // change item_name so let EXPLAIN doesn't show Sum(Sum(a)) for
        // leader's aggregation
        Item *arg = item_sum->get_arg(0);
        item_sum->item_name = arg->item_name;
      }
      if (partial_query_block->add_item_to_list(new_item)) return true;
      if (fields_pushdown_desc->push_back(
              FieldPushdownDesc(item, !is_sum_func || sumfuncs_full_pushdown
                                          ? FieldPushdownDesc::Replace
                                          : FieldPushdownDesc::Clone)))
        return true;
    } else {
      FieldPushdownDesc pushdown_desc(item, FieldPushdownDesc::Clone);
      mem_root_deque<Item_field *> field_list(thd()->mem_root);
      enum_walk walk{enum_walk::PREFIX | enum_walk::ITEM_CLONE};
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
bool ParallelPlan::CloneSemiJoinExecList(Item_clone_context *context) {
  auto *join = PartialQueryBlock()->join;
  auto *source_join = SourceQueryBlock()->join;
  auto *mem_root = join->thd->mem_root;

  for (auto &sjm : source_join->sjm_exec_list) {
    // Here To identify shared or pushed down sub-queries, we need clone
    // sj_inner_exprs, and need a new NESTED_JOIN.
    auto *sj_nest = sjm.sj_nest->clone(mem_root);
    sj_nest->nested_join = new (mem_root) NESTED_JOIN;
    for (auto *item : sjm.sj_nest->nested_join->sj_inner_exprs) {
      Item *new_item;
      if (!(new_item = item->clone(context)) ||
          sj_nest->nested_join->sj_inner_exprs.push_back(new_item))
        return true;
    }

    Semijoin_mat_exec *const sjm_exec = new (mem_root)
        Semijoin_mat_exec(sj_nest, sjm.is_scan, sjm.table_count,
                          sjm.mat_table_index, sjm.inner_table_index);
    sjm_exec->table = sjm.table;
    join->sjm_exec_list.push_back(sjm_exec);
  }

  return false;
}

class OriginItemRewriteContext : public Item_clone_context {
 public:
  OriginItemRewriteContext(THD *thd, Query_block *query_block,
                           ItemRefCloneResolver *ref_resolver,
                           bool nonpushed_sum_funcs, TABLE *collector_table,
                           mem_root_deque<Item *> &partial_fields)
      : Item_clone_context(thd, query_block, ref_resolver),
        m_nonpushed_sum_funcs(nonpushed_sum_funcs),
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
    if (m_nonpushed_sum_funcs) return nullptr;
    // Find table field no for pushed down item sum
    for (uint i = 0; i < m_partial_fields.size(); i++) {
      auto *partial_item = m_partial_fields[i];
      if (item_func->is_identical(partial_item)) {
        Item_field *item_field = new Item_field(m_collector_table->field[i]);
        // Arg could be nullptr, e.g. PTI_count_sym
        if (arg) item_field->set_id(arg->id());
        return item_field;
      }
    }

    assert(false);

    return nullptr;
  }

  bool resolve_view_ref(Item_view_ref *item,
                        const Item_view_ref *from) override {
    assert(!from->get_first_inner_table());
    return ResolveItemRefByInlineClone(item, from, this);
  }

 private:
  bool m_nonpushed_sum_funcs;
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

  ItemRefCloneResolver ref_resolver(thd->mem_root);
#ifndef NDEBUG
  ref_resolver.query_block_for_inline_clone_assert = source_query_block;
#endif
  OriginItemRewriteContext clone_context(
      thd, source_query_block, &ref_resolver,
      m_aggregate_strategy == AggregateStrategy::OnePhase, collector_table,
      partial_fields);

  // Process original fields
  for (auto &desc : *fields_pushdown_desc) {
    Item *item = desc.item();
    Item *new_item = nullptr;
    if (desc.to_replace()) {
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
      assert(desc.to_clone());
      if (!(new_item = item->clone(&clone_context))) return true;
      if (new_item->type() == Item::SUM_FUNC_ITEM) {
        Item_sum *item_sum = down_cast<Item_sum *>(new_item);
        if (m_aggregate_strategy == AggregateStrategy::TwoPhase)
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
  if (clone_context.final_resolve_refs()) return true;

  return false;
}

bool ParallelPlan::GeneratePartialPlan(
    PartialItemGenContext **partial_clone_context,
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

  auto *ref_clone_resolver = new (mem_root) ItemRefCloneResolver(mem_root);
#ifndef NDEBUG
  ref_clone_resolver->query_block_for_inline_clone_assert = source_query_block;
#endif
  auto *clone_context = new (mem_root)
      PartialItemGenContext(thd, partial_query_block, ref_clone_resolver);
  if (!ref_clone_resolver || !clone_context) return true;
  *partial_clone_context = clone_context;

  auto &parallel_scan_info = m_partial_plan.GetParallelScanInfo();
  m_aggregate_strategy = decide_aggregate_strategy(
      source_join, parallel_scan_info.table, parallel_scan_info.scan_desc.keynr,
      parallel_scan_info.scan_desc.key_used);

  if (GenPartialFields(clone_context, fields_pushdown_desc)) return true;

  // XXX Set JOIN::select_distinct and JOIN::select_count
  // XXX collect pushed down fields from fields, table_ref, conditions and
  // having

  join->implicit_grouping = source_join->implicit_grouping;

  // XXX Don't need where_cond, Explain need this?
  join->where_cond = nullptr;

  // XXX group by and order by generating
  if (setup_partial_base_ref_items()) return true;

  if (ClonePartialOrders()) return true;
  if (CloneSemiJoinExecList(clone_context)) return true;

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
  PartialItemGenContext *partial_clone_context;
  FieldsPushdownDesc fields_pushdown_desc(mem_root);

  if (GeneratePartialPlan(&partial_clone_context, &fields_pushdown_desc))
    return true;

  if (GenFinalFields(&fields_pushdown_desc)) return true;

  source_join->fields = &m_fields;
  // tmp_fields[REF_SLICE_ACTIVE] is not used, here we use it for access path
  // rewrite.
  source_join->tmp_fields[REF_SLICE_ACTIVE] = m_fields;
  source_join->ref_items[REF_SLICE_ACTIVE] = source_query_block->base_ref_items;

  if (!source_join->ref_items[REF_SLICE_SAVED_BASE].is_null())
    source_join->copy_ref_item_slice(REF_SLICE_SAVED_BASE, REF_SLICE_ACTIVE);

  if (GenerateAccessPath(partial_clone_context)) return true;


  if (partial_clone_context->EndPartialPlanGen()) return true;

  if (setup_sum_funcs(thd, source_join->sum_funcs)) return true;

  auto &cached_subselects = partial_clone_context->CachedSubselects();
  m_collector->SetPreevaluateSubqueries(cached_subselects);
  m_partial_plan.SetCachedSubqueries(cached_subselects);
  m_partial_plan.SetPushdownInnerQueryExpressions(
      partial_clone_context->PushdownSubselects());
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
  AccessPathParallelizer rewriter(this, clone_context);
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "access_path_rewriting");

  assert(source_join->root_access_path());

  if (!(parallelized_path = rewriter.parallelize_access_path(
            m_collector, source_join->root_access_path(), partial_path)))
    return true;
  assert(partial_path != nullptr);

  if (source_join->root_access_path() != parallelized_path)
    source_join->set_root_access_path(parallelized_path);

  bool merge_sort_remove_duplicates;
  auto *merge_sort = rewriter.MergeSort(&merge_sort_remove_duplicates);
  if (merge_sort && m_collector->CreateMergeSort(source_join, merge_sort,
                                                 merge_sort_remove_duplicates))
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
                                  Item_clone_context *context,
                                  bool create_iterators) {
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

  if (create_iterators)
    m_root_iterator = CreateIteratorFromAccessPath(
        thd, m_root_access_path, join, /*eligible_for_batch_mode=*/true);

  return false;
}

bool Query_expression::is_pushdown_safe() const {
  // Don't support union plan clone yet.
  if (!is_simple()) return false;
  for (auto *query_block = first_query_block(); query_block;
       query_block = query_block->next_query_block()) {
    if (!query_block->is_pushdown_safe()) return false;
  }
  return true;
}

bool Query_block::clone_from(THD *thd, Query_block *from,
                             Item_clone_context *context) {
  if (pq::for_each_inner_expressions(
          this, from, [thd, context](auto *inner, auto *inner_from) {
            return inner->clone_from(thd, inner_from, context, false);
          }))
    return true;

  auto *old_ctx_query_block = context->change_query_block(this);  
  uncacheable = from->uncacheable;
  select_number = from->select_number;

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
    if (!new_item || fields.push_back(new_item)) return true;
    base_ref_items[i++] = new_item;
  }

  cond_value = from->cond_value;
  having_value = from->having_value;
  // Clone join
  if (join && join->clone_from(from->join, context)) {
    assert(thd->is_error() || thd->killed);

    return true;
  }
  context->change_query_block(old_ctx_query_block);
  return false;
}
const char *Query_block::parallel_safe(bool deny_outer_ref) const {
  if (forbid_parallel_by_upper_query_block)
    return "item_referred_by_upper_query_block";

  // Don't support window function and rollup yet
  if (!m_windows.is_empty() || olap == ROLLUP_TYPE)
    return "include_window_function_or_rullup";

  // We don't support IN sub query because we don't support clone of its refs in
  // items yet.
  auto *subselect = master->item;
  if (subselect && subselect->substype() == Item_subselect::IN_SUBS) {
    Item_in_subselect *subs = down_cast<Item_in_subselect *>(subselect);
    if (subs->strategy == Subquery_strategy::SUBQ_EXISTS)
      return "IN_subquery_strategy_is_SUBQ_EXISTS";
  }

  const char *item_refuse_cause;
  auto *unit = master;

  using PlanExprPlace = pq::PlanExprPlace;
  if (pq::walk_plan_expressions(this, [deny_outer_ref, &item_refuse_cause,
                                       unit](Item *item, PlanExprPlace place) {
        if (place == PlanExprPlace::FIELDS &&
            pq::skip_no_used_field(unit, item))
          return false;
        if (!pq::ItemRefuseParallel(item, deny_outer_ref, &item_refuse_cause))
          return false;
        if (item_refuse_cause) return true;
        switch (place) {
          case PlanExprPlace::FIELDS:
            item_refuse_cause = "fields_have_unsafe_expressions";
            break;
          case PlanExprPlace::FILTER:
            item_refuse_cause = "filter_has_unsafe_expressions";
            break;
          case PlanExprPlace::HAVING:
            item_refuse_cause = "having_has_unsafe_expressions";
            break;
          case PlanExprPlace::SJ_INNER_EXPRS:
            item_refuse_cause = "sj_inner_exprs_have_unsafe_expressions";
            break;
        }
        return true;
      }))
    return item_refuse_cause;

  // Optimizer would quit early if it found zero row result, this leads to that
  // the variable tables is not set correctly, see JOIN::zero_result_cause.
  if (!join || join->tables == 0) {
    for (TABLE_LIST *tl = leaf_tables; tl; tl = tl->next_leaf)
      if (tl->is_placeholder()) return "table_is_not_real_user_table";
  }

  if (!join) {
    // See Query_block::optimize(), optimization of inner query expressions can
    // be skipped due to zero result.
#ifndef NDEBUG
    auto *cur_unit = unit;
    bool zero_result_found = false;
    while (cur_unit) {
      auto *qb = cur_unit->master;
      if (!qb || (zero_result_found = qb->join && qb->join->zero_result_cause &&
                                      !qb->is_implicitly_grouped()))
        break;
      cur_unit = qb->master_query_expression();
    }
    assert(zero_result_found);
#endif
    return nullptr;
  }

  // We don't support const tables yet, it needs further debugging.
  if (join->const_tables > 0) return "plan_with_const_tables";

  // Loop on each table to block unsupported query
  for (uint i = 0; i < join->tables; i++) {
    auto *qt = &join->qep_tab[i];

    auto *table_ref = qt->table_ref;
    if (table_ref && table_ref->is_placeholder())
      return "table_is_not_real_user_table";

    // Un-merged derived table is blocked by this
    if (table_ref && is_temporary_table(table_ref) &&
        qt->materialize_table != QEP_TAB::MATERIALIZE_SEMIJOIN)
      return "include_unsupported_temporary_table";


    const char *cause;
    if (qt->table() && (cause = pq::TableAccessTypeRefuseParallel(qt)))
      return cause;
  }

  return nullptr;
}

bool Query_block::is_pushdown_safe() const {
  const char *cause = parallel_safe(false);
  if (cause) return false;

  for (auto *inner = first_inner_query_expression(); inner;
       inner = inner->next_query_expression()) {
    if (!inner->is_pushdown_safe()) return false;
  }

  return true;
}

bool JOIN::clone_from(JOIN *from, Item_clone_context *context) {
  // TABLE_REF clone use this, see TABLE_REF::clone().
  const_table_map = from->const_table_map;

  zero_result_cause = from->zero_result_cause;
  select_count = from->select_count;

  where_cond = nullptr;

  // See below, calc_group_buffer() depends on group_list. temporary table
  // creating also need this in access path rewriter. In partial plan of a query
  // block with parallel plan, we have already set group_list.order correctly in
  // partial plan template in AccessPathParallelizer. But in a pushed down
  // subquery, actually used ORDER in group_list may be in used_order.
  auto *group_from = from->group_list.actual_used();
  if (group_from)
    group_list.order = clone_order_list(
        group_from, {thd->mem_root, &query_block->base_ref_items,
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

  assert(query_block->olap == UNSPECIFIED_OLAP_TYPE);

  calc_group_buffer(this, group_list.order);
  send_group_parts = tmp_table_param.group_parts;

  // send_group_parts is used inside alloc_func_list()
  if (alloc_func_list()) return true;

  set_optimized();
  tables_list = query_block->leaf_tables;
  if (alloc_indirection_slices()) return true;

  tmp_fields[0] = *fields;
  ref_items[REF_SLICE_ACTIVE] = query_block->base_ref_items;

  best_read = from->best_read;
  sort_cost = from->sort_cost;
  windowing_cost = from->windowing_cost;
  if (!zero_result_cause &&
      (make_sum_func_list(*fields, true) || setup_sum_funcs(thd, sum_funcs)))
    return true;
  assert(from->root_access_path());

  pq::PartialAccessPathRewriter aprewriter(context, from, this);

  if (!(m_root_access_path =
            aprewriter.clone_and_rewrite(from->root_access_path())))
    return true;
  qep_execution_state = aprewriter.qep_execution_state();

  if (thd->is_error()) return true;

  set_plan_state(PLAN_READY);

  return false;
}
