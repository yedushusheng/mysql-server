#include "sql/parallel_query/rewrite_access_path.h"

#include "my_alloc.h"
#include "sql/filesort.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/relational_expression.h"
#include "sql/opt_range.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/planner.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"

namespace pq {
bool AccessPathRewriter::rewrite_materialize(AccessPath *in, AccessPath *out,
                                             bool) {
  out->materialize().table_path = pointer_cast<AccessPath *>(memdup_root(
      mem_root(), in->materialize().table_path, sizeof(AccessPath)));
  auto *src = in->materialize().param;
  auto **dest = &out->materialize().param;
  assert(!src->invalidators);
  MaterializePathParameters *param = new (mem_root()) MaterializePathParameters;
  if (!param) return true;
  Mem_root_array<MaterializePathParameters::QueryBlock> array(mem_root(), 1);
  array[0] = src->query_blocks[0];
  array[0].join = m_join_out;
  param->query_blocks = std::move(array);
  param->invalidators = src->invalidators;
  param->table = src->table;
  param->cte = src->cte;
  param->unit = m_join_out->query_expression();
  param->ref_slice = src->ref_slice;
  param->rematerialize = src->rematerialize;
  param->limit_rows = src->limit_rows;
  param->reject_multiple_rows = src->reject_multiple_rows;
  *dest = param;
  return false;
}

AccessPath *AccessPathRewriter::accesspath_dup(AccessPath *path) {
  return pointer_cast<AccessPath *>(
      memdup_root(mem_root(), path, sizeof(AccessPath)));
}

/**
  Work horse of rewriter, please keep cases same with WalkAccessPaths(). We
  should use WalkAccessPaths(), but here we don't need to walk into some special
  paths, see caller of rewrite_temptable_scan_path().
 */
bool AccessPathRewriter::do_rewrite(AccessPath *&path, AccessPath *curjoin,
                                    AccessPath *&out) {
  // Do a post-order traversal
  AccessPath *outer_path = nullptr;
  switch (path->type) {
    case AccessPath::TABLE_SCAN:
    case AccessPath::INDEX_SCAN:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    // PUSHED_JOIN_REF: ndb only, no need rewrite
    // FULL_TEXT_SEARCH: not support yet.
    case AccessPath::MRR:
    // FOLLOW_TAIL: temporary table only see, rewrite_temptable_scan_path()
    case AccessPath::CONST_TABLE:
    case AccessPath::INDEX_RANGE_SCAN:
    // DYNAMIC_INDEX_RANGE_SCAN: not support yet
    case AccessPath::PARALLEL_COLLECTOR_SCAN:
      break;
    // TABLE_VALUE_CONSTRUCTOR: not support yet
    // FAKE_SINGLE_ROW: not support yet
    case AccessPath::ZERO_ROWS:
      if (do_rewrite(path->zero_rows().child, path, out)) return true;
      break;
    case AccessPath::ZERO_ROWS_AGGREGATED:
    // MATERIALIZED_TABLE_FUNCTION: not support yet.
    case AccessPath::UNQUALIFIED_COUNT:
      break;
    case AccessPath::NESTED_LOOP_JOIN:
      if (do_rewrite(path->nested_loop_join().outer, path, out)) return true;
      outer_path = out;
      if (do_rewrite(path->nested_loop_join().inner, path, out)) return true;
      break;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      if (do_rewrite(path->nested_loop_semijoin_with_duplicate_removal().outer,
                     path, out))
        return true;
      outer_path = out;
      if (do_rewrite(path->nested_loop_semijoin_with_duplicate_removal().inner,
                     path, out))
        return true;
      break;
    case AccessPath::BKA_JOIN:
      if (do_rewrite(path->bka_join().outer, path, out)) return true;
      outer_path = out;
      if (do_rewrite(path->bka_join().inner, path, out)) return true;
      break;
    case AccessPath::HASH_JOIN:
      if (do_rewrite(path->hash_join().outer, path, out)) return true;
      outer_path = out;
      if (do_rewrite(path->hash_join().inner, path, out)) return true;
      break;
    case AccessPath::FILTER:
      if (do_rewrite(path->filter().child, curjoin, out)) return true;
      break;
    case AccessPath::SORT:
      if (do_rewrite(path->sort().child, curjoin, out)) return true;
      break;
    case AccessPath::AGGREGATE:
      if (do_rewrite(path->aggregate().child, curjoin, out)) return true;
      break;
    case AccessPath::TEMPTABLE_AGGREGATE:
      if (do_rewrite(path->temptable_aggregate().subquery_path, curjoin, out))
        return true;
      break;
    case AccessPath::LIMIT_OFFSET:
      if (do_rewrite(path->limit_offset().child, curjoin, out)) return true;
      break;
    case AccessPath::STREAM:
      if (do_rewrite(path->stream().child, curjoin, out)) return true;
      break;
    case AccessPath::MATERIALIZE: {
      assert(path->materialize().param->query_blocks.size() == 1);
      MaterializePathParameters::QueryBlock &query_block =
          path->materialize().param->query_blocks[0];
      if (do_rewrite(query_block.subquery_path, curjoin, out)) return true;
      break;
    }

    // MATERIALIZE_INFORMATION_SCHEMA_TABLE: not support yet.
    // APPEND: union needs this, not support yet.
    // WINDOW: not support yet.

    case AccessPath::WEEDOUT:
      if (do_rewrite(path->weedout().child, curjoin, out)) return true;
      break;

    // REMOVE_DUPLICATES:  only used in hypergraph, no need.

    case AccessPath::REMOVE_DUPLICATES_ON_INDEX:
      if (do_rewrite(path->remove_duplicates_on_index().child, curjoin, out))
        return true;
      break;

    // ALTERNATIVE: not support yet.
    // CACHE_INVALIDATOR: used by recursive CTE, not support yet.
    default:
      assert(false);
  }

  return rewrite_each_access_path(path, curjoin, outer_path, out);
}

bool AccessPathRewriter::rewrite_each_access_path(AccessPath *&path,
                                                  AccessPath *curjoin,
                                                  AccessPath *outer_path,
                                                  AccessPath *&out) {
  // Do access path modification
  AccessPath *dup = nullptr;
  switch (path->type) {
    case AccessPath::TABLE_SCAN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_table_scan(path, dup)) return true;
      break;
    case AccessPath::INDEX_SCAN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_index_scan(path, dup)) return true;
      break;
    case AccessPath::REF:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_ref(path, dup)) return true;
      break;
    case AccessPath::REF_OR_NULL:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_ref_or_null(path, dup)) return true;
      break;
    case AccessPath::EQ_REF:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_eq_ref(path, dup)) return true;
      break;
    // PUSHED_JOIN_REF: ndb only, no need rewrite
    // FULL_TEXT_SEARCH: not support yet.
    case AccessPath::MRR:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_mrr(path, dup)) return true;
      break;
    // FOLLOW_TAIL: temporary table only see, rewrite_temptable_scan_path()
    case AccessPath::CONST_TABLE:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_const_table(path, dup)) return true;
      break;
    case AccessPath::INDEX_RANGE_SCAN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_index_range_scan(path, dup)) return true;
      break;
    // DYNAMIC_INDEX_RANGE_SCAN: not support yet
    // PARALLEL_COLLECTOR_SCAN: no need rewrite
    // TABLE_VALUE_CONSTRUCTOR: not support yet
    // FAKE_SINGLE_ROW: not support yet
    case AccessPath::ZERO_ROWS:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      dup->zero_rows().child = out;
      break;
    case AccessPath::ZERO_ROWS_AGGREGATED:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      break;
    case AccessPath::UNQUALIFIED_COUNT:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_unqualified_count(path, dup)) return true;
      break;
    // MATERIALIZED_TABLE_FUNCTION: not support yet.
    case AccessPath::NESTED_LOOP_JOIN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      dup->nested_loop_join().outer = outer_path;
      dup->nested_loop_join().inner = out;
      break;
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_nested_loop_semijoin_with_duplicate_removal(path, dup))
        return true;
      dup->nested_loop_semijoin_with_duplicate_removal().outer = outer_path;
      dup->nested_loop_semijoin_with_duplicate_removal().inner = out;
      break;
    case AccessPath::BKA_JOIN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      dup->bka_join().outer = outer_path;
      dup->bka_join().inner = out;
      break;
    case AccessPath::HASH_JOIN:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_hash_join(path, dup)) return true;
      dup->hash_join().outer = outer_path;
      dup->hash_join().inner = out;
      break;
    case AccessPath::FILTER:
      assert(!path->filter().materialize_subqueries);
      // HAVING is also a filter access path
      dup = accesspath_dup_if_out(path);
      if (rewrite_filter(path, dup)) return true;
      if (dup) dup->filter().child = out;
      break;
    case AccessPath::SORT:
      dup = accesspath_dup_if_out(path);
      if (rewrite_sort(path, dup)) return true;
      if (dup) dup->sort().child = out;
      break;
    case AccessPath::AGGREGATE:
      // We only push down plan nodes under group node
      if (end_of_out_path()) break;

      dup = accesspath_dup(path);
      if (rewrite_aggregate(path, dup)) return true;
      dup->aggregate().child = out;
      break;
    case AccessPath::TEMPTABLE_AGGREGATE:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_temptable_aggregate(path, dup)) return true;
      dup->temptable_aggregate().subquery_path = out;
      break;
    case AccessPath::LIMIT_OFFSET:
      assert(!path->limit_offset().send_records_override);
      dup = accesspath_dup_if_out(path);
      if (rewrite_limit_offset(path, dup, curjoin != nullptr)) return true;
      if (dup) dup->limit_offset().child = out;
      break;
    case AccessPath::STREAM:
      dup = accesspath_dup_if_out(path);
      if (rewrite_stream(path, dup)) return true;
      if (dup) dup->stream().child = out;
      break;
    case AccessPath::MATERIALIZE:
      dup = accesspath_dup_if_out(path);
      if (rewrite_materialize(path, dup, curjoin != nullptr)) return true;
      if (dup) dup->materialize().param->query_blocks[0].subquery_path = out;
      break;

    // MATERIALIZE_INFORMATION_SCHEMA_TABLE: not support yet.
    // APPEND: not support yet.
    // WINDOW: not support yet.

    case AccessPath::WEEDOUT:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_weedout(path, dup)) return true;
      dup->weedout().child = out;
      break;

     // REMOVE_DUPLICATES:  only used in hypergraph, no need.

    case AccessPath::REMOVE_DUPLICATES_ON_INDEX:
      assert(!end_of_out_path());
      dup = accesspath_dup(path);
      if (rewrite_remove_duplicates_on_index(path, dup)) return true;
      dup->remove_duplicates_on_index().child = out;
      break;
    // ALTERNATIVE: not support yet.
    // CACHE_INVALIDATOR: used by recursive CTE, not support yet.
    default:
      assert(false);
  }

  if (dup) {
    out = dup;
    post_rewrite_out_path(out);
  }

  return false;
}

AccessPathParallelizer::AccessPathParallelizer(
    Item_clone_context *item_clone_context, JOIN *join_in,
    PartialPlan *partial_plan)
    : AccessPathRewriter(item_clone_context, join_in, partial_plan->Join()),
      m_partial_plan(partial_plan) {}

AccessPath *AccessPathParallelizer::parallelize_access_path(
    Collector *collector, AccessPath *in, AccessPath *&partial_path) {
  THD *thd = m_join_out->thd;
  m_collector_table = collector->CollectorTable();

  partial_path = nullptr;
  if (do_rewrite(in, nullptr, partial_path)) return nullptr;

  AccessPath *collector_path =
    NewParallelCollectorAccessPath(thd, collector, m_collector_table, true);

  AccessPath *root_path = nullptr;
  // All plan is pushed down, so just replace whole plan with collector path
  if (!collector_path_pos()) {
    CopyBasicProperties(*partial_path, collector_path);
    root_path = collector_path;
  } else
    *m_collector_path_pos = collector_path;

  if (!root_path) root_path = in;

  return root_path;
}

void AccessPathParallelizer::post_rewrite_out_path(AccessPath *out) {
  if (m_fake_timing_iterator) out->iterator = m_fake_timing_iterator;
}

void AccessPathParallelizer::set_collector_path_pos(AccessPath **path) {
  m_collector_path_pos = path;
  if (m_sorting_info.table) return;
  // collector always use collector table to sort
  assert(m_collector_table);
  set_sorting_info({m_collector_table, REF_SLICE_SAVED_BASE});
}

void AccessPathParallelizer::rewrite_index_access_path(TABLE *table,
                                                       bool use_order,
                                                       bool reverse) {
  auto *join = m_join_in;
  if (use_order && join->m_ordered_index_usage != JOIN::ORDERED_INDEX_VOID) {
    merge_sort = join->m_ordered_index_usage == JOIN::ORDERED_INDEX_ORDER_BY
                     ? join->order.order
                     : join->group_list.order;
    /// Optimizer reset order and group_list of JOIN if it choose the
    /// execution plan. We save it to merge_sort before optimizer reset it.
    /// See also JOIN::make_tmp_tables_info().
    if (!merge_sort && !join->merge_sort.empty())
      merge_sort = join->merge_sort.order;
  }

  if (reverse && m_partial_plan->IsParallelScanTable(table))
    m_partial_plan->SetParallelScanReverse();
}

bool AccessPathParallelizer::rewrite_index_scan(AccessPath *in, AccessPath *out
                                                [[maybe_unused]]) {
  assert(out);
  auto &index_scan = in->index_scan();
  rewrite_index_access_path(index_scan.table, index_scan.use_order,
                            index_scan.reverse);
  return false;
}

bool AccessPathParallelizer::rewrite_ref(AccessPath *in,
                                         AccessPath *out [[maybe_unused]]) {
  assert(out);
  auto &ref = in->ref();
  rewrite_index_access_path(ref.table, ref.use_order, ref.reverse);
  return false;
}

bool AccessPathParallelizer::rewrite_ref_or_null(AccessPath *in, AccessPath *out
                                                 [[maybe_unused]]) {
  assert(out);
  auto &ref_or_null = in->ref_or_null();
  rewrite_index_access_path(ref_or_null.table, ref_or_null.use_order, false);
  return false;
}

bool AccessPathParallelizer::rewrite_eq_ref(AccessPath *in,
                                            AccessPath *out [[maybe_unused]]) {
  assert(out);
  auto &eq_ref = in->eq_ref();

  rewrite_index_access_path(eq_ref.table, eq_ref.use_order, false);
  return false;
}

bool AccessPathParallelizer::rewrite_index_range_scan(AccessPath *in,
                                                      AccessPath *out
                                                      [[maybe_unused]]) {
  assert(out);
  auto &index_range_scan = in->index_range_scan();
  auto *quick = index_range_scan.quick;
  assert(quick->head == index_range_scan.table);

  rewrite_index_access_path(
      quick->head, m_join_in->m_ordered_index_usage != JOIN::ORDERED_INDEX_VOID,
      quick->reverse_sorted());

  return false;
}

static Mem_root_array<std::pair<TABLE *, uint>> *
get_tables_for_unqualified_count(QEP_TAB *qep_tab, uint tables,
                                 MEM_ROOT *mem_root) {
  auto *count_tables =
      new (mem_root) Mem_root_array<std::pair<TABLE *, uint>>(mem_root);
  if (!count_tables) return nullptr;
  for (uint i = 0; i < tables; i++) {
    auto *qt = &qep_tab[i];
    // See get_exact_record_count() for keyno usage.
    uint keyno =
        qt->type() == JT_ALL ||
                (qt->effective_index() == qt->table()->s->primary_key &&
                 qt->table()->file->primary_key_is_clustered())
            ? MAX_KEY
            : qt->effective_index();

    if (count_tables->push_back(std::make_pair(qt->table(), keyno)))
      return nullptr;
  }

  return count_tables;
}

bool AccessPathParallelizer::rewrite_unqualified_count(AccessPath *&in,
                                                       AccessPath *out) {
  // Create QEP_TABs for get_exact_record_count() calling.
  if (!(out->unqualified_count().tables = get_tables_for_unqualified_count(
            m_join_in->qep_tab, m_join_in->primary_tables, mem_root())))
    return true;

  auto *aggregate_path = NewAggregateAccessPath(m_join_out->thd, in, false);
  set_collector_path_pos(&aggregate_path->aggregate().child);
  in = aggregate_path;
  return false;
}

/// Rewrite the temporary table scan access path under a materialize access
/// path, replace its table with @param table
static void rewrite_temptable_scan_path(AccessPath *table_path, TABLE *table,
                                        Item_clone_context *clone_context) {
  TABLE **table_ptr = nullptr;

  assert(table_path->type == AccessPath::TABLE_SCAN ||
         table_path->type == AccessPath::INDEX_RANGE_SCAN ||
         table_path->type == AccessPath::FOLLOW_TAIL ||
         table_path->type == AccessPath::EQ_REF ||
         table_path->type == AccessPath::CONST_TABLE);

  switch (table_path->type) {
    case AccessPath::TABLE_SCAN:
      table_ptr = &table_path->table_scan().table;
      break;
    case AccessPath::INDEX_RANGE_SCAN:
      table_ptr = &table_path->index_range_scan().table;
      break;
    case AccessPath::FOLLOW_TAIL:
      table_ptr = &table_path->follow_tail().table;
      break;
    case AccessPath::EQ_REF:
      table_ptr = &table_path->eq_ref().table;
      table_path->eq_ref().ref = table_path->eq_ref().ref->clone(
          *table_ptr, INNER_TABLE_BIT, clone_context);
      break;
    case AccessPath::CONST_TABLE:
      table_ptr = &table_path->const_table().table;
      table_path->const_table().ref = table_path->const_table().ref->clone(
          *table_ptr, INNER_TABLE_BIT, clone_context);
      break;
    default:
      assert(false);
  }

  assert(table_ptr);
  *table_ptr = table;
}

#ifndef NDEBUG
static void _assert_same_rewrite_table(TABLE *orig, TABLE *table) {
  assert(orig->s->fields == table->s->fields);
  for (uint i = 0; i < table->s->fields; i++) {
    assert(table->field[i]->type() == orig->field[i]->type());
    assert(table->field[i]->result_type() == orig->field[i]->result_type());
  }

  // Assert same keys
  assert(MaterializeIsDoingDeduplication(table) ==
         MaterializeIsDoingDeduplication(orig));

  assert((table->key_info == nullptr) == (orig->key_info == nullptr));
  if (table->key_info) {
    assert(table->s->keys == orig->s->keys);
    for (uint keyno = 0; keyno < table->s->keys; keyno++) {
      KEY *key = &table->key_info[keyno];
      KEY *orig_key = &orig->key_info[keyno];
      assert(key->actual_key_parts == orig_key->actual_key_parts);
      for (uint partno = 0; partno < key->actual_key_parts; partno++) {
        KEY_PART_INFO *part = &key->key_part[partno];
        KEY_PART_INFO *orig_part = &orig_key->key_part[partno];
        assert(part->field->field_index() == orig_part->field->field_index());
      }
    }
  }
}
#define assert_same_rewrite_table(orig, table) \
  _assert_same_rewrite_table(orig, table)
#else
#define assert_same_rewrite_table(orig, table)
#endif

/// Recreate a materialize table, new table is returned by @param table
bool recreate_materialized_table(THD *thd, JOIN *join, ORDER *group,
                                        bool distinct, bool save_sum_fields,
                                        int ref_slice, ha_rows limit_rows,
                                        TABLE **table,
                                        Temp_table_param **tmp_table_param) {
  mem_root_deque<Item *> *curr_fields = &join->tmp_fields[ref_slice - 1];
  *tmp_table_param =
      new (thd->mem_root) Temp_table_param(join->tmp_table_param);
  if (!tmp_table_param) return true;
  bool grouped;

  calc_group_buffer(grouped, **tmp_table_param, group);
  // See make_tmp_tables_info()
  bool reset_with_sum_funcs = ref_slice == 2 && distinct && !group;
  count_field_types(join->query_block, *tmp_table_param, *curr_fields,
                    reset_with_sum_funcs, save_sum_fields);
  (*tmp_table_param)->hidden_field_count = CountHiddenFields(*curr_fields);

  (*tmp_table_param)->skip_create_table = true;

  // item's marker in group can be changed in create_tmp_table(). The
  // materialize table rewriter should not change that. This leads to temp
  // table of query result to collector reclength is inconsistent with
  // collector table.
  mem_root_deque<Item::item_marker> marker_saver(thd->mem_root);
  for (ORDER *tmp = group; tmp; tmp = tmp->next) {
    if (marker_saver.push_back((*tmp->item)->marker)) return true;
  }
  //  Switch_ref_item_slice slice_switch(join, ref_slice - 1);
  *table = create_tmp_table(
      thd, *tmp_table_param, *curr_fields, group, distinct, save_sum_fields,
      join->query_block->active_options(), limit_rows, "");
  if (!*table) return true;
  (*table)->alias = "<temporary>";

  // Restore original markers
  auto marker_saver_it = marker_saver.begin();
  for (ORDER *tmp = group; tmp; tmp = tmp->next)
    (*tmp->item)->marker = *marker_saver_it++;

  // See handling of need_tmp_before_win in make_tmp_tables_info(), if there is
  // tmp table REF_SLICE_SAVED_BASE must be assigned.
  if (join->ref_items[REF_SLICE_SAVED_BASE].is_null()) {
    if (join->alloc_ref_item_slice(thd, REF_SLICE_SAVED_BASE)) return true;
    join->copy_ref_item_slice(REF_SLICE_SAVED_BASE, REF_SLICE_ACTIVE);
  }
  if (join->ref_items[ref_slice].is_null() &&
      join->alloc_ref_item_slice(thd, ref_slice))
    return true;
  if (change_to_use_tmp_fields(curr_fields, thd, join->ref_items[ref_slice],
                               &join->tmp_fields[ref_slice],
                               join->query_block->m_added_non_hidden_fields))
    return true;

  curr_fields = &join->tmp_fields[ref_slice];
  // Change fields to current temptable fields, see make_tmp_tables_info()
  join->fields = curr_fields;

  return false;
}

bool AccessPathParallelizer::rewrite_materialize(AccessPath *in,
                                                 AccessPath *out,
                                                 bool under_join) {
  auto *src = in->materialize().param;
  auto &query_block = src->query_blocks[0];

  if (out) {
    // Clone access path itself for workers
    if (super::rewrite_materialize(in, out, under_join)) return true;
    if (src->table->group) {
      assert(m_join_out->group_list.empty());
      // Uses JOIN::group_list to save TABLE::group of this access path to avoid
      // overwrite original group of temporary table.
      m_join_out->group_list = ORDER_with_src(
          clone_order_list(
              src->table->group,
              {mem_root(), &m_join_out->query_block->base_ref_items,
               m_join_out->fields->size(), nullptr}),
          ESC_GROUP_BY);
    }

    out->materialize().table_path =
        accesspath_dup(in->materialize().table_path);
    post_rewrite_out_path(out->materialize().table_path);

    // Following scenario is for full push down, that is leader do not do
    // this materialization any more.

    //  This is the semijoin materialize currently.
    if (under_join) {
      assert(src->ref_slice == -1);
      return false;
    }

    if (!MaterializeIsDoingDeduplication(src->table)) {
#ifndef NDEBUG
      // This should be SQL_BUFFER_RESULT or do DISTINCT by LIMIT 1, We just
      // push down this to worker and leader do nothing. see ConnectJoins()
      // QEP_TAB::needs_duplicate_removal processing.
      if (!(m_join_in->query_block->active_options() & OPTION_BUFFER_RESULT)) {
        uint i = 0;
        for (; i < m_join_in->tables; i++) {
          if (m_join_in->qep_tab[i].table() == src->table) break;
        }
        // See (5) of need_tmp_before_win setting, a complicated sorting could
        // create a tmp table.
        assert(m_join_in->qep_tab[i].needs_duplicate_removal ||
               m_join_in->qep_tab[i].filesort);
      }
#endif
      return false;
    }
    // We can push down GROUP BY and ORDER BY if they have same fields and no
    // aggregation functions.
    if (*m_join_in->sum_funcs == nullptr && src->table->group &&
        !m_join_in->order.empty()) {
      bool full_pushdown = true;
      ORDER *grp, *ord;
      for (grp = src->table->group, ord = m_join_in->order.order; grp && ord;
           grp = grp->next, ord = ord->next) {
        if (grp->item != ord->item) {
          full_pushdown = false;
          break;
        }
      }
      full_pushdown &= (grp == ord);
      if (full_pushdown) {
        merge_sort_remove_duplicates = true;
        return false;
      }
    }
  }

  // We need recreate materialize table due to underlying table change (base
  // table to collector table).
  TABLE *orig_table [[maybe_unused]] = src->table;
  if (recreate_materialized_table(
          m_join_in->thd, m_join_in, src->table->group,
          src->table->s->is_distinct,
          !m_join_in->group_list.empty() && m_join_in->simple_group,
          src->ref_slice, src->limit_rows, &src->table,
          &query_block.temp_table_param))
    return true;
  TABLE *table = src->table;

  assert_same_rewrite_table(orig_table, table);

  auto *temp_table_param = query_block.temp_table_param;
  // See setup_tmptable_write_func(), Should we add precomputed_group_by
  // here? JOIN::streaming_aggregation is reset by make_tmp_tables_info(),
  // We reset it true by make_group_fields() in rewrite_aggregate()
  if (m_join_in->streaming_aggregation) {
    for (Item_sum **func_ptr = m_join_in->sum_funcs; *func_ptr != nullptr;
         ++func_ptr) {
      if (temp_table_param->items_to_copy->push_back(
              Func_ptr(*func_ptr, (*func_ptr)->get_result_field())))
        return true;
    }
    // See make_tmp_tables_info(), only first tmp table needs to append
    // entries to items_to_copy
    m_join_in->streaming_aggregation = false;
  }

  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_in->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, temp_table_param});

  rewrite_temptable_scan_path(in->materialize().table_path, table,
                              m_item_clone_context);

  set_sorting_info({table, src->ref_slice});

  // XXX group list also need skip this path
  if (!collector_path_pos()) set_collector_path_pos(&query_block.subquery_path);
  return false;
}

bool AccessPathParallelizer::rewrite_temptable_aggregate(
    AccessPath *in, AccessPath *out) {
  auto &tagg = in->temptable_aggregate();

  assert(m_join_out->group_list.empty());

  m_join_out->group_list = ORDER_with_src(
      clone_order_list(
          tagg.table->group,
          {m_join_out->thd->mem_root, &m_join_out->query_block->base_ref_items,
           m_join_out->fields->size(), nullptr}),
      ESC_GROUP_BY);

  TABLE *orig_table [[maybe_unused]] = tagg.table;
  if (recreate_materialized_table(m_join_in->thd, m_join_in, tagg.table->group,
                                  false, false, tagg.ref_slice, HA_POS_ERROR,
                                  &tagg.table, &tagg.temp_table_param))
    return true;
  TABLE *table = tagg.table;

  assert_same_rewrite_table(orig_table, table);

  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_in->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, tagg.temp_table_param});

  if (out) {
    out->temptable_aggregate().table_path = accesspath_dup(tagg.table_path);
    post_rewrite_out_path(out->temptable_aggregate().table_path);
  }

  rewrite_temptable_scan_path(tagg.table_path, table, m_item_clone_context);

  set_sorting_info({table, tagg.ref_slice});
  set_collector_path_pos(&tagg.subquery_path);
  return false;
}

bool AccessPathRewriter::do_stream_rewrite(JOIN *join, AccessPath *path) {
  auto &stream = path->stream();
  THD *thd = join->thd;
  auto ref_slice = stream.ref_slice;
  // On leader, slice may be allocated in planing stage, So here make sure
  // it is not allocated yet.
  if (join->ref_items[ref_slice].is_null() &&
      join->alloc_ref_item_slice(thd, ref_slice))
    return true;
  if (recreate_materialized_table(thd, join, nullptr, false, true, ref_slice,
                                  HA_POS_ERROR, &stream.table,
                                  &stream.temp_table_param))
    return true;
  if (stream.join != join) stream.join = join;

  // See setup_tmptable_write_func(), stream aggregation and precomputed
  // GROUP BY needs this hack for aggregation functions.
  Temp_table_param *const tmp_tbl = stream.temp_table_param;
  if (join->streaming_aggregation || tmp_tbl->precomputed_group_by) {
    for (Item_sum **func_ptr = join->sum_funcs; *func_ptr != nullptr;
         ++func_ptr) {
      tmp_tbl->items_to_copy->push_back(
          Func_ptr(*func_ptr, (*func_ptr)->get_result_field()));
    }
  }
  set_sorting_info({stream.table, ref_slice});
  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  join->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{stream.table, stream.temp_table_param});
  return false;
}

bool AccessPathParallelizer::rewrite_stream(AccessPath *in, AccessPath *out) {
  if (out) return false;
  return do_stream_rewrite(m_join_in, in);
}

bool AccessPathParallelizer::rewrite_filter(AccessPath *in, AccessPath *out) {
  assert(in->filter().condition->parallel_safe() == Item_parallel_safe::Safe);

  // HAVING is not pushed down.
  if (out && !(out->filter().condition =
            in->filter().condition->clone(m_item_clone_context)))
    return true;

  return false;
}

bool AccessPathParallelizer::rewrite_sort(AccessPath *in, AccessPath *out) {
  auto &sort_in = in->sort();
  Filesort *filesort = sort_in.filesort;
  if (out) {
    merge_sort = sort_in.filesort->src_order;
    auto &sort_out = out->sort();
    sort_out.filesort->src_order =
        clone_order_list(filesort->src_order,
                         {mem_root(), &m_join_out->query_block->base_ref_items,
                          m_join_out->fields->size(), nullptr});
    return false;
  }
  // Rewrite orignal filesort since its underlying table changed.
  Switch_ref_item_slice slice_switch(m_join_in, m_sorting_info.ref_item_slice);
  sort_in.filesort = new (mem_root()) Filesort(
      m_join_in->thd, {m_sorting_info.table}, filesort->keep_buffers,
      filesort->src_order, filesort->limit, filesort->m_force_stable_sort,
      filesort->m_remove_duplicates, filesort->m_force_sort_positions, false);
  m_join_in->filesorts_to_cleanup.push_back(sort_in.filesort);
  return false;
}

bool AccessPathParallelizer::rewrite_aggregate(AccessPath *in, AccessPath *) {
  ORDER_with_src group_list = m_join_in->merge_sort.empty()
                                  ? m_join_in->group_list
                                  : m_join_in->merge_sort;
  m_join_out->group_list =
      group_list.clone({mem_root(), &m_join_out->query_block->base_ref_items,
                        m_join_out->fields->size(), nullptr});

  // Recreate group fields original join since underlying table changed
  if (!m_join_in->merge_sort.empty()) {
    assert(m_join_in->group_list.empty());
    m_join_in->group_list = m_join_in->merge_sort;
  }
  m_join_in->group_fields_cache.clear();
  m_join_in->group_fields.destroy_elements();
  if (make_group_fields(m_join_in, m_join_in)) return true;

  set_collector_path_pos(&in->aggregate().child);
  return false;
}

bool AccessPathParallelizer::rewrite_limit_offset(AccessPath *in,
                                                  AccessPath *out,
                                                  bool under_join) {
  if (!out) return false;

  assert(!collector_path_pos());

  // The collector table engine doesn't move offset for LIMIT.
  if (in->limit_offset().offset_moved_in_engine) {
    in->limit_offset().offset_moved_in_engine = false;
    // Offset could not be pushed down to worker.
    out->limit_offset().offset_moved_in_engine = false;
  }
  // Don't push down offset to worker
  if (out->limit_offset().offset != 0) out->limit_offset().offset = 0;

  // See calls of NewLimitOffsetAccessPath(), Semi or Anti JOIN use
  // limit_offset access path with limit = 1, gather should not be applied
  // below that.
  if (in->limit_offset().limit != 1 || !under_join) {
    set_collector_path_pos(&in->limit_offset().child);
    m_pushed_limit_offset = true;
  }
  return false;
}

TABLE *PartialAccessPathRewriter::find_leaf_table(TABLE *table) const {
  Query_block *query_block = m_join_out->query_block;
  auto *tables = query_block->leaf_tables;

  for (TABLE_LIST *tl = tables; tl; tl = tl->next_leaf) {
    if (tl->is_identical(table->pos_in_table_list)) return tl->table;
  }

  assert(false);

  return nullptr;
}

/// XXX TDSQL seems that pushed_cond also is used in LIMIT pushdown, so
/// should it also be cloned.
static bool clone_handler_pushed_cond(Item_clone_context *context, uint keyno,
                                      const TABLE *from, TABLE *table) {
  Item *cond, *from_cond = from->file->pushed_idx_cond;
  if (from_cond) {
    assert(from_cond->parallel_safe() == Item_parallel_safe::Safe);
    assert(keyno < MAX_KEY);
    if (!(cond = from_cond->clone(context))) return true;

    auto *remained_cond [[maybe_unused]] =
        table->file->idx_cond_push(keyno, cond);
    assert(remained_cond == nullptr);
  }

  if (table->file->tdsql_clone_pushed(from->file, context, true)) return true;

  return false;
}

template <typename aptype>
bool PartialAccessPathRewriter::rewrite_base_scan(aptype &out, uint keyno) {
  auto *orig_table = out.table;
  TABLE *table = find_leaf_table(orig_table);

  if (clone_handler_pushed_cond(m_item_clone_context, keyno, orig_table, table))
    return true;

  if (orig_table->key_read) table->set_keyread(orig_table->key_read);

  out.table = table;

  set_sorting_info({table, REF_SLICE_SAVED_BASE});

  return false;
}

template <typename aptype>
bool PartialAccessPathRewriter::rewrite_base_ref(aptype &out) {
  if (rewrite_base_scan(out, out.ref->key)) return true;
  auto *ref = out.ref;
  if (!(out.ref = ref->clone(out.table, m_join_out->const_table_map,
                             m_item_clone_context)))
    return true;

  return false;
}

bool PartialAccessPathRewriter::rewrite_table_scan(AccessPath *,
                                                   AccessPath *out) {
  auto &table_scan = out->table_scan();
  return rewrite_base_scan(table_scan, MAX_KEY);
}

bool PartialAccessPathRewriter::rewrite_index_scan(AccessPath *,
                                                   AccessPath *out) {
  auto &index_scan = out->index_scan();
  return rewrite_base_scan(index_scan, index_scan.idx);
}

bool PartialAccessPathRewriter::rewrite_ref(AccessPath *, AccessPath *out) {
  return rewrite_base_ref(out->ref());
}

bool PartialAccessPathRewriter::rewrite_ref_or_null(AccessPath *,
                                                    AccessPath *out) {
  return rewrite_base_ref(out->ref_or_null());
}

bool PartialAccessPathRewriter::rewrite_eq_ref(AccessPath *, AccessPath *out) {
  return rewrite_base_ref(out->eq_ref());
}

bool PartialAccessPathRewriter::rewrite_mrr(AccessPath *, AccessPath *out) {
  return rewrite_base_ref(out->mrr());
}

bool PartialAccessPathRewriter::rewrite_const_table(AccessPath *,
                                                    AccessPath *out) {
  return rewrite_base_ref(out->const_table());
}

bool PartialAccessPathRewriter::rewrite_index_range_scan(AccessPath *,
                                                         AccessPath *out) {
  auto &index_range_scan = out->index_range_scan();
  if (rewrite_base_scan(index_range_scan, index_range_scan.quick->index))
    return true;
  // Target table has been set inside of rewrite_base_scan()
  auto *table = index_range_scan.table;
  auto *join = m_join_out;
  auto *quick = index_range_scan.quick->clone(join, table);

  if (!quick || join->quick_selects_to_cleanup.push_back(quick)) return true;

  index_range_scan.quick = quick;

  set_sorting_info({table, REF_SLICE_SAVED_BASE});
  return false;
}

bool PartialAccessPathRewriter::rewrite_unqualified_count(AccessPath *&in,
                                                          AccessPath *out) {
  auto *src_tables = in->unqualified_count().tables;
  auto *count_tables =
      new (mem_root()) Mem_root_array<std::pair<TABLE *, uint>>(mem_root());
  for (auto it : *src_tables) {
    TABLE *orig_table = it.first;
    TABLE *table = find_leaf_table(orig_table);
    uint keyno = it.second;
    if (count_tables->push_back(std::make_pair(table, keyno))) return true;

    // To call row count RPC, TDSQL 3 could convert some non select_count
    // queries to select_count. Here we clone tables' pushed down condition.
    if (clone_handler_pushed_cond(m_item_clone_context, keyno, orig_table,
                                  table))
      return true;

    if (orig_table->key_read) table->set_keyread(orig_table->key_read);
  }
  out->unqualified_count().tables = count_tables;

  return false;
}

bool PartialAccessPathRewriter::
    rewrite_nested_loop_semijoin_with_duplicate_removal(AccessPath *in,
                                                        AccessPath *out) {
  const TABLE *orig_table =
      in->nested_loop_semijoin_with_duplicate_removal().table;
  auto *orig_key = in->nested_loop_semijoin_with_duplicate_removal().key;
  auto &nested_loop_semijoin =
      out->nested_loop_semijoin_with_duplicate_removal();
  // Calculate key number (offset in key_info) then get corresponding index
  // in partial plan.
  int keyno = orig_key - orig_table->key_info;
  auto *table = find_leaf_table(const_cast<TABLE *>(orig_table));
  nested_loop_semijoin.table = table;
  nested_loop_semijoin.key = table->key_info + keyno;
  return false;
}

bool PartialAccessPathRewriter::rewrite_hash_join(AccessPath *,
                                                  AccessPath *out) {
  auto &hash_join = out->hash_join();
  THD *thd = m_join_out->thd;
  // See CreateHashJoinAccessPath(), It just use these fields in query
  // execution.
  const RelationalExpression *orig_expr = hash_join.join_predicate->expr;
  RelationalExpression *expr = new (mem_root()) RelationalExpression(thd);
  if (!expr) return true;
  expr->left = expr->right = nullptr;
  expr->type = orig_expr->type;
  for (auto *item : orig_expr->join_conditions) {
    auto *new_item = item->clone(m_item_clone_context);
    if (!new_item || expr->join_conditions.push_back(new_item)) return true;
  }
  for (auto *item : orig_expr->equijoin_conditions) {
    auto *new_item = item->clone(m_item_clone_context);
    if (!new_item || expr->equijoin_conditions.push_back(
                         down_cast<Item_func_eq *>(new_item)))
      return true;
  }

  JoinPredicate *pred = new (thd->mem_root) JoinPredicate;
  if (!pred) return true;

  pred->expr = expr;
  hash_join.join_predicate = pred;
  return false;
}

/**
  Re-create materialized table for semi-join, see
  setup_semijoin_materialized_table().
*/
static TABLE *recreate_semijoin_materialized_table(
    MEM_ROOT *mem_root, PartialPlan *partial_plan,
    Item_clone_context *clone_context, TABLE *orig_table,
    Temp_table_param *temp_table_param, Query_block *query_block) {
  THD *thd = clone_context->thd();
  TABLE *table;
  mem_root_deque<Item *> sjm_fields(mem_root);
  if (partial_plan->CloneSJMatInnerExprsForTable(orig_table, &sjm_fields,
                                                 clone_context))
    return nullptr;

  count_field_types(query_block, temp_table_param, sjm_fields, false, true);
  temp_table_param->bit_fields_as_long = true;

  const char *name = strdup_root(mem_root, orig_table->alias);
  if (name == nullptr) return nullptr; /* purecov: inspected */

  if (!(table =
            create_tmp_table(thd, temp_table_param, sjm_fields, nullptr,
                             true /* distinct */, true /* save_sum_fields */,
                             thd->variables.option_bits | TMP_TABLE_ALL_COLUMNS,
                             HA_POS_ERROR /* rows_limit */, name)))
    return nullptr; /* purecov: inspected */

  table->file->ha_extra(HA_EXTRA_IGNORE_DUP_KEY);
  table->keys_in_use_for_query.set_all();

  auto table_list = new (thd->mem_root) TABLE_LIST("", name, TL_IGNORE);
  if (table_list == nullptr) return nullptr; /* purecov: inspected */
  table_list->table = table;
  table_list->set_tableno(orig_table->pos_in_table_list->tableno());
  table_list->m_id = orig_table->pos_in_table_list->m_id;
  table->pos_in_table_list = table_list;
  table->pos_in_table_list->query_block = query_block;

  /// See Also GetTableAccessPath() for items_to_copy regenerating.
  temp_table_param->items_to_copy = nullptr;
  ConvertItemsToCopy(sjm_fields, table->visible_field_ptr(), temp_table_param);

  return table;
}

bool PartialAccessPathRewriter::rewrite_materialize(AccessPath *in,
                                                    AccessPath *out,
                                                    bool under_join) {
  assert(out);
  if (super::rewrite_materialize(in, out, under_join)) return true;
  auto *dest_param = out->materialize().param;
  auto &param_query_block = dest_param->query_blocks[0];
  THD *thd = m_join_out->thd;
  TABLE *table;
  Temp_table_param *temp_table_param;

  if (under_join) {
    // This is the semijoin materialization table
    if (!(temp_table_param = new (mem_root()) Temp_table_param)) return true;
    if (!(table = recreate_semijoin_materialized_table(
              mem_root(), m_join_in->partial_plan, m_item_clone_context,
              dest_param->table, temp_table_param, m_join_out->query_block)))
      return true;

    dest_param->table = table;
    param_query_block.temp_table_param = temp_table_param;
  } else {
    Query_block *query_block_out = m_join_out->query_block;
    Query_block *query_block_in = m_join_in->query_block;
    if (m_join_out->alloc_ref_item_slice(thd, dest_param->ref_slice))
      return true;
    ORDER *group =
        m_join_in->group_list.empty()
            ? nullptr
            : clone_order_list(m_join_in->group_list.order,
                               {thd->mem_root, &query_block_out->base_ref_items,
                                query_block_out->fields.size(),
                                &query_block_in->base_ref_items[0]});

    if (recreate_materialized_table(
            thd, m_join_out, group, dest_param->table->s->is_distinct, false,
            dest_param->ref_slice, dest_param->limit_rows, &dest_param->table,
            &param_query_block.temp_table_param))
      return true;
    table = dest_param->table;
    temp_table_param = param_query_block.temp_table_param;
    // See setup_tmptable_write_func()
    if (temp_table_param->precomputed_group_by) {
      for (Item_sum **func_ptr = m_join_out->sum_funcs; *func_ptr != nullptr;
           ++func_ptr) {
        if (temp_table_param->items_to_copy->push_back(
                Func_ptr(*func_ptr, (*func_ptr)->get_result_field())))
          return true;
      }
    }
  }

  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_out->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, temp_table_param});
  set_sorting_info({table, dest_param->ref_slice});
  out->materialize().table_path = accesspath_dup(out->materialize().table_path);
  rewrite_temptable_scan_path(out->materialize().table_path, table,
                              m_item_clone_context);
  post_rewrite_out_path(out->materialize().table_path);
  return false;
}

bool PartialAccessPathRewriter::rewrite_weedout(AccessPath *in,
                                                AccessPath *out) {
  auto *orig_sjtbl = in->weedout().weedout_table;
  auto &weedout = out->weedout();
  assert(orig_sjtbl->tmp_table != nullptr);
  auto *sjtbl = new (mem_root()) SJ_TMP_TABLE(*orig_sjtbl);
  if (!sjtbl) return true;
  sjtbl->tabs = mem_root()->ArrayAlloc<SJ_TMP_TABLE_TAB>(orig_sjtbl->tabs_end -
                                                         orig_sjtbl->tabs);
  if (sjtbl->tabs == nullptr) return true;
  sjtbl->tabs_end = std::uninitialized_copy(orig_sjtbl->tabs,
                                            orig_sjtbl->tabs_end, sjtbl->tabs);
  for (auto *tab = sjtbl->tabs; tab != sjtbl->tabs_end; tab++) {
    tab->qep_tab = nullptr;
    tab->table = find_leaf_table(tab->table);
  }
  sjtbl->tmp_table = create_duplicate_weedout_tmp_table(
      m_join_out->thd, sjtbl->rowid_len + sjtbl->null_bytes, sjtbl);
  if (sjtbl->tmp_table == nullptr) return true;
  if (sjtbl->tmp_table->hash_field)
    sjtbl->tmp_table->file->ha_index_init(0, false);
  m_join_out->sj_tmp_tables.push_back(sjtbl->tmp_table);

  weedout.weedout_table = sjtbl;
  return false;
}

bool PartialAccessPathRewriter::rewrite_remove_duplicates_on_index(
    AccessPath *in, AccessPath *out) {
  auto &ia = in->remove_duplicates_on_index();
  auto &oa = out->remove_duplicates_on_index();
  auto *table = find_leaf_table(ia.table);
  int keyno = ia.key - ia.table->key_info;
  oa.table = table;
  oa.key = table->key_info + keyno;
  return false;
}

bool PartialAccessPathRewriter::rewrite_temptable_aggregate(AccessPath *,
                                                            AccessPath *out) {
  auto &tagg = out->temptable_aggregate();
  THD *thd = m_join_out->thd;
  auto *query_block_out = m_join_out->query_block;
  auto *query_block_in = m_join_in->query_block;
  ORDER *group =
      clone_order_list(m_join_in->group_list.order,
                       {thd->mem_root, &query_block_out->base_ref_items,
                        query_block_out->fields.size(),
                        &query_block_in->base_ref_items[0]});

  if (recreate_materialized_table(thd, m_join_out, group, false, false,
                                  tagg.ref_slice, HA_POS_ERROR, &tagg.table,
                                  &tagg.temp_table_param))
    return true;
  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_out->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{tagg.table, tagg.temp_table_param});

  tagg.table_path = accesspath_dup(tagg.table_path);
  rewrite_temptable_scan_path(tagg.table_path, tagg.table,
                              m_item_clone_context);
  post_rewrite_out_path(tagg.table_path);
  set_sorting_info({tagg.table, tagg.ref_slice});
  return false;
}

bool PartialAccessPathRewriter::rewrite_sort(AccessPath *in, AccessPath *out) {
  auto &sort_out = out->sort();
  auto &sort_in = in->sort();
  // Rewrite orignal filesort since its underlying table changed.
  Filesort *filesort = sort_in.filesort;
  // XXX It's not safe, because leader may changed, we should clone it to
  // template first when parallelizing plan.
  ORDER *new_order = clone_order_list(
      filesort->src_order, {mem_root(), &m_join_out->query_block->base_ref_items,
                            m_join_out->fields->size(),
                            &m_join_in->query_block->base_ref_items[0]});
  Switch_ref_item_slice slice_switch(m_join_out, m_sorting_info.ref_item_slice);
  if (!(sort_out.filesort = new (mem_root()) Filesort(
            m_join_out->thd, {m_sorting_info.table}, filesort->keep_buffers,
            new_order, filesort->limit, filesort->m_force_stable_sort,
            filesort->m_remove_duplicates, filesort->m_force_sort_positions,
            false)))
    return true;
  m_join_out->filesorts_to_cleanup.push_back(sort_out.filesort);
  return false;
}

bool PartialAccessPathRewriter::rewrite_filter(AccessPath *, AccessPath *out) {
  if (!(out->filter().condition =
            out->filter().condition->clone(m_item_clone_context)))
    return true;
  return false;
}

bool PartialAccessPathRewriter::rewrite_aggregate(AccessPath *, AccessPath *) {
  if (m_join_in->group_list.empty()) return false;
  m_join_out->group_list = m_join_in->group_list.clone(
      {mem_root(), &m_join_out->query_block->base_ref_items,
       m_join_out->fields->size(), &m_join_in->query_block->base_ref_items[0]});
  if (make_group_fields(m_join_out, m_join_out)) return true;
  return false;
}

bool PartialAccessPathRewriter::rewrite_stream(AccessPath *, AccessPath *out) {
  return do_stream_rewrite(m_join_out, out);
}
}  // namespace pq
