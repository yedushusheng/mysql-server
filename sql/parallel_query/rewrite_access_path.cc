#include "sql/parallel_query/rewrite_access_path.h"

#include "my_alloc.h"
#include "sql/filesort.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/opt_range.h"
#include "sql/parallel_query/planner.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"

namespace pq {

static AccessPathChanges NewAccessPathChanges(AccessPath *path, TABLE *table,
                                       Temp_table_param *temp_table_param) {
  AccessPathChanges changes;
  changes.access_path = path;
  auto *recreated_temp_table = &changes.u.recreated_temp_table;
  *recreated_temp_table = {table, temp_table_param};
  return changes;
}

static AccessPathChanges NewAccessPathChanges(AccessPath *path, TABLE *table) {
  AccessPathChanges changes;
  changes.access_path = path;
  changes.u.table_path.table = table;
  return changes;
}

static AccessPathChanges NewAccessPathChanges(AccessPath *path, JOIN *join) {
  AccessPathChanges changes;
  changes.access_path = path;
  changes.u.aggregate.join = join;
  return changes;
}

void AccessPathChangesStore::restore_changes() {
  for (auto &change : m_changes) change.restore();

  if (!m_access_path) return;
  if (m_collector_path_pos)
    *m_collector_path_pos = m_access_path;
  else
    m_join->set_root_access_path(m_access_path);
}

bool AccessPathRewriter::rewrite_materialize(AccessPath *in, AccessPath *out) {
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

static AccessPath *accesspath_dup(AccessPath *path, MEM_ROOT *mem_root) {
  return pointer_cast<AccessPath *>(
      memdup_root(mem_root, path, sizeof(AccessPath)));
}

/**
  Work horse of rewriter, please keep cases same with WalkAccessPaths(). We
  should use WalkAccessPaths(), but here we don't need to walk into some special
  paths, see caller of rewrite_temptable_scan_path().
 */
bool AccessPathRewriter::do_rewrite(AccessPath *path) {
  // Do a post-order traversal
  switch (path->type) {
    case AccessPath::TABLE_SCAN:
    case AccessPath::INDEX_SCAN:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    case AccessPath::INDEX_RANGE_SCAN:
    case AccessPath::PARALLEL_COLLECTOR_SCAN:
      break;
    case AccessPath::FILTER:
      if (do_rewrite(path->filter().child)) return true;
      break;
    case AccessPath::SORT:
      if (do_rewrite(path->sort().child)) return true;
      break;
    case AccessPath::AGGREGATE:
      if (do_rewrite(path->aggregate().child)) return true;
      break;
    case AccessPath::TEMPTABLE_AGGREGATE:
      if (do_rewrite(path->temptable_aggregate().subquery_path)) return true;
      break;
    case AccessPath::LIMIT_OFFSET:
      if (do_rewrite(path->limit_offset().child)) return true;
      break;
    case AccessPath::STREAM:
      if (do_rewrite(path->stream().child)) return true;
      break;
    case AccessPath::MATERIALIZE: {
      assert(path->materialize().param->query_blocks.size() == 1);
      MaterializePathParameters::QueryBlock &query_block =
          path->materialize().param->query_blocks[0];
      if (do_rewrite(query_block.subquery_path)) return true;
      break;
    }
    default:
      assert(false);
  }

  return rewrite_each_access_path(path);
}

bool AccessPathRewriter::rewrite_each_access_path(AccessPath *path) {
  // Do access path modification
  AccessPath *out = nullptr;
  switch (path->type) {
    case AccessPath::TABLE_SCAN: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_table_scan(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::INDEX_SCAN: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_index_scan(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::REF: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_ref(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::REF_OR_NULL: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_ref_or_null(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::EQ_REF: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_eq_ref(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::INDEX_RANGE_SCAN: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_index_range_scan(path, out)) return true;
      m_out_path = out;
      break;
    }
    case AccessPath::FILTER: {
      assert(!path->filter().materialize_subqueries);
      // HAVING is also a filter access path
      out = end_of_out_path() ? nullptr : accesspath_dup(path, mem_root());
      if (rewrite_filter(path, out)) return true;
      if (out) out->filter().child = m_out_path;
      break;
    }
    case AccessPath::SORT: {
      out = end_of_out_path() ? nullptr : accesspath_dup(path, mem_root());
      if (rewrite_sort(path, out)) return true;
      if (out) out->sort().child = m_out_path;
      break;
    }
    case AccessPath::AGGREGATE: {
      // We only push down plan nodes under group node
      if (end_of_out_path()) break;

      out = accesspath_dup(path, mem_root());
      if (rewrite_aggregate(path, out)) return true;
      auto &out_aggregate = out->aggregate();
      out_aggregate.child = m_out_path;
      break;
    }
    case AccessPath::TEMPTABLE_AGGREGATE: {
      assert(!end_of_out_path());
      out = accesspath_dup(path, mem_root());
      if (rewrite_temptable_aggregate(path, out)) return true;
      auto &out_temptable_aggregate = out->temptable_aggregate();
      out_temptable_aggregate.subquery_path = m_out_path;
      break;
    }
    case AccessPath::LIMIT_OFFSET: {
      assert(!path->limit_offset().send_records_override);
      out = end_of_out_path() ? nullptr : accesspath_dup(path, mem_root());
      if (rewrite_limit_offset(path, out)) return true;
      if (out) out->limit_offset().child = m_out_path;
      break;
    }
    case AccessPath::STREAM: {
      out = end_of_out_path() ? nullptr : accesspath_dup(path, mem_root());
      if (rewrite_stream(path, out)) return true;
      if (out) out->stream().child = m_out_path;
      break;
    }
    case AccessPath::MATERIALIZE: {
      out = end_of_out_path() ? nullptr : accesspath_dup(path, mem_root());
      if (rewrite_materialize(path, out)) return true;
      if (out) {
        auto &out_materialize = out->materialize();
        out_materialize.param->query_blocks[0].subquery_path = m_out_path;
      }
      break;
    }
    default:
      assert(false);
  }
  if (out) {
    m_out_path = out;
    post_rewrite_out_path(m_out_path);
  }
  return false;
}
AccessPathParallelizer::AccessPathParallelizer(
    Item_clone_context *item_clone_context, JOIN *join_in,
    PartialPlan *partial_plan, AccessPathChangesStore *path_changes_store)
    : AccessPathRewriter(item_clone_context, join_in, partial_plan->Join()),
      m_partial_plan(partial_plan),
      m_path_changes_store(path_changes_store) {}

AccessPath *AccessPathParallelizer::parallelize_access_path(AccessPath *in) {
  if (do_rewrite(in)) return nullptr;
  // All plan is pushed down, so just replace whole plan with collector path
  if (!collector_path_pos()) return collector_access_path();
  m_path_changes_store->register_collector_path(m_collector_path_pos);
  *m_collector_path_pos = collector_access_path();
  return in;
}

void AccessPathParallelizer::post_rewrite_out_path(AccessPath *out) {
  if (m_fake_timing_iterator) out->iterator = m_fake_timing_iterator;
}

void AccessPathParallelizer::set_collector_path_pos(AccessPath **path) {
  m_collector_path_pos = path;
  // collector always use collector table to sort
  AccessPath *collector_path = collector_access_path();
  if (!m_sorting_info.table)
    set_sorting_info({collector_path->parallel_collector_scan().table,
                      REF_SLICE_SAVED_BASE});
}

void AccessPathParallelizer::rewrite_index_access_path(
    bool use_order, bool reverse) {
  auto *join = m_join_in;
  if (use_order) {
    assert(join->m_ordered_index_usage != JOIN::ORDERED_INDEX_VOID);

    merge_sort = join->m_ordered_index_usage == JOIN::ORDERED_INDEX_ORDER_BY
                     ? join->order.order
                     : (join->group_list_planned.empty()
                            ? join->group_list.order
                            : join->group_list_planned.order);
  }
  if (reverse) m_partial_plan->SetTableParallelScanReverse();
}

bool AccessPathParallelizer::rewrite_index_scan(AccessPath *in, AccessPath *out
                                                [[maybe_unused]]) {
  assert(out);
  auto &index_scan = in->index_scan();
  rewrite_index_access_path(index_scan.use_order, index_scan.reverse);
  return false;
}

bool AccessPathParallelizer::rewrite_ref(AccessPath *in,
                                         AccessPath *out [[maybe_unused]]) {
  assert(out);
  auto &ref = in->ref();
  rewrite_index_access_path(ref.use_order, ref.reverse);
  return false;
}

bool AccessPathParallelizer::rewrite_ref_or_null(AccessPath *in, AccessPath *out
                                                 [[maybe_unused]]) {
  assert(out);
  auto &ref_or_null = in->ref_or_null();
  rewrite_index_access_path(ref_or_null.use_order, false);
  return false;
}

bool AccessPathParallelizer::rewrite_eq_ref(AccessPath *in,
                                            AccessPath *out [[maybe_unused]]) {
  assert(out);
  auto &eq_ref = in->eq_ref();

  rewrite_index_access_path(eq_ref.use_order, false);
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
      m_join_in->m_ordered_index_usage != JOIN::ORDERED_INDEX_VOID,
      quick->reverse_sorted());

  return false;
}

static TABLE *rewrite_temptable_scan_path(AccessPath *table_path,
                                               TABLE *table) {
  TABLE **table_ptr = nullptr;;
  assert(table_path->type == AccessPath::TABLE_SCAN ||
         table_path->type == AccessPath::INDEX_RANGE_SCAN ||
         table_path->type == AccessPath::FOLLOW_TAIL);
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
    default:
      assert(false);
  }
  assert(table_ptr);
  auto *old_table = *table_ptr;
  *table_ptr = table;

  return old_table;
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

static bool recreate_materialized_table(THD *thd, JOIN *join, ORDER *group,
                                        bool distinct,
                                        bool save_sum_fields,
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
                                                 AccessPath *out) {
  if (out && super::rewrite_materialize(in, out)) return true;
  auto *src = in->materialize().param;
  auto &query_block = src->query_blocks[0];
  // If MATERIALIZE is a child of AGGREGATE, we should create it on leader.
  if (out && !MaterializeIsDoingDeduplication(src->table)) {
#ifndef NDEBUG
    // This should be SQL_BUFFER_RESULT or do DISTINCT by LIMIT 1, We just push
    // down this to worker and leader do nothing. see ConnectJoins()
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
  if (out && src->table->group) {
    assert(m_join_out->group_list.empty());
    // Uses JOIN::group_list to save TABLE::group of this access path to avoid
    // overwrite original group of temporary table.
    m_join_out->group_list = ORDER_with_src(
        clone_order_list(src->table->group,
                         {mem_root(), &m_join_out->query_block->base_ref_items,
                          m_join_out->fields->size(), nullptr}),
        ESC_GROUP_BY);
  }

  m_path_changes_store->register_changes(
      NewAccessPathChanges(in, src->table, query_block.temp_table_param));
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

  if (out) {
    out->materialize().table_path =
        accesspath_dup(in->materialize().table_path, mem_root());
    post_rewrite_out_path(out->materialize().table_path);
  }

  TABLE *old_table =
      rewrite_temptable_scan_path(in->materialize().table_path, table);
  m_path_changes_store->register_changes(
      NewAccessPathChanges(in->materialize().table_path, old_table));

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

  m_path_changes_store->register_changes(
      NewAccessPathChanges(in, tagg.table, tagg.temp_table_param));
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
    out->temptable_aggregate().table_path =
        accesspath_dup(tagg.table_path, mem_root());
    post_rewrite_out_path(out->temptable_aggregate().table_path);
  }
  TABLE *old_table = rewrite_temptable_scan_path(tagg.table_path, table);
  m_path_changes_store->register_changes(
      NewAccessPathChanges(tagg.table_path, old_table));

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
  auto &stream = in->stream();
  m_path_changes_store->register_changes(
      NewAccessPathChanges(in, stream.table, stream.temp_table_param));
  return do_stream_rewrite(m_join_in, in);
}

bool AccessPathParallelizer::rewrite_filter(AccessPath *in [[maybe_unused]],
                                            AccessPath *) {
  // XXX clone condition to partial plan
  assert(in->filter().condition->parallel_safe() == Item_parallel_safe::Safe);
  m_join_out->where_cond = in->filter().condition;

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
    m_join_out->order = m_join_in->order;
    m_join_out->order.order = sort_out.filesort->src_order;
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
  ORDER_with_src group_list = m_join_in->group_list_planned.empty()
                                  ? m_join_in->group_list
                                  : m_join_in->group_list_planned;
  m_join_out->group_list =
      group_list.clone({mem_root(), &m_join_out->query_block->base_ref_items,
                        m_join_out->fields->size(), nullptr});

  // Recreate group fields original join since underlying table changed
  if (!m_join_in->group_list_planned.empty()) {
    assert(m_join_in->group_list.empty());
    m_path_changes_store->register_changes(NewAccessPathChanges(in, m_join_in));
    m_join_in->group_list = m_join_in->group_list_planned;
  }
  m_join_in->group_fields_cache.clear();
  m_join_in->group_fields.destroy_elements();
  if (make_group_fields(m_join_in, m_join_in)) return true;

  set_collector_path_pos(&in->aggregate().child);
  return false;
}

bool AccessPathParallelizer::rewrite_limit_offset(AccessPath *in,
                                                  AccessPath *out) {
  if (!out) return false;

  assert(!collector_path_pos());

  out->limit_offset().limit = in->limit_offset().limit;
  m_join_out->m_select_limit = out->limit_offset().limit;
  // Don't push down offset to worker
  out->limit_offset().offset = 0;
  set_collector_path_pos(&in->limit_offset().child);
  m_pushed_limit_offset = true;
  return false;
}

void AccessPathChanges::restore() {
  switch (access_path->type) {
    case AccessPath::MATERIALIZE: {
      auto *src = access_path->materialize().param;
      auto &query_block = src->query_blocks[0];
      src->table = u.recreated_temp_table.table;
      query_block.temp_table_param = u.recreated_temp_table.temp_table_param;
      break;
    }
    case AccessPath::TEMPTABLE_AGGREGATE: {
      auto &tagg = access_path->temptable_aggregate();
      tagg.table = u.recreated_temp_table.table;
      tagg.temp_table_param = u.recreated_temp_table.temp_table_param;
      break;
    }
    case AccessPath::STREAM: {
      auto &stream = access_path->stream();
      stream.table = u.recreated_temp_table.table;
      stream.temp_table_param = u.recreated_temp_table.temp_table_param;
      break;
    }
    case AccessPath::AGGREGATE:
      u.aggregate.join->group_list.clean();
      break;
    case AccessPath::TABLE_SCAN:
      [[fallthrough]];
    case AccessPath::INDEX_RANGE_SCAN:
      [[fallthrough]];
    case AccessPath::FOLLOW_TAIL:
      rewrite_temptable_scan_path(access_path, u.table_path.table);
      break;
    default:
      assert(false);
      break;
  }
}

TABLE *PartialAccessPathRewriter::find_leaf_table(TABLE *) const {
  Query_block *query_block = m_join_out->query_block;
  auto *table_list = query_block->leaf_tables;

  assert(!table_list->next_leaf);
  assert(table_list->table);

  return table_list->table;
}

/// XXX TDSQL seems that pushed_cond also is used in LIMIT pushdown, so
/// should it also be cloned.
static bool clone_handler_pushed_cond(Item_clone_context *context, uint keyno,
                                      const TABLE *from, TABLE *table) {
  Item *cond, *from_cond = from->file->pushed_idx_cond;
  if (from_cond) {
    assert(keyno < MAX_KEY);
    if (!(cond = from_cond->clone(context))) return true;

    auto *remained_cond [[maybe_unused]] =
        table->file->idx_cond_push(keyno, cond);
    assert(remained_cond == nullptr);
  }

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

bool PartialAccessPathRewriter::rewrite_materialize(AccessPath *in,
                                                    AccessPath *out) {
  assert(out);
  if (super::rewrite_materialize(in, out)) return true;
  auto *dest_param = out->materialize().param;
  THD *thd = m_join_out->thd;
  Query_block *query_block_out = m_join_out->query_block;
  Query_block *query_block_in = m_join_in->query_block;
  if (m_join_out->alloc_ref_item_slice(thd, dest_param->ref_slice)) return true;
  auto &query_block = dest_param->query_blocks[0];
  ORDER *group =
      m_join_in->group_list.empty()
          ? nullptr
          : clone_order_list(m_join_in->group_list.order,
                             {thd->mem_root, &query_block_out->base_ref_items,
                              query_block_out->fields.size(),
                              &query_block_in->base_ref_items[0]});

  if (recreate_materialized_table(thd, m_join_out, group,
                                  dest_param->table->s->is_distinct, false,
                                  dest_param->ref_slice, dest_param->limit_rows,
                                  &dest_param->table, &query_block.temp_table_param))
    return true;
  TABLE *table = dest_param->table;
  auto *temp_table_param = query_block.temp_table_param;
  // See setup_tmptable_write_func()
  if (temp_table_param->precomputed_group_by) {
    for (Item_sum **func_ptr = m_join_out->sum_funcs; *func_ptr != nullptr;
         ++func_ptr) {
      if (temp_table_param->items_to_copy->push_back(
              Func_ptr(*func_ptr, (*func_ptr)->get_result_field())))
        return true;
    }
  }
  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_out->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, temp_table_param});
  set_sorting_info({table, dest_param->ref_slice});
  out->materialize().table_path =
      accesspath_dup(out->materialize().table_path, mem_root());
  rewrite_temptable_scan_path(out->materialize().table_path, table);
  post_rewrite_out_path(out->materialize().table_path);
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

  tagg.table_path = accesspath_dup(tagg.table_path, mem_root());
  rewrite_temptable_scan_path(tagg.table_path, tagg.table);
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
