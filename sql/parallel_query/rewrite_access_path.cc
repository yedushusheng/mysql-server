#include "rewrite_access_path.h"

#include "my_alloc.h"
#include "sql/filesort.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"

namespace pq {
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
      assert(end_of_out_path());
      out = nullptr;
      if (rewrite_stream(path, out)) return true;
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
  if (out) m_out_path = out;
  return false;
}

AccessPath *AccessPathParallelizer::parallelize_access_path(AccessPath *in) {
  if (do_rewrite(in)) return nullptr;
  // All plan is pushed down, so just replace whole plan with collector path
  if (!collector_path_pos()) return collector_access_path();
  *m_collector_path_pos = collector_access_path();
  return in;
}

void AccessPathParallelizer::set_collector_path_pos(AccessPath **path) {
  m_collector_path_pos = path;
  // collector always use collector table to sort
  AccessPath *collector_path = collector_access_path();
  if (!m_sorting_info.table)
    set_sorting_info({collector_path->parallel_collector_scan().table,
                      REF_SLICE_SAVED_BASE});
}

bool AccessPathParallelizer::init_table_parallel_scan(TABLE *table, uint keynr,
                                                      bool reverse) {
  key_range *min_key = nullptr, *max_key = nullptr;
  bool is_asc = reverse;
  uint16_t key_used = UINT16_MAX;
  parallel_scan_desc_t scan_desc = {keynr, min_key, max_key, key_used, is_asc};
  ulong nranges = 100 * m_join_in->thd->variables.max_parallel_degree;
  int res;

  if ((res = table->file->init_parallel_scan(&table->parallel_scan_handle,
                                             &nranges, &scan_desc)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

bool AccessPathParallelizer::rewrite_table_scan(AccessPath *in, AccessPath *out
                                                [[maybe_unused]]) {
  assert(out);
  TABLE *table = in->table_scan().table;

  return init_table_parallel_scan(table, table->s->primary_key, false);
}

bool AccessPathParallelizer::rewrite_index_scan(AccessPath *in, AccessPath *out
                                                [[maybe_unused]]) {
  assert(out);
  auto &index_scan = in->index_scan();
  TABLE *table = index_scan.table;
  if (index_scan.use_order) {
    assert(m_join_in->m_ordered_index_usage != JOIN::ORDERED_INDEX_VOID);

    merge_sort = m_join_in->m_ordered_index_usage == JOIN::ORDERED_INDEX_ORDER_BY
                     ? m_join_in->order.order
                     : (m_join_in->group_list_planned.empty()
                            ? m_join_in->group_list.order
                            : m_join_in->group_list_planned.order);
  }
  return init_table_parallel_scan(table, index_scan.idx, index_scan.reverse);
}

static void rewrite_temptable_scan_path(AccessPath *table_path,
                                               TABLE *table) {
  assert(table_path->type == AccessPath::TABLE_SCAN ||
         table_path->type == AccessPath::INDEX_RANGE_SCAN ||
         table_path->type == AccessPath::FOLLOW_TAIL);
  switch (table_path->type) {
    case AccessPath::TABLE_SCAN:
      table_path->table_scan().table = table;
      break;
    case AccessPath::INDEX_RANGE_SCAN:
      table_path->index_range_scan().table = table;
      break;
    case AccessPath::FOLLOW_TAIL:
      table_path->follow_tail().table = table;
      break;
    default:
      assert(false);
  }
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
  *table = create_tmp_table(
      thd, *tmp_table_param, *curr_fields, group, distinct, save_sum_fields,
      join->query_block->active_options(), limit_rows, "");
  if (!*table) return true;
  (*table)->alias = "<temporary>";
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

  TABLE *orig_table [[maybe_unused]] = src->table;
  if (recreate_materialized_table(m_join_in->thd, m_join_in, src->table->group,
                                  src->table->s->is_distinct, false,
                                  src->ref_slice, src->limit_rows, &src->table,
                                  &query_block.temp_table_param))
    return true;
  TABLE *table = src->table;

  assert_same_rewrite_table(orig_table, table);

  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_in->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, query_block.temp_table_param});

  if (out)
    out->materialize().table_path =
        accesspath_dup(in->materialize().table_path, mem_root());
  rewrite_temptable_scan_path(in->materialize().table_path, table);

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

  if (out)
    out->temptable_aggregate().table_path =
        accesspath_dup(tagg.table_path, mem_root());
  rewrite_temptable_scan_path(tagg.table_path, table);
  set_sorting_info({table, tagg.ref_slice});
  set_collector_path_pos(&tagg.subquery_path);
  return false;
}

bool AccessPathParallelizer::rewrite_stream(AccessPath *in, AccessPath *) {
  auto &stream = in->stream();
  if (recreate_materialized_table(m_join_in->thd, m_join_in, nullptr,
                                  false, true, stream.ref_slice, HA_POS_ERROR,
                                  &stream.table, &stream.temp_table_param))
    return true;

  // XXX see setup_tmptable_write_func(), do some refactor for this. Also
  // the case OT_MATERIALIZE and precomputed_group_by process

  Temp_table_param *const tmp_tbl = stream.temp_table_param;
  if (m_join_in->streaming_aggregation && !tmp_tbl->precomputed_group_by) {
    for (Item_sum **func_ptr = m_join_in->sum_funcs; *func_ptr != nullptr;
         ++func_ptr) {
      tmp_tbl->items_to_copy->push_back(
          Func_ptr(*func_ptr, (*func_ptr)->get_result_field()));
    }
  }
  set_sorting_info({stream.table, stream.ref_slice});
  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_in->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{stream.table, stream.temp_table_param});
  return false;
}

bool AccessPathParallelizer::rewrite_filter(AccessPath *in, AccessPath *) {
  // XXX clone condition to partial plan
  assert(in->filter().condition->parallel_safe() == Item_parallel_safe::Safe);
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
  ORDER_with_src group_list = m_join_in->group_list_planned.empty()
                                  ? m_join_in->group_list
                                  : m_join_in->group_list_planned;
  m_join_out->group_list =
      group_list.clone({mem_root(), &m_join_out->query_block->base_ref_items,
                        m_join_out->fields->size(), nullptr});

  // Recreate group fields original join since underlying table changed
  if (!m_join_in->group_list_planned.empty()) {
    assert(m_join_in->group_list.empty());
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
  // Don't push down offset to worker
  out->limit_offset().offset = 0;
  set_collector_path_pos(&in->limit_offset().child);
  return false;
}

TABLE *PartialAccessPathRewriter::find_leaf_table(TABLE *) const {
  Query_block *query_block = m_join_out->query_block;
  auto *table_list = query_block->leaf_tables;

  assert(!table_list->next_leaf);

  return table_list->table;
}

static bool attach_table_parallel_scan(parallel_scan_handle_t handle,
                                       TABLE *table) {
  if (!handle) return false;

  table->parallel_scan_handle = handle;

  int res;
  if ((res = table->file->attach_parallel_scan(handle)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

bool PartialAccessPathRewriter::rewrite_table_scan(AccessPath *in,
                                                   AccessPath *out) {
  auto &out_table_scan = out->table_scan();
  auto ps_handle = in->table_scan().table->parallel_scan_handle;
  TABLE *table = find_leaf_table(out_table_scan.table);
  out_table_scan.table = table;

  if (attach_table_parallel_scan(ps_handle, table)) return true;

  set_sorting_info({table, REF_SLICE_SAVED_BASE});
  return false;
}

bool PartialAccessPathRewriter::rewrite_index_scan(AccessPath *in,
                                                   AccessPath *out) {
  auto &out_index_scan = out->index_scan();
  auto ps_handle = in->index_scan().table->parallel_scan_handle;
  TABLE *table = find_leaf_table(out_index_scan.table);
  out_index_scan.table = table;
  if (attach_table_parallel_scan(ps_handle, table)) return true;

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
      clone_order_list(m_join_in->group_list.order,
                       {thd->mem_root, &query_block_out->base_ref_items,
                        query_block_out->fields.size(),
                        &query_block_in->base_ref_items[0]});

  if (recreate_materialized_table(thd, m_join_out, group,
                                  dest_param->table->s->is_distinct, false,
                                  dest_param->ref_slice, dest_param->limit_rows,
                                  &dest_param->table, &query_block.temp_table_param))
    return true;
  TABLE *table = dest_param->table;
  // We made a new table, so make sure it gets properly cleaned up
  // at the end of execution.
  m_join_out->temp_tables.push_back(
      JOIN::TemporaryTableToCleanup{table, query_block.temp_table_param});
  set_sorting_info({table, dest_param->ref_slice});
  out->materialize().table_path =
      accesspath_dup(out->materialize().table_path, mem_root());
  rewrite_temptable_scan_path(out->materialize().table_path, table);
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

bool PartialAccessPathRewriter::rewrite_stream(AccessPath *, AccessPath *) {
  assert(false);
  return false;
}
}  // namespace pq
