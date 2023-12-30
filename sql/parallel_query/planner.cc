#include "sql/parallel_query/planner.h"

#include "scope_guard.h"
#include "sql/filesort.h"
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/opt_trace.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/rewrite_access_path.h"
#include "sql/sql_join_buffer.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"

namespace pq {

static bool IsItemParallelSafe(const Item *item) {
  assert(item);
  return item->parallel_safe() == Item_parallel_safe::Safe;
}

static void ChooseParallelPlan(JOIN *join) {
  THD *thd = join->thd;
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object wrapper(trace);
  Opt_trace_object trace_choosing(trace, "considering");
  bool chosen = false;
  const char *cause = nullptr;
  auto trace_set_guard =
      create_scope_guard([&trace_choosing, &chosen, &cause]() {
        trace_choosing.add("chosen", chosen);
        if (!chosen) trace_choosing.add_alnum("cause", cause);
      });
  if (thd->variables.max_parallel_degree == 0) {
    cause = "max_parallel_degree_is_not_set";
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
  // Only support table scan and index scan
  QEP_TAB *tab = &join->qep_tab[0];
  if (tab->type() != JT_ALL && tab->type() != JT_INDEX_SCAN) {
    cause = "access_type_is_not_table_scan_or_index_scan";
    return;
  }
  for (Item *item : join->query_block->fields) {
    if (IsItemParallelSafe(item)) continue;
    cause = "include_unsafe_items_in_fields";
    return;
  }
  if (tab->condition() && !IsItemParallelSafe(tab->condition())) {
    cause = "filter_has_unsafe_condition";
    return;
  }
  if (join->having_cond && !IsItemParallelSafe(join->having_cond)) {
    cause = "having_is_unsafe";
    return;
  }

  chosen = true;
  join->parallel_plan =
      new (thd->mem_root) ParallelPlan(thd->mem_root, join->query_block);
}

bool GenerateParallelPlan(JOIN *join) {
  THD *thd = join->thd;
  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object trace_wrapper(trace);
  Opt_trace_object trace_parallel_plan(trace, "parallel_plan");
  Opt_trace_array trace_steps(trace, "steps");

  ChooseParallelPlan(join);
  if (thd->is_error()) return true;

  ParallelPlan *parallel_plan = join->parallel_plan;

  if (!parallel_plan) return false;
  bool fallback;
  if (parallel_plan->Generate(fallback)) return true;

  if (fallback) join->parallel_plan = nullptr;
  return false;
}

Query_expression *PartialPlan::QueryExpression() const {
  return m_query_block->master_query_expression();
}

JOIN *PartialPlan::Join() const { return m_query_block->join; }

ParallelPlan::ParallelPlan(MEM_ROOT *mem_root, Query_block *query_block)
    : m_fields(mem_root), m_source_query_block(query_block) {}

JOIN *ParallelPlan::PartialJoin() const {
  return m_partial_plan.Join();
}

JOIN *ParallelPlan::SourceJoin() const {
  return m_source_query_block->join;
}

THD *ParallelPlan::thd() const {
  JOIN *join = m_source_query_block->join;
  assert(join);
  return join->thd;
}

bool ParallelPlan::AddPartialLeafTables() {
  Query_block *source = m_source_query_block;
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
};

bool ParallelPlan::GenPartialFields(Item_clone_context *context,
                                    FieldsPushdownDesc *fields_pushdown_desc) {
  Query_block *source = SourceQueryBlock();
  Query_block *partial_query_block = PartialQueryBlock();

  for (uint i = 0; i < source->fields.size(); i++) {
    Item *item = source->fields[i];
    Item *new_item;
    // if ONLY_FULL_GROUP_BY is turned off, the functions contains aggregation
    // may include some item fields.
    // XXX check whether all sums is safe to push down.
    bool pushdown =
        item->parallel_safe() == Item_parallel_safe::Safe &&
        !(item->has_aggregation() && item->type() != Item::SUM_FUNC_ITEM);

    if (pushdown) {
      if (!(new_item = item->clone(context))) return true;
      if (new_item->type() == Item::SUM_FUNC_ITEM) {
        Item_sum *item_sum = down_cast<Item_sum *>(new_item);
        item_sum->set_sum_stage(Item_sum::TRANSITION_STAGE);
        // change item_name so let EXPLAIN doesn't show Sum(Sum(a)) for leader's
        // aggregation
        Item *arg = item_sum->get_arg(0);
        item_sum->item_name = arg->item_name;
      }
      if (partial_query_block->add_item_to_list(new_item)) return true;
      if (fields_pushdown_desc->push_back(item->type() != Item::SUM_FUNC_ITEM
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
                           TABLE *collector_table,
                           mem_root_deque<Item *> &partial_fields)
      : Item_clone_context(thd, query_block),
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
    assert(item_func->arg_count <= 1);
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

  void repoint_ref(Item_ref *item_ref, const Item_ref *from) override {
    assert(from->ref);
    auto &base_ref_items = m_query_block->base_ref_items;
    for (uint i = 0; i < m_query_block->fields.size(); i++) {
      auto *item = base_ref_items[i];
      if (!item) break;
      if (item->is_identical(*from->ref)) {
        item_ref->ref = &base_ref_items[i];
        break;
      }
    }
    assert(item_ref->ref);
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

  OriginItemRewriteContext clone_context(thd, source_query_block,
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
      // XXXXXX change source plan
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

    // XXXXXX change source plan
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
  join->set_executed();
  join->primary_tables = source_join->primary_tables;
  join->const_tables = source_join->const_tables;
  join->tables = source_join->tables;

  if (AddPartialLeafTables()) return true;

  if (!(*partial_clone_context =
            new (mem_root) PartialItemGenContext(thd, partial_query_block)))
    return true;

  if (GenPartialFields(*partial_clone_context, fields_pushdown_desc))
    return true;

  // XXX Set JOIN::select_distinct and JOIN::select_count
  // XXX collect pushed down fields from fields, table_ref, conditions and
  // having

  join->implicit_grouping = source_join->implicit_grouping;

  // XXX Don't need where_cond, Explain need this?
  join->where_cond = nullptr;

  // XXX group by and order by generating
  if (setup_partial_base_ref_items()) return true;

  if (ClonePartialOrders()) return true;

  count_field_types(partial_query_block, &join->tmp_table_param,
                    partial_query_block->fields, false, false);
  if (join->alloc_func_list() ||
      join->make_sum_func_list(partial_query_block->fields, false))
    return true;

  if (CreateCollector(thd)) return true;

  // We don't support blob type in row exchange
  TABLE *table = m_collector->CollectorTable();
  if (table->s->blob_fields > 0) {
    trace.add_alnum("fallback_cause", "have_blob_fields");
    return true;
  }
  return false;
}

bool ParallelPlan::Generate(bool &fallback) {
  JOIN *source_join = SourceJoin();
  THD *thd = source_join->thd;
  MEM_ROOT *mem_root = thd->mem_root;
  Query_block *source_query_block = source_join->query_block;
  Item_clone_context *partial_clone_context;
  FieldsPushdownDesc fields_pushdown_desc(mem_root);

  Opt_trace_context *const trace = &thd->opt_trace;
  Opt_trace_object trace_wrapper(trace);
  Opt_trace_object trace_generating(trace, "generating");
  Opt_trace_array trace_steps(trace, "steps");

  fallback = false;
  if (GeneratePartialPlan(&partial_clone_context, &fields_pushdown_desc)) {
    DestroyCollector();
    Opt_trace_object trace_fallback(trace);
    trace_fallback.add("fallback", true);
    fallback = true;
    return false;
  }

  if (GenFinalFields(&fields_pushdown_desc)) return true;

  // XXXXXX change source plan
  source_join->fields = &m_fields;
  source_join->tmp_fields[REF_SLICE_ACTIVE] = m_fields;
  source_join->ref_items[REF_SLICE_ACTIVE] = source_query_block->base_ref_items;
  if (!source_join->ref_items[REF_SLICE_SAVED_BASE].is_null())
    source_join->copy_ref_item_slice(REF_SLICE_SAVED_BASE, REF_SLICE_ACTIVE);

  if (GenerateAccessPath(partial_clone_context)) return true;

  return false;
}

bool ParallelPlan::CreateCollector(THD *thd) {
  if (!(m_collector = new (thd->mem_root)
            Collector(thd->variables.max_parallel_degree, &m_partial_plan)) ||
      m_collector->CreateCollectorTable()) {
    destroy(m_collector);
    m_collector = nullptr;
    return true;
  }

  return false;
}

void ParallelPlan::EndCollector(THD *thd, ha_rows *found_rows) {
  m_collector->End(thd, found_rows);
}
void ParallelPlan::DestroyCollector() {
  if (!m_collector) return;
  destroy(m_collector);
  m_collector = nullptr;
}

AccessPath *ParallelPlan::CreateCollectorAccessPath(THD *thd) {
  return NewParallelCollectorAccessPath(thd, m_collector,
                                        m_collector->CollectorTable(), true);
}

bool ParallelPlan::GenerateAccessPath(Item_clone_context *clone_context) {
  JOIN *source_join = SourceJoin();
  JOIN *partial_join = PartialJoin();
  THD *thd = source_join->thd;
  AccessPath *partial_path;
  AccessPath *parallelized_path;
  AccessPathParallelizer rewriter(clone_context, source_join, partial_join);
  Opt_trace_object trace_wrapper(&thd->opt_trace);
  Opt_trace_object trace(&thd->opt_trace, "access_path_rewriting");

  assert(source_join->root_access_path());
  rewriter.set_collector_access_path(CreateCollectorAccessPath(thd));

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

  partial_join->set_root_access_path(partial_path);
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
    if (!(added = query_block->add_table_to_list(
              thd, &table_ident, tl->alias, table_options,
              tl->lock_descriptor().type, tl->mdl_request.type)))
      return true;

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