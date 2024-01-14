#include "sql/parallel_query/plan_deparser.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/item_sum.h"
#include "sql/filesort.h"

using std::string;

namespace pq {
PlanDeparser::PlanDeparser(Query_block *query_block)
    : m_query_block(query_block), m_deparse_fields(thd()->mem_root) {}

THD *PlanDeparser::thd() { return m_query_block->join->thd; }

/*
 * create the select fields from query block
 */
bool PlanDeparser::deparse_select_items(THD *thd) {
  auto *join = m_query_block->join;
  // transform fields, e.g. avg, min, max
  bool has_group_by = !join->group_list.empty();

  // Has sum_funcs and not gorup list? For count(*) but no group list, it
  // needs to handle specially.
  m_count_appended = !has_group_by && *join->sum_funcs != nullptr;
  // Don't append count(0) if there are only COUNT
  bool sum_only_has_count = true;

  uint cur_item_index = 0;
  for (auto *item : m_query_block->visible_fields()) {
    DeField *defield;
    Item_sum *item_sum = item->type() == Item::SUM_FUNC_ITEM
                             ? down_cast<Item_sum *>(item)
                             : nullptr;

    DeField::type detyp;
    // If it is not an AVG item, then add it to the select fields directly.
    if (!item_sum)
      detyp = DeField::NORMAL_ITEM;
    else if (item_sum->sum_func() == Item_sum::AVG_FUNC)
      detyp = DeField::AVG;
    else {
      detyp = DeField::NORMAL_ITEM;
      if (item_sum->sum_func() != Item_sum::COUNT_FUNC)
        sum_only_has_count = false;
    }

    auto ind = cur_item_index++;
    auto aux_ind =
        detyp == DeField::AVG ? cur_item_index++ : INVALID_FIELD_INDEX;
    if (!(defield = new (thd->mem_root) DeField(detyp, item, ind, aux_ind)) ||
        m_deparse_fields.push_back(defield))
      return true;
  }
  if (m_count_appended && sum_only_has_count) m_count_appended = false;

  if (m_count_appended &&
      m_deparse_fields.push_back(new (thd->mem_root) DeField(
          DeField::COUNT, nullptr, cur_item_index++, INVALID_FIELD_INDEX)))
    return true;

  /*
    Even for the explain analyze, it only needs to append "select" because it
    only collects the analyze information of workers but not the details which
    the worker executes in the data nodes.
  */
  m_statement.append(STRING_WITH_LEN("select "));
  bool first = true;
  constexpr enum_query_type qt_deparse =
      enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
  for (auto *af : m_deparse_fields) {
    if (first)
      first = false;
    else
      m_statement.append(STRING_WITH_LEN(","));
    if (af->m_type == DeField::NORMAL_ITEM) {
      af->item->print_item_w_name(thd, &m_statement, qt_deparse);
      continue;
    }
    if (af->m_type == DeField::COUNT) {
      m_statement.append(STRING_WITH_LEN("count(0)"));
      continue;
    }

    m_statement.append(STRING_WITH_LEN("sum("));
    auto *item_sum = down_cast<Item_sum *>(af->item);
    item_sum->get_arg(0)->print(thd, &m_statement, qt_deparse);
    m_statement.append(STRING_WITH_LEN("),count("));
    item_sum->get_arg(0)->print(thd, &m_statement, qt_deparse);
    m_statement.append(STRING_WITH_LEN(")"));
  }

  if (m_deparse_fields.empty()) m_statement.append(STRING_WITH_LEN("1"));;

  return false;
}

void PlanDeparser::deparse_field(Field *field) {
  const char *table_name = field->orig_table_alias ? field->orig_table_alias : field->orig_table_name;
  // 1. select x, expr as a1 from t group by a1; 
  // 2. select c1,c2,xx from t group by 1 order by 2;
  // the field can't find the table name, just ignore
  if (table_name) {
    append_identifier(&m_statement, table_name, strlen(table_name)); 
    m_statement.append(STRING_WITH_LEN("."));
  }
  const char *field_name = field->orig_field_name ? field->orig_field_name : field->field_name;
  append_identifier(&m_statement, field_name, strlen(field_name)); 
}

void PlanDeparser::deparse_tmp_hash_group(ORDER *group) {
  bool first = true;
  for (ORDER *ord = group; ord; ord = ord->next) {
    if (first) first = false;
    else {
      m_statement.append(STRING_WITH_LEN(","));
    }
    Field *field = ord->field_in_tmp_table;
    deparse_field(field);
  }
}

bool PlanDeparser::deparse_materialize_access_path(THD *thd,
                                                   const AccessPath *path,
                                                   String &cond, String &join_cond) {
  MaterializePathParameters *param = path->materialize().param;
  bool first = true;
  for (const MaterializePathParameters::QueryBlock &query_block :
       param->query_blocks) {
    if (!first) m_statement.append(STRING_WITH_LEN(","));
    deparse_access_path(thd, query_block.subquery_path, cond, join_cond);
    first = false;
  }
  // group by use tmp table to remove duplicate
  TABLE *table = param->table;
  assert(table);
  if (table->hash_field != nullptr) {
    deparse_where(thd);
    m_statement.append(STRING_WITH_LEN(" group by "));
    // FIXME: if no group return my_error
    if (table->group) {
      deparse_tmp_hash_group(table->group);  
      return false;
    } else {
      my_error(ER_PARALLEL_DEPARSE_ERROR, MYF(0), "can't find group by fields");
      return true;
    }
  }

  if (table->key_info != nullptr) {
    for (size_t i = 0; i < table->s->keys; ++i) {
      if ((table->key_info[i].flags & HA_NOSAME) != 0) {
        deparse_where(thd);
        m_statement.append(STRING_WITH_LEN(" group by "));
        first = true;
        KEY_PART_INFO *key_part = table->key_info[i].key_part;
        KEY_PART_INFO *key_part_end = key_part + table->key_info[i].user_defined_key_parts;
        for (; key_part < key_part_end; key_part++) {
          if (!first) {
            m_statement.append(STRING_WITH_LEN(", "));
          } else {
            first = false;
          }
          Field *field = key_part->field;
          deparse_field(field);
        }
        return false; // process the first unique key
      }
    }
  }

  return false;
}

void PlanDeparser::deparse_table(THD *thd[[maybe_unused]], const TABLE *table) {
   assert(table);
   append_identifier(&m_statement, table->s->db.str, table->s->db.length);
   m_statement.append(".");
   append_identifier(&m_statement, table->s->table_name.str, table->s->table_name.length);
   if (table->alias &&
       table->alias[0] &&
       (table->s->table_name.length != strlen(table->alias) || 
       memcmp(table->s->table_name.str, table->alias, table->s->table_name.length))) {
     m_statement.append(STRING_WITH_LEN(" "));
     append_identifier(&m_statement, table->alias, strlen(table->alias));
   } 
}

bool PlanDeparser::deparse_join_type(THD *thd[[maybe_unused]], JoinType join_type) {
  switch (join_type) {
    case JoinType::INNER:
      m_statement.append(STRING_WITH_LEN(" inner join "));
      break;
    case JoinType::OUTER:
      m_statement.append(STRING_WITH_LEN(" left join "));
      break;
    case JoinType::ANTI:
      // not support yet
      assert(false);
      break;
    case JoinType::SEMI:
      // not support yet
      assert(false);
      break;
    default:
      assert(false);
      return true;
  }
  return false;
}

bool PlanDeparser::deparse_hashjoin_type(THD *thd[[maybe_unused]],
                                         RelationalExpression::Type join_type) {
  switch (join_type) {
    case RelationalExpression::INNER_JOIN:
      m_statement.append(STRING_WITH_LEN(" inner join "));
      break;
    case RelationalExpression::STRAIGHT_INNER_JOIN:
      m_statement.append(STRING_WITH_LEN(" straight_join "));
      break;
    case RelationalExpression::LEFT_JOIN:
      m_statement.append(STRING_WITH_LEN(" left join "));
      break;
    case RelationalExpression::ANTIJOIN:
      // not support yet
      assert(false);
      break;
    case RelationalExpression::SEMIJOIN:
      // not support yet
      assert(false);
      break;
    default:
      assert(false);
      return true;
  }
  // print deparse sql
  LogPQDebug("collector deparse sql:%s to worker", m_statement.c_ptr_safe());

  return false;
}

/*
  deparse equi condition of hash join (actually only inner hash join)
  non-equi condition has transform to child filter access path so do not deal here
*/
void PlanDeparser::deparse_hashjoin_equi_condition(THD *thd[[maybe_unused]], const JoinPredicate *predicate, String &cond) {
  for (Item_func_eq *condition : predicate->expr->equijoin_conditions) {
    if (!cond.is_empty()) {
      cond.append(STRING_WITH_LEN(" and "));
    }
    constexpr enum_query_type qt_deparse =
              enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
    condition->print(thd, &cond, qt_deparse);
  }
}

void PlanDeparser::deparse_pushdown_condition(THD *thd[[maybe_unused]], TABLE *table, String &cond) {
  if (table->file->pushed_cond) {
    if (!cond.is_empty()) {
      cond.append(STRING_WITH_LEN(" and "));
    }   
    constexpr enum_query_type qt_deparse =
              enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
    table->file->pushed_cond->print(thd, &cond, qt_deparse);
  }
}

/* deparse the group by using tmp table */
bool PlanDeparser::deparse_tmp_group(THD *thd[[maybe_unused]], TABLE *table) {
  // deparse where before group
  deparse_where(thd);
  m_statement.append(STRING_WITH_LEN(" group by "));
  // use hash to group
  if (table->hash_field != nullptr) {
    // FIXME: if no group return my_error
    if (table->group) {
      deparse_tmp_hash_group(table->group);
      return false;
    } else {
      my_error(ER_PARALLEL_DEPARSE_ERROR, MYF(0), "can't find group by fields");
      return true;
    }
  } 
  assert(table->s && table->s->key_info);
  KEY *key = table->s->key_info;
  KEY_PART_INFO *key_part = key->key_part;
  KEY_PART_INFO *key_part_end = key->key_part + key->user_defined_key_parts;
  bool first = true;
  for (; key_part < key_part_end; key_part++) {
    if (!first) {
      m_statement.append(STRING_WITH_LEN(", ")); 
    } else {
      first = false;
    }
    Field *field = key_part->field;
    deparse_field(field);
  }

  return false;
}

/* deparse the group by index */
void PlanDeparser::deparse_group(THD *thd[[maybe_unused]]) {
  List<Cached_item> &list = m_query_block->join->group_fields;
  if (list.elements <= 0) return;
  // print where before group
  deparse_where(thd);
  m_statement.append(STRING_WITH_LEN(" group by "));
  List_iterator<Cached_item> li(list);
  bool first = true;
  Cached_item *it = nullptr;
  while ((it = li++)) {
    if (!first) {
      m_statement.append(STRING_WITH_LEN(", "));
    } else {
      first = false;
    }
    constexpr enum_query_type qt_deparse =
    enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
    it->get_item()->print(thd, &m_statement, qt_deparse); 
  } 
}

void PlanDeparser::deparse_sort(THD *thd[[maybe_unused]], Filesort *filesort) {
  // append where before order
  deparse_where(thd);
  m_statement.append(STRING_WITH_LEN(" order by "));
  bool first = true;
  for (unsigned i = 0; i < filesort->sort_order_length(); ++i) {
    if (first) {
      first = false;
    } else {
      m_statement.append(STRING_WITH_LEN(", "));
    }

    const st_sort_field *order = &filesort->sortorder[i];
    // field is from tmp table append the name directlly
    // it has added the `` already
    // eg: sum(`t`.`b`) in Item_field->field_name
    if (order->item->type() == Item::FIELD_ITEM &&
        ((Item_field*)order->item)->orig_db_name() == nullptr &&
        ((Item_field*)order->item)->orig_table_name() == nullptr &&
        ((Item_field*)order->item)->field_name != nullptr) {
      m_statement.append(((Item_field*)order->item)->field_name);
    } else {
      constexpr enum_query_type qt_deparse =
      enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
      order->item->print(thd, &m_statement, qt_deparse);
    }
    if (order->reverse) {
      m_statement.append(STRING_WITH_LEN(" desc"));
    }
  }
}

void PlanDeparser::deparse_limit(THD *thd[[maybe_unused]], const AccessPath *path) {
  char buf[256];
  if (path->limit_offset().offset == 0) {
    snprintf(buf, sizeof(buf), " limit %llu",
             path->limit_offset().limit);
  } else if (path->limit_offset().limit == HA_POS_ERROR) {
    snprintf(buf, sizeof(buf), " offset %llu",
             path->limit_offset().offset);
  } else {
    snprintf(buf, sizeof(buf), " limit %llu offset %llu",
             path->limit_offset().limit - path->limit_offset().offset,
             path->limit_offset().offset);
  }
  m_statement.append(buf);
}

// deparse the key.fields = ref.fields
void PlanDeparser::deparse_ref(const TABLE_REF &ref, const KEY *key,
                               bool include_nulls, const char *table_name,
                               size_t table_name_len, String &ret) {
  const uchar *key_buff = ref.key_buff;
  for (unsigned key_part_idx = 0; key_part_idx < ref.key_parts;
       ++key_part_idx) {
    if (key_part_idx != 0) {
      ret.append(STRING_WITH_LEN(" and ("));
    } else {
      ret.append(STRING_WITH_LEN("("));
    }
    const Field *field = key->key_part[key_part_idx].field;
    if (field->is_field_for_functional_index()) {
      // Do not print out the column name if the column represents a functional
      // index. Instead, print out the indexed expression.
      ret.append(ItemToString(field->gcol_info->expr_item).c_str());
    } else {
      assert(!field->is_hidden_by_system());
      ret.append(STRING_WITH_LEN("`"));
      ret.append(table_name, table_name_len);
      ret.append(STRING_WITH_LEN("`.`"));
      ret.append(field->field_name);
      ret.append(STRING_WITH_LEN("`"));
    }
    ret.append(STRING_WITH_LEN(" = "));
    constexpr enum_query_type qt_deparse =
            enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
    ref.items[key_part_idx]->print(current_thd, &ret, qt_deparse);

    // If we have ref_or_null access, find out if this keypart is the one that
    // is -or-NULL (there's always only a single one).
    if (include_nulls && key_buff == ref.null_ref_key) {
      ret.append(STRING_WITH_LEN(" or `"));
      ret.append(table_name, table_name_len);
      ret.append(STRING_WITH_LEN("`.`"));
      ret.append(field->field_name);
      ret.append(STRING_WITH_LEN("` is null"));
    }
    ret.append(STRING_WITH_LEN(")"));
    key_buff += key->key_part[key_part_idx].store_length;
  }
}

bool PlanDeparser::deparse_access_path(THD *thd, const AccessPath *path,
                                       String &cond, String &join_cond, bool group_sort) {
  switch (path->type) {
    case AccessPath::TABLE_SCAN: {
      TABLE *table = path->table_scan().table;
      // pushed_condition
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::INDEX_SCAN: {
      TABLE *table = path->index_scan().table;
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::REF: {
      TABLE *table = path->ref().table;
      // avoid duplicate condition both in pushdown and ref
      if (table->file->pushed_cond) {
        deparse_pushdown_condition(thd, table, cond);
      } else {
        const KEY *key = &table->key_info[path->ref().ref->key];
        deparse_ref(*path->ref().ref, key, /*include_nulls=*/false,
                    table->s->table_name.str, table->s->table_name.length, join_cond);
      }
      deparse_table(thd, table);
      break;
    }
    case AccessPath::REF_OR_NULL: {
      TABLE *table = path->ref_or_null().table;
      // avoid duplicate
      if (table->file->pushed_cond) {
        deparse_pushdown_condition(thd, table, cond);
      } else {
        const KEY *key = &table->key_info[path->ref_or_null().ref->key];
        deparse_ref(*path->ref_or_null().ref, key, /*include_nulls=*/true,
                    table->s->table_name.str, table->s->table_name.length, join_cond);
      }
      deparse_table(thd, table);
      break;
    }
    case AccessPath::EQ_REF: {
      TABLE *table = path->eq_ref().table;
      const KEY *key = &table->key_info[path->eq_ref().ref->key];
      deparse_ref(*path->eq_ref().ref, key, /*include_nulls=*/false,
                  table->s->table_name.str, table->s->table_name.length, join_cond);
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::PUSHED_JOIN_REF: {
      TABLE *table = path->pushed_join_ref().table;
      const KEY *key = &table->key_info[path->pushed_join_ref().ref->key];
      deparse_ref(*path->pushed_join_ref().ref, key, false,
                  table->s->table_name.str, table->s->table_name.length, join_cond);
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::FULL_TEXT_SEARCH: {
      TABLE *table = path->full_text_search().table;
      const KEY *key = &table->key_info[path->full_text_search().ref->key];
      deparse_ref(*path->full_text_search().ref, key, false,
                  table->s->table_name.str, table->s->table_name.length, join_cond);
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::CONST_TABLE: {
      TABLE *table = path->const_table().table;
      // if condition not pushdown use the condition of ref 
      if (!table->file->pushed_cond) {
        const KEY *key = &table->key_info[path->const_table().ref->key];
        deparse_ref(*path->const_table().ref, key, false,
                  table->s->table_name.str, table->s->table_name.length, cond);
      } else {
         deparse_pushdown_condition(thd, table, cond);
      }
      deparse_table(thd, table);
      break;
    }
    case AccessPath::MRR: {
      TABLE *table = path->mrr().table;
      const KEY *key = &table->key_info[path->mrr().ref->key];
      deparse_ref(*path->mrr().ref, key, false,
                  table->s->table_name.str, table->s->table_name.length, join_cond);
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::FOLLOW_TAIL: {
      TABLE *table = path->follow_tail().table;
      deparse_table(thd, table); 
      break;
    }
    case AccessPath::INDEX_RANGE_SCAN: {
      TABLE *table = path->index_range_scan().table;
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::DYNAMIC_INDEX_RANGE_SCAN: {
      TABLE *table = path->dynamic_index_range_scan().table;
      deparse_pushdown_condition(thd, table, cond);
      deparse_table(thd, table);
      break;
    }
    case AccessPath::PARALLEL_COLLECTOR_SCAN: {
      assert(false); // partial plan don't have this path
      break;
    }
    case AccessPath::TABLE_VALUE_CONSTRUCTOR:
    case AccessPath::FAKE_SINGLE_ROW:
    case AccessPath::ZERO_ROWS:
    case AccessPath::ZERO_ROWS_AGGREGATED:
    case AccessPath::MATERIALIZED_TABLE_FUNCTION:
      assert(false);
      break;
    case AccessPath::UNQUALIFIED_COUNT:
      if (path->unqualified_count().tables) {
        auto *table = path->unqualified_count().tables->at(0).first;
        deparse_table(thd, table);
      } else {
        assert(false);
      }
      break;
    case AccessPath::NESTED_LOOP_JOIN:
      deparse_access_path(thd, path->nested_loop_join().outer, cond, join_cond);
      if (deparse_join_type(thd, path->nested_loop_join().join_type)) return true;
      deparse_access_path(thd, path->nested_loop_join().inner, cond, join_cond);
      // JOIN condition comes from FILTER node or base scan (e.g. ref scan)
      // under NESTED_LOOP_JOIN on access path tree. Maybe here we should
      // deparse these conditions as a predicate condition instead of JOIN
      // condition.
      if (!join_cond.is_empty()) {
        m_statement.append(STRING_WITH_LEN(" on "));
        m_statement.append(join_cond.ptr(), join_cond.length());
        // clear ?
        join_cond.length(0);
      } else {
        m_statement.append(STRING_WITH_LEN(" on true "));
      }
      break;
    case AccessPath::BKA_JOIN:
      deparse_access_path(thd, path->bka_join().outer, cond, join_cond);
      if (deparse_join_type(thd, path->bka_join().join_type)) return true;
      deparse_access_path(thd, path->bka_join().inner, cond, join_cond);
      // See comments in case NESTED_LOOP_JOIN above.
      if (!join_cond.is_empty()) {
        m_statement.append(STRING_WITH_LEN(" on "));
        m_statement.append(join_cond.ptr(), join_cond.length());
        // clear ?
        join_cond.length(0);
      } else {
        m_statement.append(STRING_WITH_LEN(" on true "));
      }
      break;
    case AccessPath::HASH_JOIN: {
      deparse_access_path(thd, path->hash_join().outer, cond, join_cond);
      const JoinPredicate *predicate = path->hash_join().join_predicate;
      RelationalExpression::Type type = path->hash_join().rewrite_semi_to_inner
                                            ? RelationalExpression::INNER_JOIN
                                            : predicate->expr->type;
      if (deparse_hashjoin_type(thd, type)) return true; 
      deparse_access_path(thd, path->hash_join().inner, cond, join_cond);
      if (!join_cond.is_empty()) {
        m_statement.append(STRING_WITH_LEN(" on "));
        m_statement.append(join_cond.ptr(), join_cond.length());
        // clear ?
        join_cond.length(0);
      } else {
        m_statement.append(STRING_WITH_LEN(" on true "));
      }

      deparse_hashjoin_equi_condition(thd, predicate, cond); 
      break;
    }
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      // support later
      assert(false);
      break;
    case AccessPath::FILTER: {
      if (!cond.is_empty()) {
        cond.append(STRING_WITH_LEN(" and "));
      }
      constexpr enum_query_type qt_deparse =
      enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
      path->filter().condition->print(thd, &cond, qt_deparse);
      deparse_access_path(thd, path->filter().child, cond, join_cond);
      break;
    }
    case AccessPath::SORT: {
      deparse_access_path(thd, path->sort().child, cond, join_cond);
      if (group_sort) {
        deparse_group(thd);
      }
      deparse_sort(thd, path->sort().filesort);
      break;
    }
    case AccessPath::AGGREGATE: {
      JOIN *join = m_query_block->join;
      bool is_group_sort = false;
      if (join->grouped || join->group_optimized_away) {
        if (path->aggregate().child->type == AccessPath::SORT) {
          // push the group by before the order by
          // eg select * from t group by a;
          // accesspath: aggregate:
          //                  sort:
          // avoid syntex error need write group first
          //    -> select * from t group by a order by a
          is_group_sort = true;  
        } 
      }
      deparse_access_path(thd, path->aggregate().child, cond, join_cond, is_group_sort); 
      if (is_group_sort) {
        is_group_sort = false;
      } else { 
        deparse_group(thd);
      }
      break;
    }
    case AccessPath::TEMPTABLE_AGGREGATE: {
      deparse_access_path(thd, path->temptable_aggregate().subquery_path, cond, join_cond);
      TABLE *table = path->temptable_aggregate().table;
      // deparse group not using index, group by tmp table index
      if (deparse_tmp_group(thd, table)) return true;
      break;
    }
    case AccessPath::LIMIT_OFFSET: {
      deparse_access_path(thd, path->limit_offset().child, cond, join_cond);
      // append where if needed
      deparse_where(thd);
      deparse_limit(thd, path);
      break;
    }
    case AccessPath::STREAM:
      deparse_access_path(thd, path->stream().child, cond, join_cond);
      break;
    case AccessPath::MATERIALIZE:
      if (deparse_materialize_access_path(thd, path, cond, join_cond)) return true;
      break;
    case AccessPath::MATERIALIZE_INFORMATION_SCHEMA_TABLE:
      deparse_access_path(thd, path->materialize_information_schema_table().table_path, cond, join_cond);
      break;
    case AccessPath::APPEND:
      assert(false);
      break;
    case AccessPath::WINDOW: {
      // not support yet
      assert(false);
      break;
    }
    case AccessPath::WEEDOUT: {
      // not support yet
      assert(false);
      break;
    }
    case AccessPath::REMOVE_DUPLICATES: {
      deparse_access_path(thd, path->remove_duplicates().child, cond, join_cond);
      break;
    }
    case AccessPath::REMOVE_DUPLICATES_ON_INDEX:
      deparse_access_path(thd, path->remove_duplicates_on_index().child, cond, join_cond);
      break;
    case AccessPath::ALTERNATIVE: {
      const TABLE *table =
          path->alternative().table_scan_path->table_scan().table;
      deparse_table(thd, table);
      break;
    }
    case AccessPath::CACHE_INVALIDATOR:
      deparse_access_path(thd, path->cache_invalidator().child, cond, join_cond);
      break;
  }

  return false;
}

/*
 * create the from clause
 */
bool PlanDeparser::deparse_from(THD *thd) {
  auto *join = m_query_block->join;
  m_statement.append(STRING_WITH_LEN(" from "));

  if (deparse_access_path(thd, join->root_access_path(), m_pull_out_cond, m_join_cond)) return true; 

  return false;
}

void PlanDeparser::deparse_where(THD *thd[[maybe_unused]]) {
  if (!m_join_cond.is_empty() || !m_pull_out_cond.is_empty()) {
    m_statement.append(STRING_WITH_LEN(" where "));
    // if join condition not empty append it to where clause
    if (!m_join_cond.is_empty()) {
      m_statement.append(m_join_cond.ptr(), m_join_cond.length());
      m_join_cond.length(0); 
      if (!m_pull_out_cond.is_empty()) {
        m_statement.append(STRING_WITH_LEN(" and "));
        m_statement.append(m_pull_out_cond.ptr(), m_pull_out_cond.length());
        m_pull_out_cond.length(0);
      }
    } else {
      assert(!m_pull_out_cond.is_empty());
      m_statement.append(m_pull_out_cond.ptr(), m_pull_out_cond.length());
      m_pull_out_cond.length(0);
    }
  }
}

bool PlanDeparser::deparse() {
  THD *thd = m_query_block->join->thd;
  if (deparse_select_items(thd) || deparse_from(thd))
    return true;
  // print the pulled out condition if it's not print yet
  deparse_where(thd);

  return false;
}

}  // namespace pq
