#include "sql/parallel_query/plan_deparser.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/item_sum.h"

namespace pq {
PlanDeparser::PlanDeparser(THD *thd, Query_block *query_block)
    : m_deparse_fields(thd->mem_root), m_query_block(query_block) {}

bool PlanDeparser::deparse(THD *thd) {
  auto *join = m_query_block->join;
  // transform fields, e.g. avg, min, max
  bool has_group_by = !join->group_list.empty();

  m_count_appended = !has_group_by && *join->sum_funcs != nullptr;

  uint cur_item_index = 0;
  for (auto *item : m_query_block->visible_fields()) {
    DeField *defield;
    Item_sum *item_sum = item->type() == Item::SUM_FUNC_ITEM
                             ? down_cast<Item_sum *>(item)
                             : nullptr;
    if (!item_sum || item_sum->sum_func() != Item_sum::AVG_FUNC) {
      if (!(defield = new (thd->mem_root) DeField(item, cur_item_index++)) ||
          m_deparse_fields.push_back(defield))
        return true;
      continue;
    }
    auto avgindex = cur_item_index++;
    if (!(defield =
              new (thd->mem_root) DeField(item, avgindex, cur_item_index++)) ||
        m_deparse_fields.push_back(defield))
      return true;
  }

  if (m_count_appended)
    m_deparse_fields.push_back(new (thd->mem_root) DeField(cur_item_index++));

  // TODO: Remove no_parallel hint
  m_statement.append(STRING_WITH_LEN("select /*+ no_parallel */ "));
  bool first = true;
  constexpr enum_query_type qt_deparse =
      enum_query_type(QT_NO_DEFAULT_DB | QT_PRINT_FOR_PLAN_DEPARSE);
  for (auto *af : m_deparse_fields) {
    if (first)
      first = false;
    else
      m_statement.append(',');
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
    m_statement.append(')');
  }
  m_statement.append(STRING_WITH_LEN(" from "));
  for (TABLE_LIST *tl = m_query_block->leaf_tables; tl != nullptr;
       tl = tl->next_leaf) {
    tl->print(thd, &m_statement, qt_deparse);
  }

  if (join->where_cond) {
    m_statement.append(STRING_WITH_LEN(" where "));
    join->where_cond->print_item_w_name(thd, &m_statement, qt_deparse);
  }

  if (has_group_by) {
    m_statement.append(STRING_WITH_LEN(" group by "));
    m_query_block->print_order(thd, &m_statement, join->group_list.order,
                               qt_deparse);
  }
  if (!join->order.empty()) {
    m_statement.append(STRING_WITH_LEN(" order by "));
    m_query_block->print_order(thd, &m_statement, join->order.order,
                               qt_deparse);
  }

  return false;
}
}  // namespace pq
