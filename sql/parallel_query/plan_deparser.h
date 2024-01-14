#ifndef PARALLEL_QUERY_PLAN_DEPARSER_H
#define PARALLEL_QUERY_PLAN_DEPARSER_H
#include <climits>
#include "mem_root_deque.h"
#include "sql_string.h"

class Query_block;
class Field;
class THD;
class Item;

namespace pq {
#define INVALID_FIELD_INDEX UINT_MAX
class DeField {
 public:
  enum type { NORMAL_ITEM, COUNT, AVG };
  DeField(Item *f, uint index)
      : m_type(NORMAL_ITEM), item(f), source_field_index(index) {}
  DeField(Item *f, uint index, uint aux_index)
      : m_type(AVG),
        item(f),
        source_field_index(index),
        source_field_aux_index(aux_index) {}
  DeField(uint index) : m_type(COUNT), source_field_index(index) {}

  bool save(char *data);
  type m_type;
  Item *item{nullptr};
  Field *to;
  uint source_field_index;
  uint source_field_aux_index{INVALID_FIELD_INDEX};
};

class PlanDeparser {
 public:
  PlanDeparser(THD *thd, Query_block *query_block);
  bool deparse(THD *thd);
  String *statement() { return &m_statement; }
  bool CountAppended() const { return m_count_appended; }
  DeField *DeparseField(uint i) { return m_deparse_fields[i]; }

 private:
  mem_root_deque<DeField *> m_deparse_fields;
  bool m_count_appended{false};
  Query_block *m_query_block;
  String m_statement;
};
}  // namespace pq
#endif
