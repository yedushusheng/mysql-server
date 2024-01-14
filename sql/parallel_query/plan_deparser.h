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
  DeField(type typ, Item *f, uint index, uint aux_index)
      : m_type(typ),
        item(f),
        source_field_index(index),
        source_field_aux_index(aux_index) {}

  bool save(char *data);
  type m_type;
  Item *item;
  Field *to;
  uint source_field_index;
  uint source_field_aux_index;
};

class PlanDeparser {
 public:
  PlanDeparser(Query_block *query_block);
  THD *thd();
  bool deparse();
  bool IsDeparsed() { return m_statement.length() > 0; }
  String *statement() { return &m_statement; }
  bool CountAppended() const { return m_count_appended; }
  DeField *DeparseField(uint i) { return m_deparse_fields[i]; }

 private:
  Query_block *m_query_block;
  mem_root_deque<DeField *> m_deparse_fields;
  bool m_count_appended{false};
  String m_statement;
};
}  // namespace pq
#endif
