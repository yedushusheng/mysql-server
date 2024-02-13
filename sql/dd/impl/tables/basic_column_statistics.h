#ifndef DD_TABLES__BASIC_COLUMN_STATISTICS_INCLUDED
#define DD_TABLES__BASIC_COLUMN_STATISTICS_INCLUDED

#include <new>

#include "sql/dd/impl/raw/object_keys.h"
#include "sql/dd/impl/types/basic_column_statistic_impl.h"
#include "sql/dd/impl/types/entity_object_table_impl.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/basic_column_statistic.h"

namespace dd {
class Object_key;
class Global_name_key;
class Name_key;
class Raw_record;

namespace tables {

///////////////////////////////////////////////////////////////////////////

class Basic_column_statistics : virtual public Entity_object_table_impl {
 public:
  Basic_column_statistics();

  static const Basic_column_statistics &instance();

  static const CHARSET_INFO *name_collation();

  enum enum_fields {
    FIELD_CATALOG_ID,
    FIELD_NAME,
    FIELD_SCHEMA_NAME,
    FIELD_TABLE_NAME,
    FIELD_COLUMN_NAME,
    FIELD_LAST_ANALYZED,
    FIELD_DISTINCT_CNT,
    FIELD_NULL_CNT,
    FIELD_MAX_VALUE,
    FIELD_MIN_VALUE,
    FIELD_AVG_LEN,
    FIELD_DISTINCT_CNT_SYNOPSIS,
    FIELD_DISTINCT_CNT_SYNOPSIS_SIZE,
    FIELD_SPARE1,
    FIELD_SPARE2,
    FIELD_SPARE3,
    FIELD_SPARE4,
    FIELD_SPARE5,
    FIELD_SPARE6,
    NUMBER_OF_FIELDS  // Always keep this entry at the end of the enum
  };

  enum enum_indexes {
    INDEX_UK_SCHEMA_TABLE_COLUMN,
    INDEX_UK_NAME = static_cast<uint>(Common_index::UK_NAME)
  };

  Basic_column_statistic *create_entity_object(
      const Raw_record &) const override;

  static bool update_object_key(Global_name_key *key,
                                const String_type &basic_column_statistic_name);

  static Basic_column_statistic::Name_key *create_object_key(
      const String_type &name);
  
  static Object_key *create_range_key_by_table_name(
      const String_type &schema_name, const String_type &table_name);      
};

}  // namespace tables
}  // namespace dd

#endif  // DD_TABLES__BASIC_COLUMN_STATISTICS_INCLUDED
