#ifndef DD_SYSTEM_VIEWS__BASIC_COLUMN_STATISTICS_INCLUDED
#define DD_SYSTEM_VIEWS__BASIC_COLUMN_STATISTICS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace dd {
namespace system_views {

/*
  The class representing INFORMATION_SCHEMA.BASIC_COLUMN_STATISTICS system view
  definition.
  This class used to collect basic column statistics info,
  these column statistics used by RBO.
*/
class Basic_column_statistics
    : public System_view_impl<System_view_select_definition_impl> {
 public:
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

  Basic_column_statistics();

  static const Basic_column_statistics &instance();

  static const String_type &view_name() {
    static String_type s_view_name("BASIC_COLUMN_STATISTICS");
    return s_view_name;
  }

  const String_type &name() const override {
    return Basic_column_statistics::view_name();
  }
};

}  // namespace system_views
}  // namespace dd

#endif  // DD_SYSTEM_VIEWS__BASIC_COLUMN_STATISTICS_INCLUDED
