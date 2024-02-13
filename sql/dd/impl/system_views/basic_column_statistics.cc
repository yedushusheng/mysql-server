#include "sql/dd/impl/system_views/basic_column_statistics.h"

namespace dd {
namespace system_views {

const Basic_column_statistics &Basic_column_statistics::instance() {
  static Basic_column_statistics *s_instance = new Basic_column_statistics();
  return *s_instance;
}

Basic_column_statistics::Basic_column_statistics() {
  m_target_def.set_view_name(view_name());
  m_target_def.add_field(FIELD_CATALOG_ID, "CATALOG_ID", "CATALOG_ID");
  m_target_def.add_field(FIELD_NAME, "NAME", "NAME");
  m_target_def.add_field(FIELD_SCHEMA_NAME, "SCHEMA_NAME", "SCHEMA_NAME");
  m_target_def.add_field(FIELD_TABLE_NAME, "TABLE_NAME", "TABLE_NAME");
  m_target_def.add_field(FIELD_COLUMN_NAME, "COLUMN_NAME", "COLUMN_NAME");
  m_target_def.add_field(FIELD_LAST_ANALYZED, "LAST_ANALYZED", "LAST_ANALYZED");
  m_target_def.add_field(FIELD_DISTINCT_CNT, "DISTINCT_CNT", "DISTINCT_CNT");
  m_target_def.add_field(FIELD_NULL_CNT, "NULL_CNT", "NULL_CNT");
  m_target_def.add_field(FIELD_MAX_VALUE, "MAX_VALUE", "MAX_VALUE");
  m_target_def.add_field(FIELD_MIN_VALUE, "MIN_VALUE", "MIN_VALUE");
  m_target_def.add_field(FIELD_AVG_LEN, "AVG_LEN",
                         "AVG_LEN");
  m_target_def.add_field(FIELD_DISTINCT_CNT_SYNOPSIS, "DISTINCT_CNT_SYNOPSIS",
                         "DISTINCT_CNT_SYNOPSIS");
  m_target_def.add_field(FIELD_DISTINCT_CNT_SYNOPSIS_SIZE,
                         "DISTINCT_CNT_SYNOPSIS_SIZE",
                         "DISTINCT_CNT_SYNOPSIS_SIZE");
  m_target_def.add_field(FIELD_SPARE1, "SPARE1", "SPARE1");
  m_target_def.add_field(FIELD_SPARE2, "SPARE2", "SPARE2");
  m_target_def.add_field(FIELD_SPARE3, "SPARE3", "SPARE3");
  m_target_def.add_field(FIELD_SPARE4, "SPARE4", "SPARE4");
  m_target_def.add_field(FIELD_SPARE5, "SPARE5", "SPARE5");
  m_target_def.add_field(FIELD_SPARE6, "SPARE6", "SPARE6");

  m_target_def.add_from("mysql.basic_column_statistics");

  m_target_def.add_where(
      "CAN_ACCESS_COLUMN(SCHEMA_NAME, TABLE_NAME, COLUMN_NAME)");
}

}  // namespace system_views
}  // namespace dd
