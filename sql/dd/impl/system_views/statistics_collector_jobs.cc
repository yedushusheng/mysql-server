#include "sql/dd/impl/system_views/statistics_collector_jobs.h"

namespace dd {
namespace system_views {

const Statistics_collector_jobs &Statistics_collector_jobs::instance() {
  static Statistics_collector_jobs *s_instance =
      new Statistics_collector_jobs();
  return *s_instance;
}

Statistics_collector_jobs::Statistics_collector_jobs() {
  m_target_def.set_view_name(view_name());
  m_target_def.add_field(FIELD_ID, "ID", "ID");
  m_target_def.add_field(FIELD_SCHEMA_NAME, "SCHEMA_NAME", "SCHEMA_NAME");
  m_target_def.add_field(FIELD_TABLE_NAME, "TABLE_NAME", "TABLE_NAME");
  m_target_def.add_field(
      FIELD_STATISTICS_COLLECTOR_STATUS, "STATISTICS_COLLECTOR_STATUS",
      "CONVERT_STATISTICS_COLLECTOR_STATUS(statistics_collector_status)");
  m_target_def.add_field(FIELD_START_TIMESTAMP, "START_TIMESTAMP",
                         "START_TIMESTAMP");
  m_target_def.add_field(FIELD_LAST_TIMESTAMP, "LAST_TIMESTAMP",
                         "LAST_TIMESTAMP");
  m_target_def.add_field(FIELD_STATISTICS_COLLECTOR_SQL,
                         "STATISTICS_COLLECTOR_SQL",
                         "STATISTICS_COLLECTOR_SQL");
  m_target_def.add_field(FIELD_RET_CODE, "RET_CODE", "RET_CODE");

  m_target_def.add_from("mysql.statistics_collector_jobs");
}

}  // namespace system_views
}  // namespace dd