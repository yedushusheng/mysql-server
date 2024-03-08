#include "sql/dd/impl/tables/statistics_collector_jobs.h"

#include "sql/dd/impl/raw/object_keys.h"       // Composite_char_key
#include "sql/dd/impl/tables/dd_properties.h"  // TARGET_DD_VERSION
#include "sql/dd/impl/types/object_table_definition_impl.h"

namespace dd {
namespace tables {

///////////////////////////////////////////////////////////////////////////

void Statistics_collector_jobs_base::add_target_def_fields(
    Object_table_definition_impl *target_def) {
  add_id_field(target_def);
  target_def->add_field(FIELD_SCHEMA_NAME, "FIELD_SCHEMA_NAME",
                        "schema_name VARCHAR(64) NOT NULL");
  target_def->add_field(FIELD_TABLE_NAME, "FIELD_TABLE_NAME",
                        "table_name VARCHAR(64) NOT NULL");
  target_def->add_field(FIELD_STATISTICS_COLLECTOR_STATUS,
                        "FIELD_STATISTICS_COLLECTOR_STATUS",
                        "statistics_collector_status int not NULL");
  target_def->add_field(FIELD_START_TIMESTAMP, "FIELD_START_TIMESTAMP",
                        "start_timestamp timestamp not NULL");
  target_def->add_field(FIELD_LAST_TIMESTAMP, "FIELD_LAST_TIMESTAMP",
                        "last_timestamp timestamp not NULL");
  target_def->add_field(FIELD_STATISTICS_COLLECTOR_SQL,
                        "FIELD_STATISTICS_COLLECTOR_SQL",
                        "statistics_collector_sql longtext");
  target_def->add_field(FIELD_RET_CODE, "FIELD_RET_CODE",
                        "ret_code int not NULL");

  add_indexes(target_def);
}

///////////////////////////////////////////////////////////////////////////

Statistics_collector_jobs::Statistics_collector_jobs() {
  m_target_def.set_table_name("statistics_collector_jobs");

  add_target_def_fields(&m_target_def);
}

void Statistics_collector_jobs::add_id_field(
    Object_table_definition_impl *target_def) {
  target_def->add_field(FIELD_ID, "FIELD_ID",
                        "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT");
}

void Statistics_collector_jobs::add_indexes(
    Object_table_definition_impl *target_def) {
  target_def->add_index(INDEX_PK_ID, "INDEX_PK_ID", "PRIMARY KEY (id)");
  target_def->add_index(INDEX_K_SCHEMA_TABLE_NAME, "INDEX_K_SCHEMA_TABLE_NAME",
                        "KEY(schema_name, table_name)");
}

const Statistics_collector_jobs &Statistics_collector_jobs::instance() {
  static Statistics_collector_jobs *s_instance =
      new Statistics_collector_jobs();
  return *s_instance;
}

Statistics_collector_job::Name_key *Statistics_collector_jobs::create_name_key(
    const String_type &schema_name, const String_type &table_name) {
  return new (std::nothrow)
      Composite_char_key(INDEX_K_SCHEMA_TABLE_NAME, FIELD_SCHEMA_NAME,
                         schema_name, FIELD_TABLE_NAME, table_name);
}

}  // namespace tables
}  // namespace dd