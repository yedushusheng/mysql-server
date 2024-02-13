#include "sql/dd/impl/tables/basic_column_statistics.h"
#include "sql/dd/impl/types/basic_column_statistic_impl.h"

#include "sql/dd/impl/raw/object_keys.h"
#include "sql/dd/impl/tables/dd_properties.h"  // TARGET_DD_VERSION
#include "sql/dd/impl/types/object_table_definition_impl.h"

namespace dd {
namespace tables {

///////////////////////////////////////////////////////////////////////////

const Basic_column_statistics &Basic_column_statistics::instance() {
  static Basic_column_statistics *s_instance = new Basic_column_statistics();
  return *s_instance;
}

///////////////////////////////////////////////////////////////////////////

const CHARSET_INFO *Basic_column_statistics::name_collation() {
  return Object_table_definition_impl::name_collation();
}

///////////////////////////////////////////////////////////////////////////

Basic_column_statistics::Basic_column_statistics() {
  m_target_def.set_table_name("basic_column_statistics");

  m_target_def.add_field(FIELD_CATALOG_ID, "FIELD_CATALOG_ID",
                         "catalog_id BIGINT UNSIGNED NOT NULL");
  m_target_def.add_field(FIELD_NAME, "FIELD_NAME",
                         "name VARCHAR(64) NOT NULL COLLATE " +
                             String_type(name_collation()->name));
  m_target_def.add_field(FIELD_SCHEMA_NAME, "FIELD_SCHEMA_NAME",
                         "schema_name VARCHAR(64) NOT NULL");
  m_target_def.add_field(FIELD_TABLE_NAME, "FIELD_TABLE_NAME",
                         "table_name VARCHAR(64) NOT NULL");
  m_target_def.add_field(FIELD_COLUMN_NAME, "FIELD_COLUMN_NAME",
                         "column_name VARCHAR(64) NOT NULL");
  m_target_def.add_field(FIELD_LAST_ANALYZED, "FIELD_LAST_ANALYZED",
                         "last_analyzed TIMESTAMP NOT NULL");
  m_target_def.add_field(FIELD_DISTINCT_CNT, "FIELD_DISTINCT_CNT",
                         "distinct_cnt BIGINT(20) UNSIGNED DEFAULT NULL");
  m_target_def.add_field(FIELD_NULL_CNT, "FIELD_NULL_CNT",
                         "null_cnt BIGINT(20) UNSIGNED DEFAULT NULL");
  // set index max length 3072
  m_target_def.add_field(FIELD_MAX_VALUE, "FIELD_MAX_VALUE",
                         "max_value VARCHAR(3072) DEFAULT NULL");
  m_target_def.add_field(FIELD_MIN_VALUE, "FIELD_MIN_VALUE",
                         "min_value VARCHAR(3072) DEFAULT NULL");
  m_target_def.add_field(FIELD_AVG_LEN, "FIELD_AVG_LEN",
                         "avg_len BIGINT(20) UNSIGNED DEFAULT NULL");
  m_target_def.add_field(FIELD_DISTINCT_CNT_SYNOPSIS,
                         "FIELD_DISTINCT_CNT_SYNOPSIS",
                         "distinct_cnt_synopsis VARCHAR(64) DEFAULT NULL");
  m_target_def.add_field(
      FIELD_DISTINCT_CNT_SYNOPSIS_SIZE, "FIELD_DISTINCT_CNT_SYNOPSIS_SIZE",
      "distinct_cnt_synopsis_size BIGINT(20) UNSIGNED DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE1, "FIELD_SPARE1",
                         "spare1 BIGINT(20) DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE2, "FIELD_SPARE2",
                         "spare2 BIGINT(20) DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE3, "FIELD_SPARE3",
                         "spare3 BIGINT(20) DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE4, "FIELD_SPARE4",
                         "spare4 VARCHAR(64) DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE5, "FIELD_SPARE5",
                         "spare5 VARCHAR(64) DEFAULT NULL");
  m_target_def.add_field(FIELD_SPARE6, "FIELD_SPARE6",
                         "spare6 VARCHAR(64) DEFAULT NULL");

  m_target_def.add_index(INDEX_UK_SCHEMA_TABLE_COLUMN,
                         "INDEX_UK_SCHEMA_TABLE_COLUMN",
                         "UNIQUE KEY (schema_name, table_name, column_name)");
  m_target_def.add_index(INDEX_UK_NAME, "INDEX_UK_NAME", "UNIQUE KEY(name)");
}

///////////////////////////////////////////////////////////////////////////

bool Basic_column_statistics::update_object_key(
    Global_name_key *key, const String_type &basic_column_statistic_name) {
  key->update(FIELD_NAME, basic_column_statistic_name, name_collation());
  return false;
}

///////////////////////////////////////////////////////////////////////////

Basic_column_statistic *Basic_column_statistics::create_entity_object(
    const Raw_record &) const {
  return new (std::nothrow) Basic_column_statistic_impl();
}

///////////////////////////////////////////////////////////////////////////

Basic_column_statistic::Name_key *Basic_column_statistics::create_object_key(
    const String_type &name) {
  return new (std::nothrow) Global_name_key(
      Basic_column_statistics::INDEX_UK_NAME, name, name_collation());
}

///////////////////////////////////////////////////////////////////////////

Object_key *Basic_column_statistics::create_range_key_by_table_name(
    const String_type &schema_name, const String_type &table_name) {
  return new (std::nothrow) Index_stat_range_key(
      Basic_column_statistics::INDEX_UK_SCHEMA_TABLE_COLUMN, FIELD_SCHEMA_NAME,
      schema_name, FIELD_TABLE_NAME, table_name);
}

}  // namespace tables
}  // namespace dd
