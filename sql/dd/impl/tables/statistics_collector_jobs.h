#ifndef DD_TABLES__STATISTICS_COLLECTOR_JOBS_INCLUDED
#define DD_TABLES__STATISTICS_COLLECTOR_JOBS_INCLUDED

#include <new>
#include <string>

#include "sql/dd/impl/raw/object_keys.h"  // Composite_char_key
#include "sql/dd/impl/types/entity_object_table_impl.h"  // Entity_object_table_impl
#include "sql/dd/types/statistics_collector_job.h"
#include "sql/dd/impl/types/statistics_collector_job_impl.h"

namespace dd {

class Raw_record;

namespace tables {

///////////////////////////////////////////////////////////////////////////

class Statistics_collector_jobs_base {
 public:
  enum enum_fields {
    FIELD_ID,
    FIELD_SCHEMA_NAME,
    FIELD_TABLE_NAME,
    FIELD_STATISTICS_COLLECTOR_STATUS,
    FIELD_START_TIMESTAMP,
    FIELD_LAST_TIMESTAMP,
    FIELD_STATISTICS_COLLECTOR_SQL,
    FIELD_RET_CODE
  };

  void add_target_def_fields(Object_table_definition_impl* target_def);

  virtual void add_id_field(Object_table_definition_impl* target_def) = 0;

  virtual void add_indexes(Object_table_definition_impl* target_def) = 0;
};

class Statistics_collector_jobs : virtual public Entity_object_table_impl, public Statistics_collector_jobs_base {
 public:
  Statistics_collector_jobs();

  static const Statistics_collector_jobs &instance();

  Statistics_collector_job *create_entity_object(const Raw_record &) const override {
    return new (std::nothrow) Statistics_collector_job_impl();
  }

  static Statistics_collector_job::Name_key *create_name_key(const String_type &schema_name,
                                            const String_type &table_name);

  void add_id_field(Object_table_definition_impl* target_def) override;

  void add_indexes(Object_table_definition_impl* target_def) override;

 private:
  enum enum_indexes {
    INDEX_PK_ID = static_cast<uint>(Object_table_impl::Common_index::PK_ID),
    INDEX_K_SCHEMA_TABLE_NAME /* NOT UK */
  };

 private:
  Statistics_collector_jobs(const Statistics_collector_jobs&) = delete;
  Statistics_collector_jobs& operator=(const Statistics_collector_jobs&) = delete;
};

} // namespace tables
} // namespace dd

#endif // DD_TABLES__STATISTICS_COLLECTOR_JOBS_INCLUDED