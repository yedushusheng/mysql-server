#include "sql/dd/impl/types/statistics_collector_job_impl.h"  // Statistics_collector_job_impl

#include <ostream>
#include <string>

#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/dd/impl/raw/object_keys.h"
#include "sql/dd/impl/raw/raw_record.h"      // Raw_record
#include "sql/dd/impl/tables/statistics_collector_jobs.h"     // Statistics_collector_jobs
#include "sql/dd/impl/transaction_impl.h"    // Open_dictionary_tables_ctx

namespace dd {
class Object_key;
}  // namespace dd

using dd::tables::Statistics_collector_jobs;

namespace dd {

///////////////////////////////////////////////////////////////////////////
// Ddl_job implementation.
///////////////////////////////////////////////////////////////////////////

bool Statistics_collector_job_impl::validate() const {
  if (schema_name().empty() || table_name().empty()) {
    my_error(ER_INVALID_DD_OBJECT, MYF(0), DD_table::instance().name().c_str(),
             "schema name or table name not supplied.");
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Statistics_collector_job_impl::restore_attributes(const Raw_record &r) {
  restore_id(r, Statistics_collector_jobs::FIELD_ID);
  m_schema_name = r.read_str(Statistics_collector_jobs::FIELD_SCHEMA_NAME);
  m_table_name = r.read_str(Statistics_collector_jobs::FIELD_TABLE_NAME);

  m_statistics_collector_status = r.read_uint(Statistics_collector_jobs::FIELD_STATISTICS_COLLECTOR_STATUS);
  m_start_timestamp = r.read_uint(Statistics_collector_jobs::FIELD_START_TIMESTAMP);
  m_last_timestamp = r.read_uint(Statistics_collector_jobs::FIELD_LAST_TIMESTAMP);
  m_statistics_collector_sql = r.read_str(Statistics_collector_jobs::FIELD_STATISTICS_COLLECTOR_SQL);
  m_ret_code = r.read_uint(Statistics_collector_jobs::FIELD_RET_CODE);

  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Statistics_collector_job_impl::store_attributes(Raw_record *r) {
  return store_id(r, Statistics_collector_jobs::FIELD_ID) ||
         r->store(Statistics_collector_jobs::FIELD_SCHEMA_NAME, m_schema_name) ||
         r->store(Statistics_collector_jobs::FIELD_TABLE_NAME, m_table_name) ||
         r->store(Statistics_collector_jobs::FIELD_STATISTICS_COLLECTOR_STATUS, m_statistics_collector_status) ||
         r->store(Statistics_collector_jobs::FIELD_START_TIMESTAMP, m_start_timestamp) ||
         r->store(Statistics_collector_jobs::FIELD_LAST_TIMESTAMP, m_last_timestamp) ||
         r->store(Statistics_collector_jobs::FIELD_STATISTICS_COLLECTOR_SQL, m_statistics_collector_sql) ||
         r->store(Statistics_collector_jobs::FIELD_RET_CODE, m_ret_code);
}

///////////////////////////////////////////////////////////////////////////

void Statistics_collector_job_impl::debug_print(String_type &outb) const {
  dd::Stringstream_type ss;
  ss << "DDL JOB OBJECT: { "
     << "m_schema_name: " << m_schema_name << "; "
     << "m_table_name: " << m_table_name << "; "
     << "m_statistics_collector_status: " << m_statistics_collector_status << "; "
     << "m_start_timestamp: " << m_start_timestamp << "; "
     << "m_last_timestamp: " << m_last_timestamp << "; "
     << "m_statistics_collector_sql: " << m_statistics_collector_sql << "; "
     << "m_ret_code: " << m_ret_code;

  ss << " }";
  outb = ss.str();
}

///////////////////////////////////////////////////////////////////////////

String_type Statistics_collector_job_impl::basic_obj_info(const Raw_record &r) const {
  dd::Stringstream_type ss;
  ss << "[STATISTICS_COLLECTOR_JOB] id " << r.read_int(tables::Statistics_collector_jobs::FIELD_ID);
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////

const Object_table &Statistics_collector_job_impl::object_table() const {
  return DD_table::instance();
}

///////////////////////////////////////////////////////////////////////////

void Statistics_collector_job_impl::register_tables(Open_dictionary_tables_ctx *otx) {
  /**
    The requirement is that we should be able to update
    Statistics_colletor_jobs DD tables even when someone holds
    global read lock, when we execute ANALYZE TABLE.
  */
  otx->mark_ignore_global_read_lock();
  otx->add_table<Statistics_collector_jobs>();
}

Statistics_collector_job_impl* Statistics_collector_job_impl::clone() const {
  return new Statistics_collector_job_impl(*this);
}

Statistics_collector_job_impl::Statistics_collector_job_impl(const Statistics_collector_job_impl& src) 
  : Entity_object_impl(src),
    m_schema_name(src.m_schema_name),
    m_table_name(src.m_table_name),
    m_statistics_collector_status(src.m_statistics_collector_status),
    m_start_timestamp(src.m_start_timestamp),
    m_last_timestamp(src.m_last_timestamp),
    m_statistics_collector_sql(src.m_statistics_collector_sql),
    m_ret_code(src.m_ret_code) {}

///////////////////////////////////////////////////////////////////////////

}  // namespace dd