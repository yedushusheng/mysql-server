#ifndef DD__STATISTICS_COLLECTOR_JOB_IMPL_INCLUDED
#define DD__STATISTICS_COLLECTOR_JOB_IMPL_INCLUDED

#include <memory>
#include <new>
#include <string>

#include "my_inttypes.h"
#include "sql/dd/impl/raw/raw_record.h"
#include "sql/dd/impl/types/entity_object_impl.h"  // dd::Entity_object_impl
#include "sql/dd/object_id.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/entity_object_table.h"
#include "sql/dd/types/statistics_collector_job.h"  // dd::Statistics_collector_job


namespace dd {

///////////////////////////////////////////////////////////////////////////

class Charset;
class Object_key;
class Object_table;
class Open_dictionary_tables_ctx;
class Raw_table;
class Transaction;
class Weak_object;
class Object_table;

///////////////////////////////////////////////////////////////////////////
const char constexpr* CollectorStatusStr[] = {
  "FAIL",
  "SUCCESS",
  "EXECUTING"
};

class Statistics_collector_job_impl : public Entity_object_impl, public Statistics_collector_job {
 public:
  Statistics_collector_job_impl() {}

  /*
    Indicate statistics collector job status, given definition:
    STATISTICS_COLLECTOR_FAIL, collect and update statistics info failed.
    STATISTICS_COLLECTOR_SUCC, collect and update statistics info success.
    STATISTICS_COLLECTOR_EXECUTING, collect and update statistics info is executing.
  */
  enum Statistics_collector_status_flag {
    STATISTICS_COLLECTOR_FAIL = 0,
    STATISTICS_COLLECTOR_SUCC = 1 << 0,
    STATISTICS_COLLECTOR_EXECUTING = 1 << 1
  };

 public:
  void debug_print(String_type &outb) const override;

  const Object_table &object_table() const override;

  bool validate() const override;

  bool restore_attributes(const Raw_record &r) override;
  bool store_attributes(Raw_record *r) override;

  String_type basic_obj_info(const Raw_record &r) const override;

 public:
  static void register_tables(Open_dictionary_tables_ctx *otx);

  /////////////////////////////////////////////////////////////////////////
  // schema name.
  /////////////////////////////////////////////////////////////////////////

  const String_type &schema_name() const override { return m_schema_name; }

  void set_schema_name(const String_type &schema_name) override {
    m_schema_name = schema_name;
  }

  /////////////////////////////////////////////////////////////////////////
  // table name.
  /////////////////////////////////////////////////////////////////////////

  const String_type &table_name() const override { return m_table_name; }

  void set_table_name(const String_type &table_name) override {
    m_table_name = table_name;
  }

  /////////////////////////////////////////////////////////////////////////
  // statistics_collector_status.
  /////////////////////////////////////////////////////////////////////////

  int statistics_collector_status() const override { return m_statistics_collector_status; }
  void set_statistics_collector_status(int statistics_collector_status) override {
    m_statistics_collector_status = statistics_collector_status;
  }

  /////////////////////////////////////////////////////////////////////////
  // start_timestamp.
  /////////////////////////////////////////////////////////////////////////

  ulonglong start_timestamp() const override { return m_start_timestamp; }
  void set_start_timestamp(ulonglong start_timestamp) override {
    m_start_timestamp = start_timestamp;
  }

  /////////////////////////////////////////////////////////////////////////
  // last_timestamp.
  /////////////////////////////////////////////////////////////////////////

  ulonglong last_timestamp() const override { return m_last_timestamp; }
  void set_last_timestamp(ulonglong last_timestamp) override {
    m_last_timestamp = last_timestamp;
  }

  /////////////////////////////////////////////////////////////////////////
  // statistics_collector_sql.
  /////////////////////////////////////////////////////////////////////////

  const String_type &statistics_collector_sql() const override { return m_statistics_collector_sql; }
  void set_statistics_collector_sql(const String_type &statistics_collector_sql) override {
    m_statistics_collector_sql = statistics_collector_sql;
  }

  /////////////////////////////////////////////////////////////////////////
  // ret code.
  /////////////////////////////////////////////////////////////////////////

  int ret_code() const override { return m_ret_code; }
  void set_ret_code(int ret_code) override {
    m_ret_code = ret_code;
  }

  Statistics_collector_job_impl *clone() const override;

 public:

  // Fix "inherits ... via dominance" warnings
  Entity_object_impl *impl() override { return Entity_object_impl::impl(); }
  const Entity_object_impl *impl() const override {
    return Entity_object_impl::impl();
  }
  Object_id id() const override { return Entity_object_impl::id(); }
  bool is_persistent() const override {
    return Entity_object_impl::is_persistent();
  }
  const String_type &name() const override { return Entity_object_impl::name(); }
  void set_name(const String_type &name) override {
    Entity_object_impl::set_name(name);
  }
  void reset_id() override { set_id(INVALID_OBJECT_ID); }

 private:
  // Fields
  String_type m_schema_name;
  String_type m_table_name;

  ulonglong m_statistics_collector_status{Statistics_collector_status_flag::STATISTICS_COLLECTOR_EXECUTING};
  ulonglong m_start_timestamp{0};
  ulonglong m_last_timestamp{0};
  String_type m_statistics_collector_sql;
  ulonglong m_ret_code;

  explicit Statistics_collector_job_impl(const Statistics_collector_job_impl& src);
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__STATISTICS_COLLECTOR_JOB_IMPL_INCLUDED