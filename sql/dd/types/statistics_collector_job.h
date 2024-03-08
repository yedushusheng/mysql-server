 
#ifndef DD__STATISTICS_COLLECTOR_JOB_INCLUDED
#define DD__STATISTICS_COLLECTOR_JOB_INCLUDED

#include "sql/dd/types/entity_object.h"        // dd::Entity_object
#include "sql/dd/types/entity_object_table.h"  // Entity_object_table

namespace dd {

///////////////////////////////////////////////////////////////////////////

class Primary_id_key;
class Composite_char_key;
class Statistics_collector_job_impl;

namespace tables {
class Statistics_collector_jobs;
}

///////////////////////////////////////////////////////////////////////////

class Statistics_collector_job : virtual public Entity_object {
 public:
  typedef Statistics_collector_job_impl Impl;
  typedef tables::Statistics_collector_jobs DD_table;
  typedef Primary_id_key Id_key;
  typedef Composite_char_key Name_key;

 public:
  virtual ~Statistics_collector_job() {}

  /////////////////////////////////////////////////////////////////////////
  // id.
  /////////////////////////////////////////////////////////////////////////

  virtual void reset_id() = 0;

  /////////////////////////////////////////////////////////////////////////
  // schema name.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &schema_name() const = 0;
  virtual void set_schema_name(const String_type &schema_name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // table name.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &table_name() const = 0;
  virtual void set_table_name(const String_type &table_name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // statistic collector status.
  /////////////////////////////////////////////////////////////////////////

  virtual int statistics_collector_status() const = 0;
  virtual void set_statistics_collector_status(
      int statistics_collector_status) = 0;

  /////////////////////////////////////////////////////////////////////////
  // start_timestamp.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong start_timestamp() const = 0;
  virtual void set_start_timestamp(ulonglong start_timestamp) = 0;

  /////////////////////////////////////////////////////////////////////////
  // last_timestamp.
  /////////////////////////////////////////////////////////////////////////

  virtual ulonglong last_timestamp() const = 0;
  virtual void set_last_timestamp(ulonglong last_timestamp) = 0;

  /////////////////////////////////////////////////////////////////////////
  // statistics collector sql.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &statistics_collector_sql() const = 0;
  virtual void set_statistics_collector_sql(
      const String_type &statistics_collector_sql) = 0;

  /////////////////////////////////////////////////////////////////////////
  // ret code.
  /////////////////////////////////////////////////////////////////////////

  virtual int ret_code() const = 0;
  virtual void set_ret_code(int ret_code) = 0;

  /**
    Allocate a new object graph and invoke the copy contructor for
    each object.

    @return pointer to dynamically allocated copy
  */
  virtual Statistics_collector_job *clone() const = 0;

 public:
  virtual class Entity_object_impl *impl() override = 0;
  virtual const class Entity_object_impl *impl() const override = 0;
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
#endif  // DD__STATISTICS_COLLECTOR_JOB_INCLUDED