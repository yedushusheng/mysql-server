#ifndef DD_SYSTEM_VIEWS__STATISTICS_COLLECTOR_JOBS_INCLUDED
#define DD_SYSTEM_VIEWS__STATISTICS_COLLECTOR_JOBS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace dd {
namespace system_views {

/*
  The class representing INFORMATION_SCHEMA.STATISTICS_COLLECTOR_JOBS system
  view definition.
*/
class Statistics_collector_jobs
    : public System_view_impl<System_view_select_definition_impl> {
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

  Statistics_collector_jobs();

  static const Statistics_collector_jobs &instance();

  static const String_type &view_name() {
    static String_type s_view_name("STATISTICS_COLLECTOR_JOBS");
    return s_view_name;
  }

  virtual const String_type &name() const override {
    return Statistics_collector_jobs::view_name();
  }

 private:
  Statistics_collector_jobs(const Statistics_collector_jobs &) = delete;
  Statistics_collector_jobs &operator=(const Statistics_collector_jobs &) =
      delete;
};

}  // namespace system_views
}  // namespace dd

#endif  // DD_SYSTEM_VIEWS__STATISTICS_COLLECTOR_JOBS_INCLUDED