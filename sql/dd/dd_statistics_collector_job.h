#ifndef DD_STATISTICS_COLLECTOR_JOB_INCLUDED
#define DD_STATISTICS_COLLECTOR_JOB_INCLUDED

#include <sys/types.h>
#include <memory>  // std:unique_ptr
#include <string>
#include <vector>
#include "sql/dd/impl/raw/object_keys.h"
#include "sql/dd/types/statistics_collector_job.h"
#include "sql/handler.h"  // legacy_db_type

namespace dd {

bool remove_statistics_collector_job_record(THD *thd,
                                            dd::Statistics_collector_job *obj,
                                            bool new_trx);

bool store_statistics_collector_job_record(THD *thd,
                                           dd::Statistics_collector_job *obj,
                                           bool new_trx);

bool find_statistics_collector_job_record(
    THD *thd, dd::Statistics_collector_job::Id_key key,
    std::unique_ptr<dd::Statistics_collector_job> &obj, bool for_update);

bool get_all_statistics_collector_jobs(
    THD *thd,
    std::vector<dd::Statistics_collector_job *> *statistics_collector_jobs);

}  // namespace dd

#endif  // DD_STATISTICS_COLLECTOR_JOB_INCLUDED