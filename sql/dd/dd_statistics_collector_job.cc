#include "sql/dd/dd_statistics_collector_job.h"
#include "sql/bootstrap.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"
#include "sql/dd/impl/cache/storage_adapter.h"
#include "sql/dd/impl/tables/statistics_collector_jobs.h"
#include "sql/dd/impl/transaction_impl.h"
#include "sql/dd/types/statistics_collector_job.h"
#include "sql/debug_sync.h"
#include "sql/sql_class.h"
#include "sql/tdsql/trans.h"
#include "sql/tdsql/trans_impl.h"
#include "sql/transaction.h"
#include "sql/tztime.h"

extern int32_t tdsql_trans_type;

namespace dd {

static int start_transaction(THD *thd) {
  int retval;
  if (!tdsql::get_or_create_tx(thd, retval)) {
    retval = TDSQL_EXIT_FAILURE;
  }

  return retval;
}

static bool check_statistics_collector_job_exists(
    THD *thd, dd::Statistics_collector_job *obj) {
  assert(thd && obj);

  bool for_update = false;
  // Pessimistic mode transaction.
  if (tdsql_trans_type == 0) {
    for_update = true;
  }

  std::unique_ptr<dd::Statistics_collector_job> tmp_obj;
  if (!dd::find_statistics_collector_job_record(thd, obj->id(), tmp_obj,
                                                for_update)) {
    if (!thd->is_error()) {
      my_error(ER_SQLENGINE_SYSTEM, MYF(0),
               "The statistics_collector job was deleted when the "
               "statistics_collector was running, please try again");
    }
    return true;
  }

  return false;
}

// Return false if success
bool store_statistics_collector_job(THD *thd,
                                    dd::Statistics_collector_job *obj) {
  assert(thd && obj);
  if (start_transaction(thd)) {
    return true;
  }

  if (check_statistics_collector_job_exists(thd, obj)) {
    return true;
  }

  return thd->dd_client()->store(obj);
}

// Return false if success
bool remove_statistics_collector_job(THD *thd,
                                     dd::Statistics_collector_job *obj) {
  assert(thd && obj);
  if (start_transaction(thd)) {
    return true;
  }

  return cache::Storage_adapter::drop(thd, obj);
}

bool store_statistics_collector_job_record(THD *thd,
                                           dd::Statistics_collector_job *obj,
                                           bool new_trx) {
  assert(thd != nullptr);

  if (new_trx) {
    Switch_attachable_rw_ctx ctx(thd);
    // Ignore kill flag while storing ddl record.
    DD_kill_immunizer kill_immunizer(thd);
    if (store_statistics_collector_job(thd, obj)) {
      trans_rollback_stmt(thd);
      trans_rollback(thd);
      return true;
    }

    return trans_commit_stmt(thd) || trans_commit(thd);
  }

  return store_statistics_collector_job(thd, obj);
}

bool find_statistics_collector_job_record(
    THD *thd, dd::Statistics_collector_job::Id_key key,
    std::unique_ptr<dd::Statistics_collector_job> &obj, bool for_update) {
  assert(thd != nullptr);

  const dd::Statistics_collector_job *new_obj = nullptr;
  tdsql::SwitchGetDDForUpdateClosure update_closure(thd, for_update);
  if (!cache::Storage_adapter::get(thd, key, ISO_READ_COMMITTED, false,
                                   &new_obj)) {
    if (new_obj == nullptr) {
      return false;
    }

    obj.reset(const_cast<dd::Statistics_collector_job *>(new_obj));
    return true;
  }

  assert(thd->is_system_thread() || thd->killed || thd->is_error());
  return false;
}

bool remove_statistics_collector_job_record(THD *thd,
                                            dd::Statistics_collector_job *obj,
                                            bool new_trx) {
  assert(thd != nullptr);

  if (new_trx) {
    Switch_attachable_rw_ctx ctx(thd);
    if (remove_statistics_collector_job(thd, obj)) {
      trans_rollback_stmt(thd);
      trans_rollback(thd);
      return true;
    }

    return trans_commit_stmt(thd) || trans_commit(thd);
  }

  return remove_statistics_collector_job(thd, obj);
}

// Scan all dd::Statistics_collector_job object from persistent storage.
bool get_all_statistics_collector_jobs(
    THD *thd,
    std::vector<dd::Statistics_collector_job *> *statistics_collector_jobs) {
  assert(thd);
  assert(statistics_collector_jobs);

  // Start a DD transaction to get the object.
  Transaction_ro trx(thd, ISO_READ_COMMITTED);
  trx.otx.register_tables<dd::Statistics_collector_job>();

  if (trx.otx.open_tables()) {
    assert(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  const Entity_object_table &table =
      Statistics_collector_job::DD_table::instance();
  // Get main object table.
  Raw_table *t = trx.otx.get_table(table.name());

  std::unique_ptr<Raw_record_set> rs;
  if (t->open_record_set(nullptr, rs)) {
    assert(thd->killed || thd->is_error());
    return true;
  }

  Raw_record *r = rs->current_record();
  while (r) {
    dd::Statistics_collector_job *object = nullptr;
    // Restore the object from the record.
    dd::Entity_object *new_object = NULL;
    if (table.restore_object_from_record(&trx.otx, *r, &new_object)) {
      assert(thd->is_system_thread() || thd->killed || thd->is_error());
      return true;
    }

    // Delete the new object if dynamic cast fails.
    if (new_object) {
      // Here, a failing dynamic cast is not a legitimate situation.
      // In production, we report an error.
      object = dynamic_cast<dd::Statistics_collector_job *>(new_object);
      if (object == nullptr) {
        delete new_object;
      } else {
        statistics_collector_jobs->push_back(object);
      }
    }

    if (rs->next(r)) {
      assert(thd->is_error() || thd->killed);
      return true;
    }
  }

  return false;
}

}  // namespace dd