/*
   Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation  // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

/* The C++ file's header */
#include "./ha_rocksdb.h"

/* C++ standard header files */
#include <algorithm>
#include <limits>
#include <map>
#include <queue>
#include <regex>
#include <set>
#include <string>
#include <vector>

/* MySQL includes */
#include "my_bit.h"
#include "my_stacktrace.h"
#include "my_sys.h"
#include "mysql/thread_pool_priv.h"
#include "mysys_err.h"
#include "sql/dd/dd_onlineddl.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h" // DD_bootstrap_ctx
#include "sql/dd/info_schema/table_stats.h"
#include "sql/debug_sync.h"
#include "sql/error_handler.h"
#include "sql/field.h"
#include "sql/item.h"       // Item
#include "sql/item_cmpfunc.h" // Item_func_like etc.
#include "sql/item_func.h"  // Item_func
#include "sql/key.h"
#include "sql/mysqld.h"
#include "sql/opt_range.h"
#include "sql/sql_audit.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_partition.h"
#include "sql/sql_select.h"
#include "sql/sql_table.h"
#include "sql/sql_thd_internal_api.h"
#include "sql/sql_update.h"  //compare_records
#include "sql/table.h"
#include "sql/tdsql/parallel/parallel_defs.h"
#include "sql/tdsql/ddl/correlate_option.h"
#include "include/scope_guard.h"

// Both MySQL and RocksDB define the same constant. To avoid compilation errors
// till we make the fix in RocksDB, we'll temporary undefine it here.
#undef CACHE_LINE_SIZE


/* MyRocks includes */
#include "./ha_rocksdb_proto.h"
#include "./ha_rockspart.h"
#include "./ib_ut0counter.h"  //UT_ARRAY_SIZE
#include "./rdb_datadic.h"

#include <sys/eventfd.h>

#include "sql/abstract_query_plan.h"

// TDSQL include
#include "push_down.h"
#include "sql/tdsql/common.h"
#include "sql/tdsql/ddl/ddl_common.h"
#include "sql/tdsql/ddl/ddl_trans.h"
#include "sql/tdsql/ddl/ddl_worker.h"
#include "sql/tdsql/errno.h"
#include "sql/tdsql/query.h"
#include "sql/tdsql/estimated_info_thread.h"
#include "sql/tdsql/optimizer/optimizer_cond_pushdown.h"
#include "sql/tdsql/parallel/iterator.h"
#include "sql/tdsql/parallel/service/data_fillback.h"
#include "sql/tdsql/parallel/service/row_count.h"
#include "sql/tdsql/route/region_mgr.h"
#include "sql/tdsql/rpc_cntl/row_count_cntl.h"
#include "sql/tdsql/rpc_cntl/rpc_batch_cntl.h"
#include "sql/tdsql/rpc_cntl/rpc_mc_cntl.h"
#include "sql/tdsql/rpc_cntl/rpc_range_stats_cntl.h"
#include "sql/tdsql/rpc_cntl/rpc_tds_cntl.h"
#include "sql/tdsql/rpc_connections.h"
#include "sql/tdsql/table_type.h"  // IsSystemTable
#include "sql/tdsql/trans.h"
#include "storage/rocksdb/tdsql/parallel_query.h"
#include "storage/rocksdb/tdsql/storage_format.h"

#include "sql/tdsql/background_thread.h"
#include "sql/tdsql/background.h"
#include "sql/strfunc.h"
#include "sql/tztime.h"

uint tdsql_max_projection_pct;
uint tdsql_save_update_time_interval;
uint tdsql_handler_record_buffer_size;
extern bool tdsql_optimize_pk_point_select;

namespace native_part {
extern void part_name(char *out_buf, const char *path,
                      const char *parent_elem_name, const char *elem_name);
}  // namespace native_part

PSI_memory_key key_memory_myrocks_estimated_info;

namespace myrocks {

static st_global_stats global_stats;
static st_export_stats export_stats;
static st_io_stall_stats io_stall_stats;

const std::string DEFAULT_CF_NAME("default");
const std::string DEFAULT_SYSTEM_CF_NAME("__system__");
const std::string PER_INDEX_CF_NAME("$per_index_cf");
static std::vector<GL_INDEX_ID> rdb_indexes_to_recalc;

typedef std::vector<std::pair<std::string, std::string>> StartEndKeys;

using namespace tdsql;

#define rdb_handler_memory_key PSI_NOT_INSTRUMENTED

#define CONVERT_TO_TDSQL_SLICE(rocksdb_slice) (tdsql::Slice(rocksdb_slice.data(),rocksdb_slice.size()))
#define CONVERT_TO_ROCKSDB_SLICE(tdsql_slice) (rocksdb::Slice(tdsql_slice.data(),tdsql_slice.size()))

void Rdb_table_handler::update(uint64_t mtcache_count, uint64_t mtcache_size, uint64_t time) {
  std::lock_guard<tdsql::Mutex> guard(mutex_);
  m_mtcache_count = mtcache_count;
  m_mtcache_size = mtcache_size;
  m_last_update_time_for_check_ = time;
  m_last_update_time_.store(time);
  ResetUpdateCount();
}

void Rdb_table_handler::update_table_stats(const dd::Table_stat *stat) {
  if (!stat) {
    return ;
  }
  update(stat->table_rows(), stat->data_length(), my_micro_time());
}

bool Rdb_table_handler::cache_is_expired_no_lock(uint64_t cachetime, uint64_t cur_time) {
  if (unlikely(!cachetime)) {
    return true;
  }

  if (cur_time > m_last_update_time_for_check_ + cachetime + time_rand_fixed_) {
    return true;
  }
  return false;
}

std::string Rdb_table_handler::ToString() {
  ardb::Buffer buff;
  buff.Printf( "Rdb_table_handler [table=%s] [%s.%s] [m_ref_count=%d] [update_time=%llu] [update_count_=%lu] [time_rand_fixed_=%lu] [update_rate_=%.4f] [data_rows=%lu] [datasize=%lu]",
              m_table_name.c_str(),
              m_real_db_name.c_str(), m_real_table_name.c_str(),
              m_ref_count.load(), GetReadableUpdateTime(), update_count_,
              time_rand_fixed_,
              GetUpdateRate(),
              m_mtcache_count,
              m_mtcache_size);
  return buff.AsString();
}


std::string Rdb_table_handler::ConStructAnalyzeReloadSql() {
  ardb::Buffer buff;
  buff.Printf("analyze table `%s`.`%s` reload ", m_real_db_name.c_str(),
              m_real_table_name.c_str());
  return buff.AsString();
}

bool IsTmpTableForLogService(TABLE* table) {
  if (table && table->s && table->s->tmp_table==INTERNAL_TMP_TABLE
      && table->in_use && table->in_use->log_service_thread) {
    return true;
  }
  return false;
}

std::string Rdb_table_handler::ConStructInsertSql() {
  time_t now = time(NULL);
  MYSQL_TIME time;
  my_tz_SYSTEM->gmt_sec_to_TIME(&time, now);
  ulonglong update_time = TIME_to_ulonglong_datetime(time);

  ardb::Buffer buff;

  std::lock_guard<tdsql::Mutex> guard(mutex_);

  buff.Printf("insert into mysql.table_stats(schema_name, table_name, table_rows, avg_row_length, data_length, max_data_length, index_length, data_free, auto_increment, update_time, cached_time) "
                  "values('%s', '%s', %lu, %lu, %lu, %lu, %lu, %lu, %lu, %llu, %llu);",
      m_real_db_name.c_str(), m_real_table_name.c_str(),
      m_mtcache_count,
      (uint64_t)(m_mtcache_count == 0 ? 0 : m_mtcache_size/m_mtcache_count),
      m_mtcache_size,
      (uint64_t)(0), (uint64_t)(0), (uint64_t)(0), (uint64_t)(0),
      update_time,
      update_time);

  return buff.AsString();
}

std::string Rdb_table_handler::ConStructUpdateSql() {
  time_t now = time(NULL);
  MYSQL_TIME time;
  my_tz_SYSTEM->gmt_sec_to_TIME(&time, now);
  ulonglong update_time = TIME_to_ulonglong_datetime(time);

  ardb::Buffer buff;

  std::lock_guard<tdsql::Mutex> guard(mutex_);

  buff.Printf("update mysql.table_stats set table_rows=%lu, data_length=%lu, avg_row_length=%lu, cached_time=%llu "
              " where schema_name='%s' and table_name='%s' and (cached_time < %llu  or update_time is null);",
              m_mtcache_count,
              m_mtcache_size,
              (uint64_t)(m_mtcache_count == 0 ? 0 : m_mtcache_size/m_mtcache_count),
              update_time,
              m_real_db_name.c_str(),
              m_real_table_name.c_str(),
              update_time);

  return buff.AsString();
}


/**
  Updates row counters based on the table type and operation type.
*/
void ha_rocksdb::update_row_stats(const operation_type &type) {
  assert(type < ROWS_MAX);

  // Find if we are modifying system databases.
  if (table->s && m_tbl_def->m_is_mysql_system_table)
    global_stats.system_rows[type].inc();
  else
    global_stats.rows[type].inc();
}

void ha_rocksdb::update_table_stats(const dd::Table_stat *stat) {
  if (!m_table_handler) {
    return ;
  }

  m_table_handler->update_table_stats(stat);
}


static handler *rocksdb_create_handler(my_core::handlerton *hton,
                                       my_core::TABLE_SHARE *table_arg,
                                       bool partitioned,
                                       my_core::MEM_ROOT *mem_root);

///////////////////////////////////////////////////////////
// Globals
///////////////////////////////////////////////////////////
handlerton *rocksdb_hton;

Rdb_ddl_manager ddl_manager;
Rdb_hton_init_state hton_init_state;

class DdlManagerLockClosure {
 public:
  DdlManagerLockClosure(bool wrlock) {
    if (wrlock) {
      mysql_rwlock_wrlock(&ddl_manager.m_rwlock);
    } else {
      mysql_rwlock_rdlock(&ddl_manager.m_rwlock);
    }
  }

  virtual ~DdlManagerLockClosure() {
    mysql_rwlock_unlock(&ddl_manager.m_rwlock);
  }
};

/**
  MyRocks background thread control
  N.B. This is besides RocksDB's own background threads
       (@see rocksdb::CancelAllBackgroundWork())
*/

// List of table names (using regex) that are exceptions to the strict
// collation check requirement.

static const char *rdb_get_error_messages(int error);

///////////////////////////////////////////////////////////
// Hash map: table name => open table handler
///////////////////////////////////////////////////////////

namespace  // anonymous namespace = not visible outside this source file
{

struct Rdb_open_tables_map {
  /* Hash table used to track the handlers of open tables */
  // m_hash's Rdb_table_handler don't own refcount
  std::unordered_map<std::string, Rdb_table_handler *> m_hash;
  /* The mutex used to protect the hash table */
  mutable mysql_mutex_t m_mutex;

  Rdb_table_handler *get_table_handler(const char *const table_name, bool &is_create);
  void release_table_handler(Rdb_table_handler *table_handler);

  std::vector<std::string> get_table_names(void) const;
};

}  // anonymous namespace

static Rdb_open_tables_map rdb_open_tables;

static int
rocksdb_check_bulk_load(THD *const thd,
                        struct SYS_VAR *var MY_ATTRIBUTE((__unused__)),
                        void *save, struct st_mysql_value *value);

static int rocksdb_check_bulk_load_allow_unsorted(
    THD *const thd, struct SYS_VAR *var MY_ATTRIBUTE((__unused__)), void *save,
    struct st_mysql_value *value);

//////////////////////////////////////////////////////////////////////////////
// Options definitions
//////////////////////////////////////////////////////////////////////////////
static constexpr ulong RDB_DEFAULT_BULK_LOAD_SIZE = 1000;
static constexpr ulong RDB_MAX_BULK_LOAD_SIZE = 1024 * 1024 * 1024;
static const constexpr ulong RDB_DEFAULT_BLOCK_SIZE = 0x4000;  // 16K
static constexpr uint64_t RDB_DEFAULT_FORCE_COMPUTE_MEMTABLE_STATS_CACHETIME =
    60 * 1000 * 1000;
static uint32_t rocksdb_debug_optimizer_n_rows = 0;

static bool rocksdb_force_compute_memtable_stats = true;
ulonglong rocksdb_force_compute_memtable_stats_cachetime =
    RDB_DEFAULT_FORCE_COMPUTE_MEMTABLE_STATS_CACHETIME;
static bool rocksdb_debug_optimizer_no_zero_cardinality = true;
#if defined(ROCKSDB_INCLUDE_VALIDATE_TABLES) && ROCKSDB_INCLUDE_VALIDATE_TABLES
static uint32_t rocksdb_validate_tables = 1;
#endif  // defined(ROCKSDB_INCLUDE_VALIDATE_TABLES) &&
        // ROCKSDB_INCLUDE_VALIDATE_TABLES
static char *rocksdb_datadir = nullptr;
static bool rocksdb_enable_bulk_load_api = true;
static bool rpl_skip_tx_api_var = false;
/* TDSQL: There are many large keys in dd tables. */
static bool rocksdb_large_prefix = true;

static MYSQL_THDVAR_BOOL(
    bulk_load, PLUGIN_VAR_RQCMDARG,
    "Use bulk-load mode for inserts. This disables "
    "unique_checks and enables rocksdb_commit_in_the_middle.",
    rocksdb_check_bulk_load, nullptr, false);

static MYSQL_THDVAR_UINT(records_in_range, PLUGIN_VAR_RQCMDARG,
                         "Used to override the result of records_in_range(). "
                         "Set to a positive number to override",
                         nullptr, nullptr, 0,
                         /* min */ 0, /* max */ INT_MAX, 0);
static MYSQL_THDVAR_UINT(force_index_records_in_range, PLUGIN_VAR_RQCMDARG,
                         "Used to override the result of records_in_range() "
                         "when FORCE INDEX is used.",
                         nullptr, nullptr, 0,
                         /* min */ 0, /* max */ INT_MAX, 0);

static MYSQL_THDVAR_BOOL(
    lock_scanned_rows, PLUGIN_VAR_RQCMDARG,
    "Take and hold locks on rows that are scanned but not updated", nullptr,
    nullptr, false);


static MYSQL_THDVAR_BOOL(bulk_load_allow_sk, PLUGIN_VAR_RQCMDARG,
                         "Allow bulk loading of sk keys during bulk-load. "
                         "Can be changed only when bulk load is disabled.",
                         /* Intentionally reuse unsorted's check function */
                         rocksdb_check_bulk_load_allow_unsorted, nullptr,
                         false);

static MYSQL_THDVAR_BOOL(bulk_load_allow_unsorted, PLUGIN_VAR_RQCMDARG,
                         "Allow unsorted input during bulk-load. "
                         "Can be changed only when bulk load is disabled.",
                         rocksdb_check_bulk_load_allow_unsorted, nullptr,
                         false);

static MYSQL_SYSVAR_BOOL(enable_bulk_load_api, rocksdb_enable_bulk_load_api,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Enables using SstFileWriter for bulk loading",
                         nullptr, nullptr, rocksdb_enable_bulk_load_api);

static MYSQL_THDVAR_ULONG(bulk_load_size, PLUGIN_VAR_RQCMDARG,
                          "Max #records in a batch for bulk-load mode", nullptr,
                          nullptr,
                          /*default*/ RDB_DEFAULT_BULK_LOAD_SIZE,
                          /*min*/ 1,
                          /*max*/ RDB_MAX_BULK_LOAD_SIZE, 0);


static MYSQL_SYSVAR_ULONGLONG(
    force_compute_memtable_stats_cachetime,
    rocksdb_force_compute_memtable_stats_cachetime, PLUGIN_VAR_RQCMDARG,
    "Time in usecs to cache memtable estimates", nullptr, nullptr,
    /* default */ RDB_DEFAULT_FORCE_COMPUTE_MEMTABLE_STATS_CACHETIME,
    /* min */ 0, /* max */ ULLONG_MAX, 0);

static struct SYS_VAR *rocksdb_system_variables[] = {
    MYSQL_SYSVAR(lock_scanned_rows),
    MYSQL_SYSVAR(records_in_range),
    MYSQL_SYSVAR(force_index_records_in_range),
    MYSQL_SYSVAR(bulk_load),
    MYSQL_SYSVAR(bulk_load_allow_sk),
    MYSQL_SYSVAR(bulk_load_allow_unsorted),
    MYSQL_SYSVAR(bulk_load_size),
    MYSQL_SYSVAR(enable_bulk_load_api),
    MYSQL_SYSVAR(force_compute_memtable_stats_cachetime),
    nullptr};

Bulk_load_closure::Bulk_load_closure(THD* thd)
    : m_thd(thd), m_default_size(RDB_DEFAULT_BULK_LOAD_SIZE * 3) {
  assert(m_thd);
  /* Backup old variables. */
  m_bulk_load = THDVAR(m_thd, bulk_load);
  m_bulk_load_allow_sk = THDVAR(m_thd, bulk_load_allow_sk);
  m_bulk_load_size = THDVAR(m_thd, bulk_load_size);
  m_tdsql_disable_trans_conflict_detect =
      m_thd->variables.tdsql_disable_trans_conflict_detect;

  THDVAR_SET(m_thd, bulk_load, &m_true_lvalue);
  THDVAR_SET(m_thd, bulk_load_allow_sk, &m_true_lvalue);
  THDVAR_SET(m_thd, bulk_load_size, &m_default_size);
  /*
    Why disable tdsql_disable_trans_conflict_detect here ?
    See issue: https://git.woa.com/tdsql3.0/TDStore/issues/325

    TODO: Maybe change another access mode for alter copy in R090.
  */
  m_thd->variables.tdsql_disable_trans_conflict_detect = false;
}

Bulk_load_closure::~Bulk_load_closure() {
  THDVAR_SET(m_thd, bulk_load, &m_bulk_load);
  THDVAR_SET(m_thd, bulk_load_allow_sk, &m_bulk_load_allow_sk);
  THDVAR_SET(m_thd, bulk_load_size, &m_bulk_load_size);
  m_thd->variables.tdsql_disable_trans_conflict_detect =
      m_tdsql_disable_trans_conflict_detect;
}

static int rocksdb_close_connection(handlerton *const, THD *const thd) {
  tdsql::Transaction *tx = tdsql::get_tx_from_thd(thd);
  if (tx != nullptr) {
    /*
    int rc = tx->finish_bulk_load(false);
    if (rc != 0) {
      LogPluginErrMsg(ERROR_LEVEL, 0,
                      "Error %d finalizing last SST file while disconnecting",
                      rc);
    }
    */
    tdsql::release_trans_in_thd(thd);
  }
  return HA_EXIT_SUCCESS;
}

/*
 * Serializes an xid to a string so that it can
 * be used as a rocksdb transaction name
 */
MY_ATTRIBUTE((unused)) static std::string rdb_xid_to_string(const XID &) {
//  assert(src.get_gtrid_length() >= 0 &&
//              src.get_gtrid_length() <= MAXGTRIDSIZE);
//  assert(src.get_bqual_length() >= 0 &&
//              src.get_bqual_length() <= MAXBQUALSIZE);
/* TDSQL: this function is currently not used. */
  std::string buf;
//  buf.reserve(RDB_XIDHDR_LEN + src.get_gtrid_length() + src.get_bqual_length());

  /*
   * expand formatID to fill 8 bytes if it doesn't already
   * then reinterpret bit pattern as unsigned and store in network order
   */
//  uchar fidbuf[RDB_FORMATID_SZ];
//  int64 signed_fid8 = src.get_format_id();
//  const uint64 raw_fid8 = *reinterpret_cast<uint64 *>(&signed_fid8);
//  rdb_netbuf_store_uint64(fidbuf, raw_fid8);
//  buf.append(reinterpret_cast<const char *>(fidbuf), RDB_FORMATID_SZ);

//  buf.push_back(src.get_gtrid_length());
//  buf.push_back(src.get_bqual_length());
//  buf.append(src.get_data(),
//             (src.get_gtrid_length()) + (src.get_bqual_length()));
  return buf;
}

/**
  Called by hton->flush_logs after MySQL group commit prepares a set of
  transactions.
*/
static bool rocksdb_flush_wal(handlerton *const hton MY_ATTRIBUTE((__unused__)),
                              bool) {
  DBUG_ENTER("rocksdb_flush_wal");
//  assert(rdb != nullptr);

  /**
    If !binlog_group_flush, we got invoked by FLUSH LOGS or similar.
    Else, we got invoked by binlog group commit during flush stage.
  */

//  if (binlog_group_flush &&
//      rocksdb_flush_log_at_trx_commit == FLUSH_LOG_NEVER) {
    /**
      rocksdb_flush_log_at_trx_commit=0
      (write and sync based on timer in Rdb_background_thread).
      Do not flush the redo log during binlog group commit.
    */
//    DBUG_RETURN(false);
//  }

//  if (!binlog_group_flush || !rocksdb_db_options->allow_mmap_writes ||
//      rocksdb_flush_log_at_trx_commit != FLUSH_LOG_NEVER) {
    /**
      Sync the WAL if we are in FLUSH LOGS, or if
      rocksdb_flush_log_at_trx_commit=1
      (write and sync at each commit).
    */
//    rocksdb_wal_group_syncs++;
//    const rocksdb::Status s =
//        rdb->FlushWAL(rocksdb_flush_log_at_trx_commit == FLUSH_LOG_SYNC);

//    if (!s.ok()) {
//      rdb_log_status_error(s);
//      DBUG_RETURN(true);
//    }
//  }
/** TDSQL: This function is called to flush wal of rocksdb. We do not have that in SQL Engine anymore,
 *  so that we just leave this function empty. But this function is call when executing create table.*/
  DBUG_RETURN(false);
}

/**
  For a slave, prepare() updates the slave_gtid_info table which tracks the
  replication progress.
*/
static int rocksdb_prepare(handlerton *const, THD *const thd,
                           bool) {
/**
 * TDSQL: This function is called within binlog prepare. Not sure if we should call tx->prepare here when 2PC
 * is implemented. Temporary leave it empty.
 */
  tdsql::Transaction *tx = tdsql::get_tx_from_thd(thd);
  if (!tx->TxnStarted()) {
    // nothing to prepare
    return HA_EXIT_SUCCESS;
  }
  /*
  if (!tx->can_prepare()) {
    return HA_EXIT_FAILURE;
  }
  if (prepare_tx ||
      (!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {

    if (thd->durability_property == HA_IGNORE_DURABILITY) {
      tx->set_sync(false);
    }
    XID xid;
    thd_get_xid(thd, reinterpret_cast<MYSQL_XID *>(&xid));
    if (!tx->prepare(rdb_xid_to_string(xid))) {
      return HA_EXIT_FAILURE;
    }

    DEBUG_SYNC(thd, "rocksdb.prepared");
  } else
    tx->make_stmt_savepoint_permanent();
  */
 // TDSQL TODO: doing prepare if needed...
  return HA_EXIT_SUCCESS;
}

/**
 do nothing for prepare/commit by xid
 this is needed to avoid crashes in XA scenarios
*/
static xa_status_code rocksdb_commit_by_xid(handlerton *const hton [[maybe_unused]],
                                            XID *const xid [[maybe_unused]]) {
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);
  assert(xid != nullptr);
/**
 * TDSQL: we do not have rocksdb transaction anymore, try leaving this function empty.
 */
/*
  assert(commit_latency_stats != nullptr);

  rocksdb::StopWatchNano timer(rocksdb::Env::Default(), true);

  const auto name = rdb_xid_to_string(*xid);
  assert(!name.empty());

  rocksdb::Transaction *const trx = rdb->GetTransactionByName(name);

  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }

  const rocksdb::Status s = trx->Commit();

  if (!s.ok()) {
    rdb_log_status_error(s);
    DBUG_RETURN(XAER_RMERR);
  }

  delete trx;

  // `Add()` is implemented in a thread-safe manner.
  commit_latency_stats->Add(timer.ElapsedNanos() / 1000);
*/
  DBUG_RETURN(XA_OK);
}

static xa_status_code
rocksdb_rollback_by_xid(handlerton *const hton MY_ATTRIBUTE((__unused__)),
                        XID *const xid [[maybe_unused]]) {
  DBUG_ENTER_FUNC();

  assert(hton != nullptr);
  assert(xid != nullptr);
/**
 * TDSQL: we do not have rocksdb transaction anymore, try leaving this function empty.
 */
/*
  assert(rdb != nullptr);

  const auto name = rdb_xid_to_string(*xid);

  rocksdb::Transaction *const trx = rdb->GetTransactionByName(name);

  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }

  const rocksdb::Status s = trx->Rollback();

  if (!s.ok()) {
    rdb_log_status_error(s);
    DBUG_RETURN(XAER_RMERR);
  }

  delete trx;
*/
  DBUG_RETURN(XA_OK);
}

/**
  Rebuilds an XID from a serialized version stored in a string.
*/
MY_ATTRIBUTE((unused)) static void rdb_xid_from_string(const std::string &, XID *const) {
/* TDSQL: this function is currently not used. */
/*
  assert(dst != nullptr);
  uint offset = 0;
  uint64 raw_fid8 =
      rdb_netbuf_to_uint64(reinterpret_cast<const uchar *>(src.data()));
  const int64 signed_fid8 = *reinterpret_cast<int64 *>(&raw_fid8);
  dst->set_format_id(signed_fid8);
  offset += RDB_FORMATID_SZ;
  dst->set_gtrid_length(src.at(offset));
  offset += RDB_GTRID_SZ;
  dst->set_bqual_length(src.at(offset));
  offset += RDB_BQUAL_SZ;

  assert(dst->get_gtrid_length() >= 0 &&
              dst->get_gtrid_length() <= MAXGTRIDSIZE);
  assert(dst->get_bqual_length() >= 0 &&
              dst->get_bqual_length() <= MAXBQUALSIZE);

  const std::string &tmp_data = src.substr(
      RDB_XIDHDR_LEN, (dst->get_gtrid_length()) + (dst->get_bqual_length()));
  dst->set_data(tmp_data.data(), tmp_data.length());
*/
}

/**
  Reading last committed binary log info from RocksDB system row.
  The info is needed for crash safe slave/master to work.
*/
static int rocksdb_recover(handlerton *, XA_recover_txn *, uint,
                           MEM_ROOT *) {
/**
 * TDSQL: we do not have slave/master anymore, try leaving this function empty.
 */
  return 0;
/*
  if (len == 0 || txn_list == nullptr) {
    return HA_EXIT_SUCCESS;
  }

  std::vector<rocksdb::Transaction *> trans_list;
  rdb->GetAllPreparedTransactions(&trans_list);

  uint count = 0;
  for (auto &trans : trans_list) {
    if (count >= len) {
      break;
    }
    auto name = trans->GetName();
    rdb_xid_from_string(name, &(txn_list[count].id));

    txn_list[count].mod_tables = new (mem_root) List<st_handler_tablename>();
    if (!txn_list[count].mod_tables) break;

    count++;
  }
  return count;
*/
}

static int rocksdb_commit(handlerton *const, THD *const thd,
                          bool commit_tx) {

  assert(thd != nullptr);

  /* note: h->external_lock(F_UNLCK) is called after this function is called) */
  tdsql::Transaction *tx = tdsql::get_tx_from_thd(thd);

  if (tx != nullptr) {
    if (commit_tx || (!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT |
                                                          OPTION_BEGIN))) {
      /*
        We get here
         - For a COMMIT statement that finishes a multi-statement transaction
         - For a statement that has its own transaction
      */
      if (tx->TxnStarted()) {
        uint64 trans_id = tx->trans_id();

#ifndef NDEBUG
        if (thd->variables.fake_commit_fail) {
          int fake_res = HA_EXIT_FAILURE;
          /* Finish this trans anyway. */
          tx->rollback();
          my_error(ER_SQLENGINE_SYSTEM, MYF(0), "fake commit failed");
          LogError("Got unknown error %d during commit, current txid is %lu.",
              fake_res, trans_id);
          thd->variables.fake_commit_fail = false;
          return fake_res;
        }
#endif

        int res = tx->commit();
        if (is_error(res)) {
          if (is_error_txn_aborted_by_tdstore(res)) {
            LogDebug("Got unknown error %d during commit, current txid is %lu.", res, trans_id);
          } else {
            LogError("Got unknown error %d during commit, current txid is %lu.", res, trans_id);
          }
          return res;
        }
      }
    } else {
      /*
        We get here when committing a statement within a transaction.
      */
      return tx->commit_stmt();
    }
  }

  return HA_EXIT_SUCCESS;
}

static int rocksdb_rollback(handlerton *const, THD *const thd,
                            bool rollback_tx) {
  tdsql::Transaction *tx = tdsql::get_tx_from_thd(thd);

  int rc = HA_EXIT_SUCCESS;

  if (tx != nullptr) {
    if (rollback_tx || (!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
      /*
        We get here, when
        - ROLLBACK statement is issued.
        - a statement with AUTOCOMMIT=1 is being rolled back (because of some
          error)

        Discard the changes made by the transaction
      */
      if (tx->TxnStarted()) {
        rc = tx->rollback();
      }
    } else {
      /*
        We get here when
        - a statement inside a transaction is rolled back
      */
      rc = tx->abort_stmt();
    }
  }
  return rc;
}

/*
  returns a vector of info for all non-replication threads
  for use by information_schema.rocksdb_trx
*/
std::vector<Rdb_trx_info> rdb_get_all_trx_info() {
  std::vector<Rdb_trx_info> trx_info;
  return trx_info;
}

/*
  returns a vector of info of recent deadlocks
  for use by information_schema.rocksdb_deadlock
*/
std::vector<Rdb_deadlock_info> rdb_get_deadlock_info() {
  std::vector<Rdb_deadlock_info> deadlock_info;
  return deadlock_info;
}

/*
  This is called for SHOW ENGINE ROCKSDB STATUS | LOGS | etc.

  For now, produce info about live files (which gives an imprecise idea about
  what column families are there).
*/
static bool rocksdb_show_status(handlerton *const hton [[maybe_unused]],
                                THD *const thd [[maybe_unused]],
                                stat_print_fn *const stat_print
                                [[maybe_unused]],
                                enum ha_stat_type) {
  assert(hton != nullptr);
  assert(thd != nullptr);
  assert(stat_print != nullptr);

  bool res = false;

  return res;
}

static inline void rocksdb_register_tx(handlerton *const, THD *const thd,
                                       tdsql::Transaction *const tx) {
  assert(tx != nullptr);

  trans_register_ha(thd, false, rocksdb_hton, nullptr);
  if (my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
    [[maybe_unused]] int ret = tx->start_stmt();
    assert(ret == HA_EXIT_SUCCESS);
    trans_register_ha(thd, true, rocksdb_hton, nullptr);
  }
}

/*
    Supporting START TRANSACTION WITH CONSISTENT SNAPSHOT

    - START TRANSACTION WITH CONSISTENT SNAPSHOT
    takes both InnoDB and RocksDB snapshots, and both InnoDB and RocksDB
    participate in transaction. When executing COMMIT, both InnoDB and
    RocksDB modifications are committed. Remember that XA is not supported yet,
    so mixing engines is not recommended anyway.
*/
static int rocksdb_start_tx_and_assign_read_view(
    handlerton *const hton, /*!< in: RocksDB handlerton */
    THD *const thd)         /*!< in: MySQL thread handle of the
                            user for whom the transaction should
                            be committed */
{
  ulong const tx_isolation = my_core::thd_tx_isolation(thd);

  int ret = HA_EXIT_SUCCESS;
  tdsql::Transaction *tx = tdsql::get_or_create_tx(thd, ret);
  if (ret != HA_EXIT_SUCCESS)
    return ret;
  assert(!IS_NULLPTR(tx));

  // assert(!tx->has_snapshot());
  tx->set_tx_read_only();
  rocksdb_register_tx(hton, thd, tx);

  if (tx_isolation == ISO_REPEATABLE_READ) {
    // tx->acquire_snapshot(true);
  } else {
    push_warning_printf(thd, Sql_condition::SL_WARNING, HA_ERR_UNSUPPORTED,
                        "RocksDB: Only REPEATABLE READ isolation level is "
                        "supported for START TRANSACTION WITH CONSISTENT "
                        "SNAPSHOT in RocksDB Storage Engine. Snapshot has not "
                        "been taken.");
  }
  return HA_EXIT_SUCCESS;
}

/* Dummy SAVEPOINT support. This is needed for long running transactions
 * like mysqldump (https://bugs.mysql.com/bug.php?id=71017).
 * Current SAVEPOINT does not correctly handle ROLLBACK and does not return
 * errors. This needs to be addressed in future versions (Issue#96).
 */
static int rocksdb_savepoint(handlerton *const, THD *const,
                             void *const) {
  return HA_EXIT_SUCCESS;
}

static int rocksdb_rollback_to_savepoint(handlerton *const, THD *const,
                                         void *const) {
  // tdsql::Transaction *&tx = tdsql::get_tx_from_thd(thd);
  // return tx->rollback_to_savepoint(savepoint);
  return true;
}

static bool
rocksdb_rollback_to_savepoint_can_release_mdl(handlerton *const,
                                              THD *const) {
  return true;
}


static uint rocksdb_partition_flags() { return (HA_CANNOT_PARTITION_FK); }

static bool rocksdb_is_supported_system_table(const char *,
                                              const char *table_name,
                                              bool is_sql_layer_system_table) {
  static const char *const tables[] = {"columns_priv",
                                       "db",
                                       "func",
                                       "help_topic",
                                       "help_category",
                                       "help_relation",
                                       "help_keyword",
                                       "plugin",
                                       "procs_priv",
                                       "proxies_priv",
                                       "servers",
                                       "tables_priv",
                                       "time_zone",
                                       "time_zone_leap_second",
                                       "time_zone_name",
                                       "time_zone_transition",
                                       "time_zone_transition_type",
                                       "user",
                                       "role_edges",
                                       "default_roles",
                                       "global_grants",
                                       "password_history",
                                       "component",
                                       "server_cost",
                                       "engine_cost",
                                       "general_log",
                                       "slow_log",
                                       "slave_relay_log_info",
                                       "slave_master_info",
                                       "slave_worker_info",
                                       "gtid_executed",
                                       "statement_outline_rules"};

  static const char *const *const end = tables + UT_ARRAY_SIZE(tables);

  return (is_sql_layer_system_table &&
          std::search_n(tables, end, 1, table_name,
                        [](const char *a, const char *b) {
                          return (strcmp(a, b) == 0);
                        }) != end);
}

static void rocksdb_dict_cache_reset(const char *schema_name,
                                     const char *table_name) {
  char name[FN_REFLEN];
  snprintf(name, sizeof name, "%s.%s", schema_name, table_name);
  DdlManagerLockClosure ddl_manager_lock(true);
  std::shared_ptr<Rdb_tbl_def> const tbl = ddl_manager.find(name, false);
  if (tbl) ddl_manager.remove(tbl, false);
}

void rocksdb_dict_cache_clean_table(TABLE_SHARE *share) {
  assert(share);
  char base_name[FN_REFLEN];
  snprintf(base_name, sizeof base_name, "%s.%s", share->db.str,
           share->table_name.str);
  DdlManagerLockClosure ddl_manager_lock(true);
  if (share->m_part_info) {
    (void)foreach_partition(
        share->m_part_info,
        [&base_name](partition_element *parent_elem,
                     partition_element *part_elem) -> bool {
          char name_buff[FN_REFLEN];
          native_part::part_name(
              name_buff, base_name,
              parent_elem ? parent_elem->partition_name : nullptr,
              part_elem->partition_name);
          std::shared_ptr<Rdb_tbl_def> const tbl =
              ddl_manager.find(name_buff, false);
          if (tbl) ddl_manager.remove(tbl, false);
          return false;
        });
  } else {
    std::shared_ptr<Rdb_tbl_def> const tbl = ddl_manager.find(base_name, false);
    if (tbl) ddl_manager.remove(tbl, false);
  }
}

#if defined(HAVE_PSI_MEMORY_INTERFACE) || defined(HAVE_TDSQL_MEMORY_INTERFACE)
static PSI_memory_info all_myrocks_memory[] = {
  {&key_memory_myrocks_estimated_info, "myrocks_estimated_info", 0, 0, PSI_DOCUMENT_ME}
};

static void init_myrocks_psi_keys() {
  const char *category = "myrocks";
  int count = array_elements(all_myrocks_memory);
  mysql_memory_register(category, all_myrocks_memory, count);
}
#endif /* HAVE_PSI_MEMORY_INTERFACE || HAVE_TDSQL_MEMORY_INTERFACE */

static int rocksdb_start_trx_and_clone_read_view(handlerton*, THD* thd, THD* from_thd) {
  assert(thd && from_thd);
  tdsql::Transaction* from_trx = from_thd->get_tdsql_trans();
  if (!from_trx || !from_trx->TxnStarted()) {
    // Reference innobase_start_trx_and_clone_read_view
    push_warning_printf(thd, Sql_condition::SL_WARNING, HA_ERR_UNSUPPORTED,
                        "TDStore: WITH CONSISTENT SNAPSHOT FROM SESSION was "
                        "ignored because the specified session does not have "
                        "an open transaction inside TDStore.");
    // Return value reference ha_clone_consistent_snapshot
    return 1;
  }

  Transaction* trx = thd->get_tdsql_trans();
  if (trx && trx->TxnStarted()) {
    // Reference innobase_start_trx_and_clone_read_view
    push_warning_printf(thd, Sql_condition::SL_WARNING, HA_ERR_UNSUPPORTED,
                        "TDStore: WITH CONSISTENT SNAPSHOT FROM SESSION was "
                        "ignored because the current session has opened transaction.");
    // Return value reference ha_clone_consistent_snapshot
    return 1;
  }

  trx = new tdsql::Transaction(thd);
  if (trx->clone_from(from_thd->get_tdsql_trans())) {
    // Return value reference ha_clone_consistent_snapshot
    return 1;
  }
  thd->set_tdsql_trans(trx);

  return 0;
}

class SaveUpdateTimeThread : public tdsql::BackgroundThread {
 public:
  SaveUpdateTimeThread()
      : tdsql::BackgroundThread(SYSTEM_THREAD_DD_INITIALIZE) {}

  virtual const char *ThreadName() const override {
    return "myrocks_save_update_time";
  }

 private:
  virtual void Run() override;
};

SaveUpdateTimeThread *save_update_time_thread;

// use update sql to save update_time
void myrocks_execute_update_sql([[maybe_unused]]THD *thd, const std::string &db_table,
                                ulong time_sec) {
  MYSQL_TIME time;
  my_tz_SYSTEM->gmt_sec_to_TIME(&time, time_sec);
  ulonglong update_time = TIME_to_ulonglong_datetime(time);

  size_t pos = db_table.find('.');
  if (pos == std::string::npos) {
    LogError("db_table:%s is not legal", db_table.c_str());
    return;
  }

  std::string db = db_table.substr(0, pos);
  std::string table = db_table.substr(pos + 1);
  char sql_buf[1024];
  snprintf(
      sql_buf, sizeof(sql_buf),
      "update mysql.table_stats set update_time =  %llu, cached_time = %llu "
      "where schema_name = '%s' and table_name = '%s' and (update_time < %llu "
      "or update_time is null)",
      update_time, update_time, db.c_str(), table.c_str(), update_time);

  tdsql::global_sql_runner.SubmitSqlTask("mysql.table_stats", sql_buf);
}

void myrocks_save_update_time(THD *thd) {
  struct Rdb_table_collector : public Rdb_tables_scanner {
   public:
    std::unordered_map<std::string, ulong> update_time;
    int add_table(const std::shared_ptr<Rdb_tbl_def> &tdef) override {
      // do not care race condition
      if (tdef->m_update_time != 0) {
        if (!tdef->m_is_mysql_system_table)
          update_time[tdef->full_tablename()] = tdef->m_update_time;
        tdef->m_update_time = 0;
      }
      return HA_EXIT_SUCCESS;
    }
  } collector;

  ddl_manager.scan_for_tables(&collector);

  for (auto &update_time : collector.update_time) {
    myrocks_execute_update_sql(thd, update_time.first, update_time.second);
  }
}

void SaveUpdateTimeThread::Run() {
  tdsql::Bg_thread_cnt_ctx btcc("myrocks_save_update_time thread",
                                tdsql::BACK_PR);
  PostInit();
  while (1) {
    btcc.Sleep(tdsql_save_update_time_interval * 1000);
    if (btcc.ToExit()) {
      break;
    }
    myrocks_save_update_time(thd_);
  }

  Destroy();

  // we do not call Stop,so delete here
  delete thd_;
  LogInfo("quit myrocks_save_update_time");
}

/*
  Storage Engine initialization function, invoked when plugin is loaded.
*/

static int rocksdb_init_func(void *const p) {

    // Validate the assumption about the size of ROCKSDB_SIZEOF_HIDDEN_PK_COLUMN.
  static_assert(sizeof(longlong) == 8, "Assuming that longlong is 8 bytes.");

  rocksdb_hton = (handlerton *)p;
  mysql_mutex_init(0, &rdb_open_tables.m_mutex, MY_MUTEX_INIT_FAST);

  rocksdb_hton->state = SHOW_OPTION_YES;
  rocksdb_hton->create = rocksdb_create_handler;
  rocksdb_hton->close_connection = rocksdb_close_connection;
  rocksdb_hton->prepare = rocksdb_prepare;
  rocksdb_hton->commit_by_xid = rocksdb_commit_by_xid;
  rocksdb_hton->rollback_by_xid = rocksdb_rollback_by_xid;
  rocksdb_hton->recover = rocksdb_recover;
  rocksdb_hton->commit = rocksdb_commit;
  rocksdb_hton->rollback = rocksdb_rollback;
  rocksdb_hton->db_type = DB_TYPE_ROCKSDB;
  rocksdb_hton->show_status = rocksdb_show_status;
  rocksdb_hton->start_consistent_snapshot =
      rocksdb_start_tx_and_assign_read_view;
  rocksdb_hton->savepoint_set = rocksdb_savepoint;
  rocksdb_hton->savepoint_rollback = rocksdb_rollback_to_savepoint;
  rocksdb_hton->savepoint_rollback_can_release_mdl =
      rocksdb_rollback_to_savepoint_can_release_mdl;
  rocksdb_hton->flush_logs = rocksdb_flush_wal;

  rocksdb_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED |
                        HTON_SUPPORTS_EXTENDED_KEYS | HTON_CAN_RECREATE;

  rocksdb_hton->partition_flags = rocksdb_partition_flags;

  /* TDSQL: Make TDStore support system tables. */
  rocksdb_hton->is_supported_system_table = rocksdb_is_supported_system_table;
  rocksdb_hton->dict_cache_reset = rocksdb_dict_cache_reset;
  rocksdb_hton->clone_consistent_snapshot = rocksdb_start_trx_and_clone_read_view;

  if (ddl_manager.init()) {
    LogPluginErrMsg(ERROR_LEVEL, 0, "Failed to initialize DDL manager.");
    return HA_EXIT_FAILURE;
  }

  save_update_time_thread = new (std::nothrow) SaveUpdateTimeThread();

  save_update_time_thread->Start();

#if defined(HAVE_PSI_MEMORY_INTERFACE) || defined(HAVE_TDSQL_MEMORY_INTERFACE)
  init_myrocks_psi_keys();
#endif /* HAVE_PSI_MEMORY_INTERFACE || HAVE_TDSQL_MEMORY_INTERFACE */

  LogPluginErrMsg(INFORMATION_LEVEL, 0, "instance opened");
  return HA_EXIT_SUCCESS;
}

/*
  Storage Engine deinitialization function, invoked when plugin is unloaded.
*/

static int rocksdb_done_func(void *const) {
  DBUG_ENTER_FUNC();

  int error = 0;

  if (rdb_open_tables.m_hash.size()) {
    // Looks like we are getting unloaded and yet we have some open tables
    // left behind.
    error = 1;
  }

  mysql_mutex_destroy(&rdb_open_tables.m_mutex);

  for (auto &it : rdb_collation_data) {
    delete it;
    it = nullptr;
  }

  ddl_manager.cleanup();

  // clear the initialized flag and unlock
  rdb_get_hton_init_state()->set_initialized(false);

  delete save_update_time_thread;

  DBUG_RETURN(error);
}

/**
  @brief
  Example of simple lock controls. The "table_handler" it creates is a
  structure we will pass to each ha_rocksdb handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Rdb_table_handler *
Rdb_open_tables_map::get_table_handler(const char *const table_name, bool &is_create) {

  assert(table_name != nullptr);

  const std::string s_table_name(table_name);

  // First, look up the table in the hash map.
  RDB_MUTEX_LOCK_CHECK(m_mutex);
  Rdb_table_handler *table_handler = nullptr;
  const auto &it = m_hash.find(s_table_name);
  if (it != m_hash.end()) {
    table_handler = it->second;
    assert(table_handler);
    table_handler->add_ref();
    assert(table_handler->m_ref_count >= 1);
    RDB_MUTEX_UNLOCK_CHECK(m_mutex);
    return table_handler;
  }

  is_create = true;
  // Since we did not find it in the hash map, attempt to create and add it
  // to the hash map.
  table_handler = new Rdb_table_handler(s_table_name);
  if (!table_handler) {
    // Allocating a new Rdb_table_handler and a new table name failed.
    RDB_MUTEX_UNLOCK_CHECK(m_mutex);
    return nullptr;
  }

  m_hash.insert({s_table_name, table_handler});

  assert(table_handler->m_ref_count == 1);

  RDB_MUTEX_UNLOCK_CHECK(m_mutex);

  return table_handler;
}

std::vector<std::string> rdb_get_open_table_names(void) {
  return rdb_open_tables.get_table_names();
}

std::vector<std::string> Rdb_open_tables_map::get_table_names(void) const {
  const Rdb_table_handler *table_handler;
  std::vector<std::string> names;

  RDB_MUTEX_LOCK_CHECK(m_mutex);
  for (const auto &it : m_hash) {
    table_handler = it.second;
    assert(table_handler != nullptr);
    names.push_back(table_handler->m_table_name);
  }
  RDB_MUTEX_UNLOCK_CHECK(m_mutex);

  return names;
}

int ha_rocksdb::update_auto_incr_val(ulonglong val) {
  if (is_partition_table()) {
    return (dynamic_cast<ha_rockspart*>(table->file))
        ->update_auto_incr_val(val);
  }

  return m_autoinc_generator.upd_auto_increment(val);
}

int ha_rocksdb::update_auto_incr_val_from_field() {
  Field *field;
  ulonglong new_val, max_val;
  field = table->key_info[table->s->next_number_index].key_part[0].field;
  max_val = tdsql::Auto_incr_generator::max_val(field->key_type());

  my_bitmap_map *const old_map =
      dbug_tmp_use_all_columns(table, table->read_set);
  new_val = field->val_int();
  // don't increment if we would wrap around
  if (new_val != max_val) {
    new_val++;
  }

  dbug_tmp_restore_column_map(table->read_set, old_map);

  // Only update if positive value was set for auto_incr column.
  if (new_val <= max_val) {
    // tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use);
    // tx->set_auto_incr(m_tbl_def->get_autoincr_gl_index_id(), new_val);

    // Update the in memory auto_incr value in m_tbl_def.
    int err = update_auto_incr_val(new_val);
    if (err) {
      return err;
    }
  }
  // If the given value is negative or exceeds the upper bound, we can simply ignore this value.
  // Such behavior is consistent with the origin myrocks codebase.

  return HA_EXIT_SUCCESS;
}

/* Get next value from hidden pk sequence. */
int ha_rocksdb::get_hidden_pk_val(longlong *val) {
  assert(has_hidden_pk(table));
  return m_hidden_pk_generator.generate_next(val);
}

/* Get the id of the hidden pk id from m_last_rowkey */
int ha_rocksdb::read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id) {
  assert(hidden_pk_id != nullptr);
  assert(table != nullptr);
  assert(has_hidden_pk(table));

  rocksdb::Slice rowkey_slice(m_last_rowkey.ptr(), m_last_rowkey.length());

  // Get hidden primary key from old key slice
  Rdb_string_reader reader(&rowkey_slice);
  if ((!reader.read(Rdb_key_def::INDEX_NUMBER_SIZE)))
    return HA_ERR_ROCKSDB_CORRUPT_DATA;

  const int length = Field_longlong::PACK_LENGTH;
  const uchar *from = reinterpret_cast<const uchar *>(reader.read(length));
  if (from == nullptr) {
    /* Mem-comparable image doesn't have enough bytes */
    return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  *hidden_pk_id = rdb_netbuf_read_uint64(&from);
  return HA_EXIT_SUCCESS;
}

/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the table_handler, then we free the memory associated
  with it.
*/

void Rdb_open_tables_map::release_table_handler(
    Rdb_table_handler *table_handler) {
  assert(table_handler != nullptr);
  assert(table_handler->m_ref_count > 0);

  //after --table_handler->m_ref_count,
  // the table_handler may be not valid,we should not use table_handler,so we use table_name cache table_handler->m_table_name
  std::string table_name(table_handler->m_table_name);
  if (!--table_handler->m_ref_count) {
    table_handler = nullptr;//after --m_ref_count, table_handler is not valid
    RDB_MUTEX_LOCK_CHECK(m_mutex);
    std::unordered_map<std::string, Rdb_table_handler *>::iterator iter = m_hash.find(table_name);
    //double check , after m_mutex
    if (iter != m_hash.end() ) {
      Rdb_table_handler *tmp = iter->second;
      assert(tmp);
      if (!tmp->m_ref_count){
        m_hash.erase(iter);
        delete tmp;
      }
    }
    RDB_MUTEX_UNLOCK_CHECK(m_mutex);
  }
}

void Rdb_table_handler::dec_ref_and_release() {
  rdb_open_tables.release_table_handler(this);
}

static handler *rocksdb_create_handler(my_core::handlerton *const hton,
                                       my_core::TABLE_SHARE *const table_arg,
                                       bool partitioned,
                                       my_core::MEM_ROOT *const mem_root) {

  if (partitioned) {
    ha_rockspart *file = new (mem_root) ha_rockspart(hton, table_arg);
    if (file && file->init_partitioning(mem_root)) {
      destroy(file);
      return (nullptr);
    }
    return (file);
  }

  return new (mem_root) ha_rocksdb(hton, table_arg);
}

// serialize pushdown relate stuff
bool ha_rocksdb::serialize(tdsql::Archive &ar) {
  ar.Serialize(pushed_idx_cond_keyno);
  ar.Serialize(in_range_check_pushed_down);
  ar.Serialize(pushed_idx_cond);
  ar.Serialize(pushed_cond);

  ar.Serialize(tdstore_pushed_cond_idx_);
  if (ar.Serialize(tdstore_pushed_cond_)) return true;

  limit_offset_cond_pushdown.serialize(ar);

  ar.Serialize(m_keyread_only);

  if (ar.IsOutput()) {
    // need change 100,10 to 0,110 for parallel work
    if (limit_offset_cond_pushdown.offset_num() != 0) {
      limit_offset_cond_pushdown.set_limit_num(
          limit_offset_cond_pushdown.limit_num() +
          limit_offset_cond_pushdown.offset_num());
      limit_offset_cond_pushdown.set_offset_num(0);
    }
  }

  return false;
}

ha_rocksdb::ha_rocksdb(my_core::handlerton *const hton,
                       my_core::TABLE_SHARE *const table_arg)
    : handler(hton, table_arg),
      m_table_handler(nullptr), m_td_scan_it(nullptr),
      m_raw_rec_cache_it(nullptr),
      tdstore_pushed_cond_pb_(nullptr),
      m_scan_it_skips_bloom(false),
      m_scan_it_lower_bound(nullptr), m_scan_it_upper_bound(nullptr),
      m_tbl_def(nullptr), m_pk_descr(nullptr), m_key_descr_arr(nullptr),
      m_pk_can_be_decoded(false), m_maybe_unpack_info(false),
      m_pk_tuple(nullptr), m_pk_packed_tuple(nullptr),
      m_sk_packed_tuple(nullptr), m_end_key_packed_tuple(nullptr),
      m_sk_match_prefix(nullptr), m_sk_match_prefix_buf(nullptr),
      m_sk_packed_tuple_old(nullptr), m_dup_sk_packed_tuple(nullptr),
      m_dup_sk_packed_tuple_old(nullptr), m_pack_buffer(nullptr),
      m_lock_rows(RDB_LOCK_NONE), m_keyread_only(false), m_encoder_arr(nullptr),
      m_row_checksums_checked(0)
#if defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
      ,
      m_in_rpl_delete_rows(false), m_in_rpl_update_rows(false),
      m_ds_mrr(this)
#endif  // defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
{
}

const std::string &ha_rocksdb::get_table_basename() const {
  return m_tbl_def->base_tablename();
}

const std::string &ha_rocksdb::get_table_fullname() const {
  return m_tbl_def->full_tablename();
}

/**
  @return
    false  OK
    other  Error inpacking the data
*/
bool ha_rocksdb::init_with_fields() {
  DBUG_ENTER_FUNC();

  const uint pk = table_share->primary_key;
  if (pk != MAX_KEY) {
    const uint key_parts = table_share->key_info[pk].user_defined_key_parts;
    check_keyread_allowed(m_pk_can_be_decoded, table_share, pk /*PK*/,
                          key_parts - 1, true);
  } else
    m_pk_can_be_decoded = false;

  cached_table_flags = table_flags();

  DBUG_RETURN(false); /* Ok */
}

bool ha_rocksdb::rpl_can_handle_stm_event() const noexcept {
  return !(rpl_skip_tx_api_var && !super_read_only);
}

/*
  If the key is a TTL key, we may need to filter it out.

  The purpose of read filtering for tables with TTL is to ensure that
  during a transaction a key which has expired already but not removed by
  compaction yet is not returned to the user.

  Without this the user might be hit with problems such as disappearing
  rows within a transaction, etc, because the compaction filter ignores
  snapshots when filtering keys.
*/
bool ha_rocksdb::should_hide_ttl_rec(const Rdb_key_def &,
                                     const rocksdb::Slice &,
                                     const int64_t) {
  return 0;
}

/**
  Convert record from table->record[0] form into a form that can be written
  into rocksdb.

  @param pk_packed_slice      Packed PK tuple. We need it in order to compute
                              and store its CRC.
  @param packed_rec      OUT  Data slice with record data.
*/

int ha_rocksdb::convert_record_to_storage_format(
    const struct update_row_info &row_info, rocksdb::Slice *const packed_rec) {
  DBUG_ASSERT_IMP(m_maybe_unpack_info, row_info.new_pk_unpack_info);
  assert(m_pk_descr != nullptr);

  Rdb_string_writer *const pk_unpack_info = row_info.new_pk_unpack_info;
  int ret = m_tbl_def->ConvertRecordToStorageFormat(table, pk_unpack_info, m_storage_record);

  if(ret) return ret;

  *packed_rec =
      rocksdb::Slice(m_storage_record.ptr(), m_storage_record.length());

  return HA_EXIT_SUCCESS;
}

/*
  @brief
    Setup which fields will be unpacked when reading rows

  @detail
    Three special cases when we still unpack all fields:
    - When this table is being updated (m_lock_rows==RDB_LOCK_WRITE).
    - When @@rocksdb_verify_row_debug_checksums is ON (In this mode, we need to
  read all fields to find whether there is a row checksum at the end. We could
  skip the fields instead of decoding them, but currently we do decoding.)
    - On index merge as bitmap is cleared during that operation

  @seealso
    ha_rocksdb::setup_field_converters()
    ha_rocksdb::convert_record_from_storage_format()
*/
void ha_rocksdb::setup_read_decoders() {

  m_decoders_vect.clear();
  m_key_requested = false;

  int last_useful = 0;
  int skip_size = 0;

  for (uint i = 0; i < table->s->fields; i++) {
    // bitmap is cleared on index merge, but it still needs to decode columns
    const bool field_requested =
        m_lock_rows == RDB_LOCK_WRITE || 0 ||
        bitmap_is_clear_all(table->read_set) ||
      bitmap_is_set(table->read_set, table->field[i]->field_index());

    // We only need the decoder if the whole record is stored.
    if (m_encoder_arr[i].m_storage_type != Rdb_field_encoder::STORE_ALL) {
      // the field potentially needs unpacking
      if (field_requested) {
        // the field is in the read set
        m_key_requested = true;
      }
      continue;
    }

    if (field_requested) {
      // We will need to decode this field
      m_decoders_vect.push_back({&m_encoder_arr[i], true, skip_size});
      last_useful = m_decoders_vect.size();
      skip_size = 0;
    } else {
      if (m_encoder_arr[i].uses_variable_len_encoding() ||
          m_encoder_arr[i].maybe_null()) {
        // For variable-length field, we need to read the data and skip it
        m_decoders_vect.push_back({&m_encoder_arr[i], false, skip_size});
        skip_size = 0;
      } else {
        // Fixed-width field can be skipped without looking at it.
        // Add appropriate skip_size to the next field.
        skip_size += m_encoder_arr[i].m_pack_length_in_rec;
      }
    }
  }

  // It could be that the last few elements are varchars that just do
  // skipping. Remove them.
  m_decoders_vect.erase(m_decoders_vect.begin() + last_useful,
                        m_decoders_vect.end());
}

#if !defined(NDEBUG)
void dbug_append_garbage_at_end(rocksdb::PinnableSlice *on_disk_rec) {
  std::string str(on_disk_rec->data(), on_disk_rec->size());
  on_disk_rec->Reset();
  str.append("abc");
  on_disk_rec->PinSelf(rocksdb::Slice(str));
}

void dbug_truncate_record(rocksdb::PinnableSlice *on_disk_rec) {
  on_disk_rec->remove_suffix(on_disk_rec->size());
}

void dbug_modify_rec_varchar12(rocksdb::PinnableSlice *on_disk_rec) {
  std::string res;
  // The record is NULL-byte followed by VARCHAR(10).
  // Put the NULL-byte
  res.append("\0", 1);
  // Then, add a valid VARCHAR(12) value.
  res.append("\xC", 1);
  res.append("123456789ab", 12);

  on_disk_rec->Reset();
  on_disk_rec->PinSelf(rocksdb::Slice(res));
}

void dbug_modify_key_varchar8(String &on_disk_rec) {
  std::string res;
  // The key starts with index number
  res.append(on_disk_rec.ptr(), Rdb_key_def::INDEX_NUMBER_SIZE);

  // Then, a mem-comparable form of a varchar(8) value.
  res.append("ABCDE\0\0\0\xFC", 9);
  on_disk_rec.length(0);
  on_disk_rec.append(res.data(), res.size());
}

void dbug_create_err_inplace_alter() {
  my_printf_error(ER_UNKNOWN_ERROR,
                  "Intentional failure in inplace alter occurred.", MYF(0));
}
#endif  // !defined(NDEBUG)

int ha_rocksdb::convert_record_from_storage_format(
    const rocksdb::Slice *const key, uchar *const buf) {

  DBUG_EXECUTE_IF("myrocks_simulate_bad_row_read1",
                  dbug_append_garbage_at_end(&m_retrieved_record););
  DBUG_EXECUTE_IF("myrocks_simulate_bad_row_read2",
                  dbug_truncate_record(&m_retrieved_record););
  DBUG_EXECUTE_IF("myrocks_simulate_bad_row_read3",
                  dbug_modify_rec_varchar12(&m_retrieved_record););

  return convert_record_from_storage_format(key, &m_retrieved_record, buf);
}

/*
  @brief
  Unpack the record in this->m_retrieved_record and this->m_last_rowkey from
  storage format into buf (which can be table->record[0] or table->record[1]).

  @param  key   Table record's key in mem-comparable form.
  @param  buf   Store record in table->record[0] format here

  @detail
    If the table has blobs, the unpacked data in buf may keep pointers to the
    data in this->m_retrieved_record.

    The key is only needed to check its checksum value (the checksum is in
    m_retrieved_record).

  @seealso
    ha_rocksdb::setup_read_decoders()  Sets up data structures which tell which
    columns to decode.

  @return
    0      OK
    other  Error inpacking the data
*/

int ha_rocksdb::convert_record_from_storage_format(
    const rocksdb::Slice *const key, const rocksdb::Slice *const value,
    uchar *const buf) {

  /*
    Decode PK fields from the key
  */
  DBUG_EXECUTE_IF("myrocks_simulate_bad_pk_read1",
                  dbug_modify_key_varchar8(m_last_rowkey););

  struct UnpackArg unpack_arg;
  unpack_arg.arg = nullptr;  // no use
  unpack_arg.type = kSqlEngineNormal;
  if (m_really_push && m_projection) {
    assert(tdstore_pushed_cond_pb_ != nullptr);
    unpack_arg.type = kSqlEngineProj;
  }

  return m_tbl_def->ConvertRecordFromStorageFormat(table,key,value,buf,m_key_requested,m_decoders_vect,&unpack_arg);
}

// called from pushed node to decode data
// unpack_arg is set before call us
int ha_rocksdb::CovertRecordPush(const rocksdb::Slice *const key,
                                 const rocksdb::Slice *const value,
                                 uchar *const buf,
                                 struct UnpackArg *unpack_arg) {
  return m_tbl_def->ConvertRecordFromStorageFormat(
      table, key, value, buf, m_key_requested, m_decoders_vect, unpack_arg);
}

int ha_rocksdb::tdsql_convert_record_from_storage_format(
    const std::string &key, const std::string &value, uchar *const buf) {
  int rc = HA_EXIT_SUCCESS;

  const rocksdb::Slice &rkey = CONVERT_TO_ROCKSDB_SLICE(key);
  const rocksdb::Slice &rvalue = CONVERT_TO_ROCKSDB_SLICE(value);

  m_retrieved_record.Reset();
  m_retrieved_record.PinSelf(rvalue);

  m_last_rowkey.copy(key.c_str(), key.size(), &my_charset_bin);
  rc = convert_record_from_storage_format(&rkey, buf);

  return rc;
}

int ha_rocksdb::tdsql_get_storage_format_pk(tdsql::parallel::StorageFormatKeys* pk) const {

  const uint pk_idx = pk_index(table, m_tbl_def);
  const Rdb_key_def &kd = *m_key_descr_arr[pk_idx];

  uchar key_buf[Rdb_key_def::INDEX_NUMBER_SIZE*2];
  rocksdb::Range range = get_range(kd.get_keyno(), key_buf);
  tdsql::parallel::StorageFormatKey curr_pk;

  curr_pk.key.replace(0, range.start.size(), range.start.ToString());
  curr_pk.successor_key.replace(0, range.limit.size(), range.limit.ToString());
  curr_pk.length = range.start.size();
  curr_pk.index_id = kd.get_index_number();

  pk->push_back(curr_pk);

  return HA_EXIT_SUCCESS;
}

bool ha_rocksdb::tdsql_key_in_pk(const std::string &key) const {
  tdsql::parallel::StorageFormatKeys pk;
  if (!tdsql_get_storage_format_pk(&pk)) {
    assert(pk.size() == 1);
    if (compare_region_key(tdsql::RegionKey(key), tdsql::RegionKey(pk.back().key)) >= 0 &&
        compare_region_key(tdsql::RegionKey(key), tdsql::RegionKey(pk.back().successor_key)) < 0) {
      return true;
    }
  }
  return false;
}

/*
  Setup data needed to convert table->record[] to and from record storage
  format.

  @seealso
     ha_rocksdb::convert_record_to_storage_format,
     ha_rocksdb::convert_record_from_storage_format
*/

void ha_rocksdb::setup_field_converters() {
  m_encoder_arr = m_tbl_def->m_encoder_arr;
  m_maybe_unpack_info = m_tbl_def->m_maybe_unpack_info;
  m_null_bytes_in_rec = m_tbl_def->m_null_bytes_in_rec;
}

int ha_rocksdb::alloc_key_buffers(
    const TABLE *const table_arg,
    const std::shared_ptr<Rdb_tbl_def> &tbl_def_arg, bool alloc_alter_buffers) {
  DBUG_ENTER_FUNC();

  assert(m_pk_tuple == nullptr);
  assert(tbl_def_arg != nullptr);

  std::shared_ptr<Rdb_key_def> *const kd_arr = tbl_def_arg->m_key_descr_arr;

  uint key_len = 0;
  uint max_packed_sk_len = 0;
  uint pack_key_len = 0;

  m_pk_descr = kd_arr[pk_index(table_arg, tbl_def_arg)];
  if (has_hidden_pk(table_arg)) {
    m_pk_key_parts = 1;
  } else {
    m_pk_key_parts =
        table->key_info[table->s->primary_key].user_defined_key_parts;
    key_len = table->key_info[table->s->primary_key].key_length;
  }

#ifdef HAVE_PSI_INTERFACE
  m_pk_tuple =
      static_cast<uchar *>(my_malloc(rdb_handler_memory_key, key_len, MYF(0)));
#else
  m_pk_tuple =
      static_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, key_len, MYF(0)));
#endif
  if (m_pk_tuple == nullptr) {
    goto error;
  }

  pack_key_len = m_pk_descr->max_storage_fmt_length();
#ifdef HAVE_PSI_INTERFACE
  m_pk_packed_tuple = static_cast<uchar *>(
      my_malloc(rdb_handler_memory_key, pack_key_len, MYF(0)));
#else
  m_pk_packed_tuple = static_cast<uchar *>(
      my_malloc(PSI_NOT_INSTRUMENTED, pack_key_len, MYF(0)));
#endif
  if (m_pk_packed_tuple == nullptr) {
    goto error;
  }

  /* Sometimes, we may use m_sk_packed_tuple for storing packed PK */
  max_packed_sk_len = pack_key_len;
  for (uint i = 0; i < table_arg->s->keys; i++) {
    if (i == table_arg->s->primary_key) /* Primary key was processed above */
      continue;

    const uint packed_len = kd_arr[i]->max_storage_fmt_length();
    if (packed_len > max_packed_sk_len) {
      max_packed_sk_len = packed_len;
    }
  }

#ifdef HAVE_PSI_INTERFACE
  if (!(m_sk_packed_tuple = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_sk_match_prefix_buf = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_sk_packed_tuple_old = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_end_key_packed_tuple = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_pack_buffer = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_scan_it_lower_bound = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
      !(m_scan_it_upper_bound = static_cast<uchar *>(
            my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0))))) {
#else
  if (!(m_sk_packed_tuple = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_sk_match_prefix_buf = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_sk_packed_tuple_old = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_end_key_packed_tuple = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_pack_buffer = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_scan_it_lower_bound = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
      !(m_scan_it_upper_bound = static_cast<uchar *>(
            my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0))))) {
#endif
    goto error;
  }

    /*
      If inplace alter is happening, allocate special buffers for unique
      secondary index duplicate checking.
    */
#ifdef HAVE_PSI_INTERFACE
  if (alloc_alter_buffers &&
      (!(m_dup_sk_packed_tuple = static_cast<uchar *>(
             my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))) ||
       !(m_dup_sk_packed_tuple_old = static_cast<uchar *>(
             my_malloc(rdb_handler_memory_key, max_packed_sk_len, MYF(0)))))) {
#else
  if (alloc_alter_buffers &&
      (!(m_dup_sk_packed_tuple = static_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))) ||
       !(m_dup_sk_packed_tuple_old = static_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, max_packed_sk_len, MYF(0)))))) {
#endif
    goto error;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);

error:
  // If we're here then this means that at some point above an allocation may
  // have failed. To avoid any resource leaks and maintain a clear contract
  // we'll clean up before returning the error code.
  free_key_buffers();

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

void ha_rocksdb::free_key_buffers() {
  my_free(m_pk_tuple);
  m_pk_tuple = nullptr;

  my_free(m_pk_packed_tuple);
  m_pk_packed_tuple = nullptr;

  my_free(m_sk_packed_tuple);
  m_sk_packed_tuple = nullptr;

  my_free(m_sk_match_prefix_buf);
  m_sk_match_prefix_buf = nullptr;

  my_free(m_sk_packed_tuple_old);
  m_sk_packed_tuple_old = nullptr;

  my_free(m_end_key_packed_tuple);
  m_end_key_packed_tuple = nullptr;

  my_free(m_pack_buffer);
  m_pack_buffer = nullptr;

  my_free(m_dup_sk_packed_tuple);
  m_dup_sk_packed_tuple = nullptr;

  my_free(m_dup_sk_packed_tuple_old);
  m_dup_sk_packed_tuple_old = nullptr;

  my_free(m_scan_it_lower_bound);
  m_scan_it_lower_bound = nullptr;

  my_free(m_scan_it_upper_bound);
  m_scan_it_upper_bound = nullptr;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::open(const char *const name, int, uint,
                     const dd::Table *table_def) {
  assert(!IS_NULLPTR(table) && !IS_NULLPTR(table->s));

  int err = close();
  if (err) {
    return err;
  }

  auto init_before_open = [this](const char *const name_, bool &is_create)->int {
    if (IsTmpTableForLogService(table)) return HA_EXIT_SUCCESS;
    m_table_handler = rdb_open_tables.get_table_handler(name_, is_create);

    if (m_table_handler == nullptr) {
      return HA_ERR_OUT_OF_MEM;
    }

    my_core::thr_lock_data_init(&m_table_handler->m_thr_lock, &m_db_lock,
                                nullptr);

    return HA_EXIT_SUCCESS;
  };

  bool table_handler_is_create = false;
  err = init_before_open(name, table_handler_is_create);
  if (err) {
    return err;
  }

  std::string fullname;
  err = rdb_normalize_tablename(name, &fullname);
  if (err != HA_EXIT_SUCCESS) {
    return err;
  }

  /* Find table def from m_ddl_hash and create one if not found. */
  bool closed = false;
  err = find_and_create_tbl_def(fullname, table_def, &closed);
  if (err) return err;

  assert(m_tbl_def);

  if (closed) {
    table_handler_is_create = false;
    err = init_before_open(name, table_handler_is_create);
    if (err) {
      return err;
    }
  }

  m_lock_rows = RDB_LOCK_NONE;

  m_key_descr_arr = m_tbl_def->m_key_descr_arr;

  /*
    Full table scan actually uses primary key
    (UPDATE needs to know this, otherwise it will go into infinite loop on
    queries like "UPDATE tbl SET pk=pk+100")
  */
  key_used_on_scan = table->s->primary_key;

  // close() above has already called free_key_buffers(). No need to do it here.
  err = alloc_key_buffers(table, m_tbl_def);

  if (err) {
    return err;
  }

  /*
    init_with_fields() is used to initialize table flags based on the field
    definitions in table->field[].
    It is called by open_binary_frm(), but that function calls the method for
    a temporary ha_rocksdb object which is later destroyed.

    If we are here in ::open(), then init_with_fields() has not been called
    for this object. Call it ourselves, we want all member variables to be
    properly initialized.
  */
  init_with_fields();

  setup_field_converters();

  //SystemTable should compute range statistics(can't reload table statistics to avoid recursive calls)
  //for user table ,next info() will reload statistics
  if (table_handler_is_create) {
    if (IsSystemTable(m_table_handler->m_real_db_name.c_str(), m_table_handler->m_real_table_name.c_str())) {
      need_update_table_cache(false, false);
    } else {
      if(!dd::info_schema::reload_table_stats(current_thd, m_table_handler->m_real_db_name.c_str(),
                                               m_table_handler->m_real_table_name.c_str(), this)) {// if ok, m_table_handler->GetCacheTime() is not 0
        LogInfo("first init dd::info_schema::reload_table_stats table[%s.%s] status success",
                m_table_handler->m_real_db_name.c_str(), m_table_handler->m_real_table_name.c_str());
      } else {
        LogError("first init dd::info_schema::reload_table_stats table[%s.%s] status fail",
                 m_table_handler->m_real_db_name.c_str(), m_table_handler->m_real_table_name.c_str());
      }
    }
  }

  if (info(HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST)) {
    return HA_EXIT_FAILURE;
  }

  /*
    The following load_XXX code calls row decode functions, and they do
    that without having done ::external_lock() or index_init()/rnd_init().
    (Note: this also means we're doing a read when there was no
    setup_field_converters() call)

    Initialize the necessary variables for them:
  */
  /* Initialize hidden_pk val generator. */
  if (has_hidden_pk(table)) {
    err = open_init_hidden_pk(table);
    if (err != HA_EXIT_SUCCESS) {
      free_key_buffers();
      return err;
    }
  }

  /* Initialize auto_increment generator iff it's a non-partitioned table. */
  if (table->found_next_number_field && !is_partition_table()) {
    err = open_init_auto_incr(table);
    if (err != HA_EXIT_SUCCESS) {
      free_key_buffers();
      return err;
    }
  }

  stats.block_size = RDB_DEFAULT_BLOCK_SIZE;

  return HA_EXIT_SUCCESS;
}

AutoIncGeneratorPtr rdb_increment_base::generator() {
  return m_generator;
}

int rdb_increment_base::init_on_open_table_base(
    uint32 index_id,
    enum ha_base_keytype type) {
  assert(index_id != 0);
  int ret = HA_EXIT_SUCCESS;
  if (!generator() || generator()->index_id() != index_id) {
    release_generator();
    m_generator = tdsql::GetOrCreateAutoIncGenerator(index_id, type);
  }

  return ret;
}

ulonglong rdb_autoinc_generator::get_current_val() {
  assert(generator());
  return generator()->current_val();
}

void rdb_autoinc_generator::get_auto_increment(
    ulonglong off, ulonglong inc,
    ulonglong nb_desired_values [[maybe_unused]],
    ulonglong *const first_value,
    ulonglong *const nb_reserved_values) {
  assert(generator());
  /*
    MySQL has a somewhat complicated way of handling the auto-increment value.
    The first time get_auto_increment is called for a statement,
    nb_desired_values is the estimate for how many values will be needed.  The
    engine can then reserve some values, and those will be automatically used
    by MySQL, until a hard-coded value shows up in the insert statement, after
    which MySQL again calls this function to reset its starting value.
   *
    For simplicity we will just ignore nb_desired_values - we aren't going to
    reserve any extra values for a multi-insert statement.  Each row will
    simply acquire the next value as needed and we will always tell MySQL that
    we only reserved 1 value.  Since we are using an atomic value for
    m_auto_incr_val this should be safe - if we had to grab a mutex, doing
    an actual reserve of some values might be a better solution.
   */

  if (off > inc) {
    off = 1;
  }

  // Optimization for the standard case where we are always simply
  // incrementing from the last position

  // Use CAS operation in a loop to make sure automically get the next auto
  // increment value while ensuring that we don't wrap around to a negative
  // number.
  //
  // We set auto_incr to the min of max_val and new_val + 1. This means that
  // if we're at the maximum, we should be returning the same value for
  // multiple rows, resulting in duplicate key errors (as expected).
  //
  // If we return values greater than the max, the SQL layer will "truncate"
  // the value anyway, but it means that we store invalid values into
  // auto_incr that will be visible in SHOW CREATE TABLE.
  ulonglong new_value;
  int32 err = generator()->generate_next(&new_value, off, inc);
  if (is_error_ok(err)) {
    *first_value = new_value;
    *nb_reserved_values = 1;
  } else {
    // first_value = ULLONG_MAX (2^64-1) stands for error in handler::update_auto_increment().
    *first_value = ULLONG_MAX;
    *nb_reserved_values = 0;
  }
}

int rdb_autoinc_generator::upd_auto_increment(ulonglong val) {
  assert(!IS_NULLPTR(generator()));

  ulonglong auto_incr_val = generator()->current_val();
  int ret = error_ok();

  while (auto_incr_val < val && !(ret = generator()->push_val(val)) ) {
    // loop until: 1) auto_incr_val >= val; 2) we successfully set ai to val; 3) error occurs.
    auto_incr_val = generator()->current_val();
  }

  if (is_error(ret)) {
    LogDebug("Failed to push value to %llu, current value %llu",
             val, generator()->current_val());
  }
  return ret;
}

int rdb_autoinc_generator::generate_next_batch() {
  return generator()->rpc_generate_next_batch_wrapper(0,
      GetAutoIncBatchSize(generator()->index_id()));
}

int rdb_autoinc_generator::init_on_open_table(
    uint32 index_id,
    enum ha_base_keytype type) {
  // When bootstrapper is in load core system tables stage, Only primary key and
  // unique name key is known, other index_ids can be zero.
  if (index_id == tdsql::kInvalidIndexId && current_thd->is_dd_system_thread())
    return HA_EXIT_SUCCESS;

  int ret = init_on_open_table_base(index_id, type);
  if (is_error_ok(ret)) {
    /*
      Auto increment field loads value on first opening
      so that SHOW CREATE TABLE displays the correct AUTO_INCREMENT value.
    */
    ret = generator()->load_on_first_use();
  }
  return ret;
}

int rdb_autoinc_generator::init_on_create_table(
    uint32 index_id,
    ulonglong val,
    enum ha_base_keytype type) {
  assert(index_id != 0);
  assert(val != 0);
  /*
    Only called by ha_rocksdb::create and
    generator() must be nullptr then.
  */
  assert(generator() == nullptr);
  int err = HA_EXIT_SUCCESS;

  /*
    Auto_incr_generator might have been created for index_id, i.e.,
    sql#1. CRAETE TABLE t (a INT AUTO_INCREMENT PRIMARY KEY, b INT);
    sql#2. CREATE INDEX t_b ON t (b) ALGORITHM=INPLACE;
    sql#1 creates an generator.
    sql#2 creates another table which has the same index_ids
  */
  m_generator = GetOrCreateAutoIncGenerator(index_id, type);
  assert(!!generator());

  /* Protected by MDL lock and can't be accessed by another coroutine. */
  err = upd_auto_increment(val);
  if (err != HA_EXIT_SUCCESS) {
    release_generator();
    return err;
  }
  return err;
}

int rdb_hidden_pk_generator::init_on_open_table(uint32 index_id) {
  // When bootstrapper is in load core system tables stage, Only primary key and
  // unique name key is known, other index_ids can be zero.
  if (index_id == tdsql::kInvalidIndexId && current_thd->is_dd_system_thread())
    return HA_EXIT_SUCCESS;
  return init_on_open_table_base(index_id, m_type);
}

int rdb_hidden_pk_generator::generate_next(longlong *val) {
  assert(m_type == HA_KEYTYPE_LONGLONG);
  assert(val);
  assert(generator());
  ulonglong uval;
  int ret = generator()->generate_next(&uval, 1, 1);
  if (is_error_ok(ret)) {
    *val = (longlong)uval; // Auto_incr_generator prevents from overflow.
  }
  return ret;
}

int ha_rocksdb::close(void) {
  DBUG_ENTER_FUNC();

  m_pk_descr = nullptr;
  m_key_descr_arr = nullptr;

  free_key_buffers();

  if (m_table_handler != nullptr) {
    rdb_open_tables.release_table_handler(m_table_handler);
    m_table_handler = nullptr;
  }

  // These are needed to suppress valgrind errors in rocksdb.partition
  m_storage_record.mem_free();
  m_last_rowkey.mem_free();
  m_sk_tails.free();
  m_sk_tails_old.free();
  m_pk_unpack_info.free();

  m_autoinc_generator.release_generator();
  m_hidden_pk_generator.release_generator();

  release_td_scan_iterator();

  if (m_parallel_thomas_put != nullptr) {
    m_parallel_thomas_put.reset(nullptr);
  }

  if (IsTmpTableForLogService(table) && m_tbl_def) {
    delete_tbl_def_cache();
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

static const char *rdb_error_messages[] = {
    "Table must have a PRIMARY KEY.",
    "Specifying DATA DIRECTORY for an individual table is not supported.",
    "Specifying INDEX DIRECTORY for an individual table is not supported.",
    "RocksDB commit failed.",
    "Failure during bulk load operation.",
    "Found data corruption.",
    "CRC checksum mismatch.",
    "Invalid table.",
    "Could not access RocksDB properties.",
    "File I/O error during merge/sort operation.",
    "RocksDB status: not found.",
    "RocksDB status: corruption.",
    "RocksDB status: invalid argument.",
    "RocksDB status: io error.",
    "RocksDB status: no space.",
    "RocksDB status: merge in progress.",
    "RocksDB status: incomplete.",
    "RocksDB status: shutdown in progress.",
    "RocksDB status: timed out.",
    "RocksDB status: aborted.",
    "RocksDB status: lock limit reached.",
    "RocksDB status: busy.",
    "RocksDB status: deadlock.",
    "RocksDB status: expired.",
    "RocksDB status: try again.",
    "Correlate table option error.",
    "Parallel scan failed.",
};

static_assert((sizeof(rdb_error_messages) / sizeof(rdb_error_messages[0])) ==
                  ((HA_ERR_ROCKSDB_LAST - HA_ERR_ROCKSDB_FIRST) + 1),
              "Number of error messages doesn't match number of error codes");

static const char *rdb_get_error_messages(int error) {
  if (error >= HA_ERR_ROCKSDB_FIRST && error <= HA_ERR_ROCKSDB_LAST) {
    return rdb_error_messages[error - HA_ERR_ROCKSDB_FIRST];
  }
  return "";
}

bool ha_rocksdb::get_error_message(const int error, String *const buf) {
  DBUG_ENTER_FUNC();

  static_assert(HA_ERR_ROCKSDB_LAST > HA_ERR_FIRST,
                "HA_ERR_ROCKSDB_LAST > HA_ERR_FIRST");
  static_assert(HA_ERR_ROCKSDB_LAST > HA_ERR_LAST,
                "HA_ERR_ROCKSDB_LAST > HA_ERR_LAST");

  assert(buf != nullptr);

  buf->append(rdb_get_error_messages(error));

  // We can be called with the values which are < HA_ERR_FIRST because most
  // MySQL internal functions will just return HA_EXIT_FAILURE in case of
  // an error.

  DBUG_RETURN(false);
}

/* MyRocks supports only the following collations for indexed columns */
static const std::set<const my_core::CHARSET_INFO *> RDB_INDEX_COLLATIONS = {
    &my_charset_bin, &my_charset_utf8_bin, &my_charset_latin1_bin};


int rdb_normalize_tablename(const std::string &tablename,
                            std::string *const strbuf) {
  assert(strbuf != nullptr);

  if (tablename.size() < 2 || tablename[0] != '.' || tablename[1] != '/') {
    assert(0);  // We were not passed table name?
    return HA_ERR_ROCKSDB_INVALID_TABLE;
  }

  size_t pos = tablename.find_first_of('/', 2);
  if (pos == std::string::npos) {
    assert(0);  // We were not passed table name?
    return HA_ERR_ROCKSDB_INVALID_TABLE;
  }

  *strbuf = tablename.substr(2, pos - 2) + "." + tablename.substr(pos + 1);

  return HA_EXIT_SUCCESS;
}

int tdstore_normalize_db_table_name(const std::string &file_name,
                            std::string *const db_name, std::string *const table_name) {
  assert(db_name != nullptr);
  assert(table_name != nullptr);

  if (file_name.size() < 2 || file_name[0] != '.' || file_name[1] != '/') {
    assert(0);
    return HA_ERR_ROCKSDB_INVALID_TABLE;
  }

  size_t pos = file_name.find_first_of('/', 2);
  if (pos == std::string::npos) {
    assert(0);
    return HA_ERR_ROCKSDB_INVALID_TABLE;
  }

  *db_name = file_name.substr(2, pos - 2);
  *table_name = file_name.substr(pos + 1);

  return HA_EXIT_SUCCESS;
}

/*
  Check to see if the user's original statement includes foreign key
  references
*/
bool ha_rocksdb::contains_foreign_key(THD *const thd) {
  bool success;

  const char *str = thd->query().str;

  assert(str != nullptr);

  while (*str != '\0') {
    // Scan from our current pos looking for 'FOREIGN'
    str = rdb_find_in_string(str, "FOREIGN", &success);
    if (!success) {
      return false;
    }

    // Skip past the found "FOREIGN'
    str = rdb_check_next_token(&my_charset_bin, str, "FOREIGN", &success);
    assert(success);

    if (!my_isspace(&my_charset_bin, *str)) {
      return false;
    }

    // See if the next token is 'KEY'
    str = rdb_check_next_token(&my_charset_bin, str, "KEY", &success);
    if (!success) {
      continue;
    }

    // See if the next token is '('
    str = rdb_check_next_token(&my_charset_bin, str, "(", &success);
    if (!success) {
      // There is an optional index id after 'FOREIGN KEY', skip it
      str = rdb_skip_id(&my_charset_bin, str);

      // Now check for '(' again
      str = rdb_check_next_token(&my_charset_bin, str, "(", &success);
    }

    // If we have found 'FOREIGN KEY [<word>] (' we can be confident we have
    // a foreign key clause.
    return success;
  }

  // We never found a valid foreign key clause
  return false;
}

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)

  @see
  ha_create_table() in handle.cc
*/

int ha_rocksdb::create(const char *const name, TABLE *const table_arg,
                       HA_CREATE_INFO *const create_info,
                       dd::Table *table_def) {

  assert(table_arg != nullptr);
  assert(create_info != nullptr);

  THD *const thd = my_core::thd_get_current_thd();

  if (unlikely(create_info->data_file_name)) {
    // DATA DIRECTORY is used to create tables under a specific location
    // outside the MySQL data directory. We don't support this for MyRocks.
    // The `rocksdb_datadir` setting should be used to configure RocksDB data
    // directory.
    return HA_ERR_ROCKSDB_TABLE_DATA_DIRECTORY_NOT_SUPPORTED;
  }

  if (unlikely(create_info->index_file_name)) {
    // Similar check for INDEX DIRECTORY as well.
    return HA_ERR_ROCKSDB_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED;
  }

  if (unlikely(create_info->encrypt_type.length > 0 &&
               my_strcasecmp(system_charset_info, "n",
                             create_info->encrypt_type.str) != 0)) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "ENCRYPTION for the RocksDB storage engine");
    return HA_WRONG_CREATE_OPTION;
  }

  if (unlikely(create_info->tablespace))
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_NOT_SUPPORTED_YET,
                        ER_THD(thd, ER_FEATURE_UNSUPPORTED),
                        "'TABLESPACEs for the RocksDB storage engine'",
                        "use default TABLESPACE");

  if (unlikely(create_info->compress.length)) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "InnoDB page COMPRESSION for the RocksDB storage engine");
    return HA_WRONG_CREATE_OPTION;
  }

  std::string str;
  int err;

  std::shared_ptr<Rdb_tbl_def> rdb_tbl_def = get_table_if_exists(name);
  if (rdb_tbl_def) {
    // get_table_if_exists and delete_tbl_def acquire ddl_manager.rwlock
    // respectively. These two operations are not atomic and acceptable because
    // ha_rocksdb::create only takes place in CREATE TABLE and DDL is exclusive.
    err = delete_tbl_def(rdb_tbl_def);
    bool is_truncate = (thd->lex->sql_command == SQLCOM_TRUNCATE);
    if (err != HA_EXIT_SUCCESS) {
      if (!is_truncate) {
        LogError(
            "[delete_tbl_def failed][A remaining table def needs to be removed "
            "from ddl_manager before creating a new table with an identical "
            "name][table:%s][rdb_tbl_def:%s][query:%s]",
            name, rdb_tbl_def->to_string().data(), thd->query().str);
      }
      return err;
    }
    if (!is_truncate) {
      LogDebug(
          "[delete_tbl_def OK][A remaining table def needs to be removed "
          "from ddl_manager before creating a new table with an identical "
          "name][table:%s][rdb_tbl_def:%s][query:%s]",
          name, rdb_tbl_def->to_string().data(), thd->query().str);
    }
  }

  if (contains_foreign_key(thd)) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "FOREIGN KEY for the RocksDB storage engine");
    return HA_ERR_UNSUPPORTED;
  }

  err = create_tbl_def(name, table_arg, create_info, table_def);

  if (err == HA_EXIT_SUCCESS) {
    LogDebug("[create_tbl_def OK][table:%s][m_tbl_def:%s]",
             table_arg->to_string().data(), m_tbl_def->to_string().data());
  }

  return err;
}

/**
  @brief
  table_id_equals_pk_id() is called to make sure that
  a table's tindex_id equals to its user-defined primary key's tindex_id.

  @return
    true  a) no user-defined primary key. or
          b) table's tindex_id equals to user-defined primary key's tindex_id. or
          c) unique key is used as a primary key (this happens to system tables).
    false a) table's tindex_id differs from user-defined primary key's tindex_id. and
          b) unique key is not used as a primary key.
*/
bool ha_rocksdb::table_id_equals_pk_id(TABLE *const table_arg) {
  assert(table_arg);
  assert(table_arg->s);
  const TABLE_SHARE *const share = table_arg->s;

  if (share->primary_key == MAX_KEY) {
    return true;
  }

  // share->primary_key points to the candidate key.
  if (is_partition_table()) {
#ifndef NDEBUG
    dbg_assert_part_elem(table_arg);
#endif
    return (m_part_elem->tindex_id ==
            m_part_elem->get_partition_index_tindex_id(table_arg,
                                                       share->primary_key));
  }

  return (share->tindex_id == share->key_info[share->primary_key].tindex_id);
}

/**
  @brief
  Remove tbl from ddl_manager.m_ddl_hash and release it.

  It is for such a scenario:
    Step1. Server1 and server2 acknowledg table 't'.
    Step2. Server1 DROP TABLE 't'.
           Server2 knows the fact that 't' has been dropped in terms of server layer while it still has an old 't' in ddl_manager.m_ddl_hash.
    Step3. Server2 CREATE TABLE 't'.
           get_table_if_exists('t') returns the old 't' and it should be removed so that the new 't' can be established.

  NOTE: Don't bother cleaning up index data as the server which executed DROP has cleaned it up.
*/
int ha_rocksdb::delete_tbl_def(const std::shared_ptr<Rdb_tbl_def> &tbl) {
  assert(tbl != nullptr);

  /*
    Remove the table entry in data dictionary (this will also remove it from
    the persistent data dictionary).
  */
  ddl_manager.remove(tbl, true /*lock*/);

  return HA_EXIT_SUCCESS;
}

/**
  @brief
  name is in form "./dbname/tablename".
*/
int ha_rocksdb::create_tbl_def(const char *const name, TABLE *const table_arg,
                               HA_CREATE_INFO *const create_info,
                               const dd::Table *table_def) {
  std::string str;
  int err = HA_EXIT_SUCCESS;

  /*
    Construct dbname.tablename ourselves, because parititioning
    passes strings like "./test/t14#P#p0" for individual partitions,
    while table_arg->s->table_name has none of that.
  */
  err = rdb_normalize_tablename(name, &str);
  if (err != HA_EXIT_SUCCESS) {
    return err;
  }

  return create_tbl_def(str, table_arg, create_info, table_def);
}

/**
  @brief
  name is in form "dbname.tablename".
*/
int ha_rocksdb::create_tbl_def(const std::string &str, TABLE *const table_arg,
                               HA_CREATE_INFO *const create_info,
                               const dd::Table *table_def, const bool lock,
                               const bool put_in_ddl_manager) {
  assert(!str.empty());
  assert(!IS_NULLPTR(table_arg));
  assert(!IS_NULLPTR(create_info));
  assert(table_id_equals_pk_id(table_arg));

  int err = HA_EXIT_SUCCESS;

  /* Create table definition and put it into ddl_manager.m_ddl_hash. */
  m_tbl_def = CreateTblDef(str, table_arg, lock, m_part_pos);

  //partition table will consume too much memory,disable it
  //NOTO: should also disable pushdown
  if (!is_system_table(table_arg->s->db.str, table_arg->s->table_name.str) &&
      table_def && !is_partition_table()) {
    THD *thd = current_thd;
    dd::String_type schema = table_arg->s->db.str;
    dd::Sdi_type sdi = dd::serialize(thd, *table_def, schema);
    m_tbl_def->set_sdi(&sdi);
  }

  uint pk_offset = 0;

  m_key_descr_arr = m_tbl_def->m_key_descr_arr;
  pk_offset = pk_index(table_arg, m_tbl_def);

  m_pk_descr = m_key_descr_arr[pk_offset];

  /*
    Init m_autoinc_generator if an init autoinc val is given in CREATE SQL.
    ha_rocksdb::open() inits m_autoinc_generator if there isn't an init autoinc val in CREATE SQL.
  */
  if (create_info->auto_increment_value && !is_partition_table()) {
    if (table_arg->found_next_number_field) {
      err = m_autoinc_generator.init_on_create_table(
          table_arg->s->autoinc_tindex_id,
          create_info->auto_increment_value,
          table_arg->found_next_number_field->key_type());
      if (err != HA_EXIT_SUCCESS) {
        delete_tbl_def_cache();
        return err;
      }
    } else {
      /*
        Autoinc field not defined but an auto_increment value is given.
        Ignore it.
      */
      LogWarn("Autoinc field not defined but an auto_increment value is given. "
          "Ignore it. Sql: %s", thd_query_unsafe(current_thd).str);
      push_warning(current_thd, Sql_condition::SL_WARNING, ER_AUTOINC_VAL_NO_DEF, nullptr);
    }
  }

  if (put_in_ddl_manager && !IsTmpTableForLogService(table)) {
    err = ddl_manager.put(m_tbl_def, lock);
  }
  if (err != HA_EXIT_SUCCESS) {
    delete_tbl_def_cache();
    return err;
  }

  return HA_EXIT_SUCCESS;
}

int ha_rocksdb::find_and_create_tbl_def(const std::string &fullname,
                                        const dd::Table *table_def,
                                        bool *closed) {
  int err = HA_EXIT_SUCCESS;
  /*
    Compatibility.
    create_info is used to pass autoinc val and it's meaningless because
    we are not in a real CREATE TABLE. Only a real CREATE TABLE sets up
    initial autoinc value.
  */
  HA_CREATE_INFO ci;

  DdlManagerLockClosure ddl_mgr_lock_closure(true /*wrlock*/);

  if (!IsTmpTableForLogService(table)) {
    m_tbl_def = ddl_manager.find(fullname, false /*lock*/);
  }

  if (m_tbl_def == nullptr) {
    /* Occasion: Server#1 created table t1 and server#2 tries to access it. */
    LogDebug(
        "Attempt to open a table that is not present in "
        "ddl_manager.m_ddl_hash. Try to create one according to server layer "
        "dd. table:%s",
        fullname.data());
    err = create_tbl_def(fullname, table, &ci, table_def, false /*lock*/,
                         true /*put_in_ddl_manger*/);
    if (err) {
      LogError(
          "Attempt to open a table that is not present in "
          "ddl_manager.m_ddl_hash. Failed to create table object according to "
          "server layer dd. table:%s",
          fullname.data());
      return err;
    }
    LogDebug(
        "Attempt to open a table that is not present in "
        "ddl_manager.m_ddl_hash. Succeed creating table object according to "
        "server layer dd. Rdb_tbl_def:%s",
        m_tbl_def->to_string().data());
    return err;
  }

  // Found rdb_tbl_def from ddl_manager.m_ddl_hash.
  LogDebug("Open table from ddl_manager.m_ddl_hash. table:%s, m_tbl_def:%s",
           table->to_string().data(), m_tbl_def->to_string().data());

  // Compare table and m_tbl_def.
  // Table comes from SQL layer.
  // m_tbl_def comes from ddl_manager.m_ddl_hash.
  int cmp = cmp_tbl_def_and_table();
  if (cmp == 0) return err;

  // m_tbl_def is different to table.
  // Create an Rdb_tbl_def according to TABLE (and TABLE_SHARE) and bind
  // it to m_tbl_def. If table is newer than m_tbl_def, put the created
  // rdb_tbl_def into ddl_manager.m_ddl_hash.
  std::string dbname_tblname = m_tbl_def->full_tablename();
  err = close();
  if (err) return err;
  *closed = true;

  bool put_in_ddl_manager = (cmp < 0);
  err = create_tbl_def(fullname, table, &ci, table_def, false /*lock*/,
                       put_in_ddl_manager);
  if (err) {
    LogError("[create_tbl_def failed][table is %s than m_tbl_def][query:%s]",
             put_in_ddl_manager ? "newer" : "older",
             table->in_use->query().str);
  } else {
    LogDebug("[create_tbl_def OK][table is %s than m_tbl_def][query:%s]",
             put_in_ddl_manager ? "newer" : "older",
             table->in_use->query().str);
  }

  return err;
}

// Compare tindex_id, schema_version and indexes of m_tbl_def and table.
// return 0: m_tbl_def == table, equal definition.
//       -1: m_tbl_def < table, m_tbl_def is older.
//        1: m_tbl_def > table, m_tbl_def is newer.
int ha_rocksdb::cmp_tbl_def_and_table() {
#ifndef NDEBUG
  if (is_partition_table()) dbg_assert_part_elem(table);
#endif
  // Compare tindex_id:
  // If tindex_id are different, the one with larger tindex_id is the new
  // version (because tindex_id is not reused).
  auto table_tindex_id =
      is_partition_table() ? m_part_elem->tindex_id : table->s->tindex_id;
  if (m_tbl_def->m_tindex_id < table_tindex_id) return -1;
  if (m_tbl_def->m_tindex_id > table_tindex_id) return 1;

  // Compare schema_version:
  // If schema_version are different, the one with larger schema_version is the
  // new version (because schema_version is always incremented by 1).
  if (m_tbl_def->m_schema_version < table->schema_version) return -1;
  if (m_tbl_def->m_schema_version > table->schema_version) return 1;

  // Compare indexes
  //
  // Why compare indexes ?
  // Identical tindex_id and schema_version are not enough to account for
  // identical Rdb_tbl_def and Table(or TABLE_SHARE).
  // Please see
  // https://git.woa.com/tdsql3.0/SQLEngine/issues/1453#note_92526744.
  //
  // What if indexes are different ?
  // If indexes are different, cannot determine which is new or old, treat TABLE
  // as new version.
  // - New version TABLE_SHARE, old version Rdb_tbl_def:
  //   Other nodes did DDL, this node is late to know.
  //   TABLE_SHARE is updated according to DDL, but rdb_tbl_def is still behind
  //
  // - Old version TABLE_SHARE, new version Rdb_tbl_def:
  //   DML and DDL concurrency.
  //   A DML holds old version TABLE_SHARE, not reached ha_rocksdb::open yet.
  //   A DDL updated new version rdb_tbl_def to ddl_manger.m_ddl_hash.
  //   Then the DML reaches ha_rocksdb::open, sees new version from m_ddl_hash
  //   and it creates rdb_tbl_def based on old share and puts the rdb_tbl_def
  //   to m_ddl_hash, then m_ddl_hash becomes old version again.
  //   A next DML will use new TABLE_SHARE, create a new version rdb_tbl_def and
  //   put it into m_ddl_hash.
  if (is_partition_table()) {
    if (!m_tbl_def->has_the_same_key(table, m_part_elem)) return -1;
  } else {
    if (!m_tbl_def->has_the_same_key(table)) return -1;
  }

  return 0;
}

/**
  @note
  This function is used only when the table has not yet been opened, and
  keyread_allowed bitmap doesn't have the correct values yet.

  See comment in ha_rocksdb::index_flags() for details.
*/

bool ha_rocksdb::check_keyread_allowed(bool &pk_can_be_decoded,
                                       const TABLE_SHARE *table_share, uint inx,
                                       uint part, bool all_parts) {
  bool res = true;

  KEY *const key_info = &table_share->key_info[inx];

  //for new format,all field can decode
  if(key_info->kv_format_version>=FORMAT_VERSION_COLL) {
      pk_can_be_decoded = true;
      return true;
  }

  //TODO:remove this,like 8.0.23

  Rdb_field_packing dummy1;
  res = dummy1.setup(nullptr, key_info->key_part[part].field, inx, part,
                     key_info->key_part[part].length);

  if (res && all_parts) {
    for (uint i = 0; i < part; i++) {
      Field *field;
      if ((field = key_info->key_part[i].field)) {
        Rdb_field_packing dummy;
        if (!dummy.setup(nullptr, field, inx, i,
                         key_info->key_part[i].length)) {
          /* Cannot do index-only reads for this column */
          res = false;
          break;
        }
      }
    }
  }

  const uint pk = table_share->primary_key;
  if (inx == pk && all_parts &&
      part + 1 == table_share->key_info[pk].user_defined_key_parts) {
    pk_can_be_decoded = res;
  }

  return res;
}

int ha_rocksdb::read_key_exact(const Rdb_key_def &kd,
                               tdsql::Iterator *const iter,
                               const bool &full_key_match [[maybe_unused]],
                               const rocksdb::Slice &key_slice,
                               const int64_t ttl_filter_ts [[maybe_unused]]) {
  assert(iter != nullptr);
  assert(kd.get_keyno() == iter->keyno());
  set_write_fence(iter);
  int rc = HA_EXIT_SUCCESS;
  while (1) {
    rc = iter->Next();
    if (is_error_iterator_no_more_rec(rc)) {
      return HA_ERR_KEY_NOT_FOUND;
    } else if (is_error(rc)) {
      return rc;
    } else {
      if (kd.value_matches_prefix(CONVERT_TO_ROCKSDB_SLICE(iter->key()), key_slice)) {
        return HA_EXIT_SUCCESS;
      }
    }
  }

  /*
    Got a record that is not equal to the lookup value, or even a record
    from another table.index.
  */
  return HA_ERR_KEY_NOT_FOUND;
}

int ha_rocksdb::read_row_from_primary_key(uchar *const buf) {
  assert(buf != nullptr);

  int rc;
  const rocksdb::Slice &rkey = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->key());
  const uint pk_size = rkey.size();
  const char *pk_data = rkey.data();

  memcpy(m_pk_packed_tuple, pk_data, pk_size);
  m_last_rowkey.copy(pk_data, pk_size, &my_charset_bin);

  /* Unpack from the row we've read */
  const rocksdb::Slice &value = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->value());
  m_retrieved_record.Reset();
  m_retrieved_record.PinSelf(value);
  rc = convert_record_from_storage_format(&rkey, &value, buf);

  return rc;
}

static inline bool enable_pk_preload(TABLE* table) {
  THD* thd = table->in_use;
  if (thd && thd->lex &&
      thd->lex->sql_command == SQLCOM_SELECT &&
      !tdsql::IsSystemTable(table->tindex_id))
    return thd->variables.tdsql_enable_pk_preload;

  return false;
}
// save pk-part from key(sk) in pk
// return 1 if fail
bool ha_rocksdb::GetPKFromSK(myrocks::rocksdb::Slice *key, int index,
                             tdsql::Slice* pk) {
  const Rdb_key_def &kd = *m_key_descr_arr[index];

  uint pk_size =
      kd.get_primary_key_tuple(table, *m_pk_descr, key, m_pk_packed_tuple);

  if (pk_size == RDB_INVALID_KEY_LEN) return 1;

  pk->assign((char*)m_pk_packed_tuple, pk_size);
  return 0;
}

int ha_rocksdb::read_row_from_secondary_key(uchar *const buf,
                                            const Rdb_key_def &kd,
                                            bool move_forward) {
  assert(buf != nullptr);

  int rc = 0;
  uint pk_size;

  /*
    TDSQL and latest percona server add.
    Due to MRR, now an index-only scan have pushed index condition.
    (If it does, we follow non-index only code path here, except that
    we don't fetch the row).
  */
  if (kd.m_is_reverse_cf) move_forward = !move_forward;
  rc = find_icp_matching_index_rec(move_forward, buf);
  if (rc) return rc;

  /* Get the key columns and primary key value */
  const rocksdb::Slice &rkey = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->key());
  const rocksdb::Slice &value = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->value());

#if !defined(NDEBUG)
  bool save_keyread_only = m_keyread_only;
#endif  // !defined(NDEBUG)
  DBUG_EXECUTE_IF("dbug.rocksdb.HA_EXTRA_KEYREAD", { m_keyread_only = true; });

  bool covered_lookup = (m_keyread_only && kd.can_cover_lookup()) ||
                        kd.covers_lookup(table, &value, &m_lookup_bitmap) ||
                        m_only_calc_records;

#if !defined(NDEBUG)
  m_keyread_only = save_keyread_only;
#endif  // !defined(NDEBUG)

  if (covered_lookup && m_lock_rows == RDB_LOCK_NONE) {
    pk_size =
        kd.get_primary_key_tuple(table, *m_pk_descr, &rkey, m_pk_packed_tuple);
    if (pk_size == RDB_INVALID_KEY_LEN) {
      rc = HA_ERR_ROCKSDB_CORRUPT_DATA;
    } else {
      rc = kd.unpack_record(table, buf, &rkey, &value,
                            0);
      global_stats.covered_secondary_key_lookups.inc();
    }
  } else {
    const rocksdb::Slice &rkey_ = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->key());
    pk_size = kd.get_primary_key_tuple(table, *m_pk_descr, &rkey_,
                                       m_pk_packed_tuple);
    if (pk_size == RDB_INVALID_KEY_LEN) {
      rc = HA_ERR_ROCKSDB_CORRUPT_DATA;
    } else {
      /* We are sure that preloading is needed now. */
      assert((uint)active_index != pk_index(table, m_tbl_def));
      if (m_raw_rec_cache_it != nullptr ||
          (m_raw_rec_cache_it == nullptr && enable_pk_preload(table))) {
        rc = get_row_from_raw_rec_cache(active_index, buf);
      } else {
        rc = get_row_by_rowid(buf, m_pk_packed_tuple, pk_size);
      }
    }
  }

  if (!rc) {
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, pk_size,
                       &my_charset_bin);
  }

  return rc;
}

/**
  @note
    The problem with this function is that SQL layer calls it, when
     - the table has not been yet opened (no ::open() call done)
     - this->table_share already exists, but it is in the process of being
       filled, so some of fields are still NULL.
     - In particular, table_share->key_info[inx].key_part[] is filled only up
       to part #part. Subsequent key parts are not yet filled.

    To complicate things further, SQL layer will call index_flags() with
    all_parts=true. Essentially, we're asked to provide flags for reading
    keyparts whose datatype is not yet known.

    We walk around this problem by using check_keyread_allowed(), which uses
    table_share object and is careful not to step on unitialized data.

    When we get a call with all_parts=true, we try to analyze all parts but
    ignore those that have key_part->field==nullptr (these are not initialized
    yet).
*/

ulong ha_rocksdb::index_flags(bool &pk_can_be_decoded,
                              const TABLE_SHARE *table_share, uint inx,
                              uint part, bool all_parts) {
  DBUG_ENTER_FUNC();

  ulong base_flags = HA_READ_NEXT |  // doesn't seem to be used
                     HA_READ_ORDER | HA_READ_RANGE | HA_READ_PREV;


  bool index_read_only = check_keyread_allowed(pk_can_be_decoded, table_share,
                                               inx, part, all_parts);

  // Following code borrowed from ha_innobase::index_flags(), see there also
  // TODO: debug why rocksdb returns wrong result.
  constexpr const char *dd_space_name = "mysql";
  const char *dbname = table_share->db.str;
  bool access_dd_table = (dbname && strstr(dbname, dd_space_name) != nullptr &&
                          strlen(dbname) == 5);

  if (index_read_only) {
    base_flags |= HA_KEYREAD_ONLY;
  }

  if (inx == table_share->primary_key) {
    /*
      Index-only reads on primary key are the same as table scan for us. Still,
      we need to explicitly "allow" them, otherwise SQL layer will miss some
      plans.
    */
    base_flags |= HA_KEYREAD_ONLY;
  } else if (index_read_only && !access_dd_table) {
    /*
      We can Index Condition Pushdown any key except the primary. With primary
      key, we get (pk, record) pair immediately, there is no place to put the
      ICP check.
    */
    base_flags |= HA_DO_INDEX_COND_PUSHDOWN;
  }

  DBUG_RETURN(base_flags);
}

ulong ha_rocksdb::index_flags(uint inx, uint part, bool all_parts) const {
  return index_flags(m_pk_can_be_decoded, table_share, inx, part, all_parts);
}

/**
  @brief
  Like get_row_by_rowid();
  Get row from preload raw records kv cache.
*/
int ha_rocksdb::get_row_from_raw_rec_cache(uint keyno, uchar *const buf) {
  int ret = TDSQL_EXIT_FAILURE;

  if (m_raw_rec_cache_it == nullptr) {
    m_raw_rec_cache_it = CreateRawRecKvCacheIterator(m_td_scan_it, table,
                                                     m_pk_descr,
                                                     m_key_descr_arr[keyno]);
    assert(m_raw_rec_cache_it != nullptr);
    if (unlikely(m_raw_rec_cache_it == nullptr)) {
      my_error(ER_SQLENGINE_SYSTEM, MYF(0), "Unable to allocate memory");
      return ret;
    }
    set_write_fence();
  }

  const tdstore::KeyValue* raw_rec = m_raw_rec_cache_it->Get();

  if (raw_rec != nullptr) {
    assert(!raw_rec->key().empty());
    assert(!raw_rec->value().empty());
    ret = tdsql_convert_record_from_storage_format(
              raw_rec->key(), raw_rec->value(), buf);
  } else {
    my_error(ER_SQLENGINE_SYSTEM, MYF(0), "Can not get preload kv cache");
  }

  return ret;
}

/*
  ha_rocksdb::read_range_first overrides handler::read_range_first.
  The only difference from handler::read_range_first is that
  ha_rocksdb::read_range_first passes end_key to
  ha_rocksdb::index_read_map_impl function.

  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::read_range_first(const key_range *const start_key,
                                 const key_range *const end_key,
                                 bool eq_range_arg,
                                 bool sorted [[maybe_unused]]) {

  int result;

  eq_range = eq_range_arg;
  set_end_range(end_key, RANGE_SCAN_ASC);

  range_key_part = table->key_info[active_index].key_part;

  if (!start_key)  // Read first record
    result = ha_index_first(table->record[0]);
  else {
    if (is_using_prohibited_gap_locks(table,
            is_using_full_unique_key(active_index, start_key->keypart_map,
                                     start_key->flag))) {
      return HA_ERR_LOCK_DEADLOCK;
    }

    result =
        index_read_map_impl(table->record[0], start_key->key,
                            start_key->keypart_map, start_key->flag, end_key);
  }

  if (only_init_iterator_) return HA_EXIT_SUCCESS;

  if (result)
    return (result == HA_ERR_KEY_NOT_FOUND) ? HA_ERR_END_OF_FILE : result;

  if (compare_key(end_range) <= 0) {
    return HA_EXIT_SUCCESS;
  } else {
    /*
      The last read row does not fall in the range. So request
      storage engine to release row lock if possible.
    */
    // unlock_row();
    return HA_ERR_END_OF_FILE;
  }
}

/**
   @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_read_map(uchar *const buf, const uchar *const key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag) {

  return index_read_map_impl(buf, key, keypart_map, find_flag, nullptr);
}

// direction =1 --> set m_scan_it_upper_bound_slice
// direction =0 --> set m_scan_it_lower_bound_slice
int ha_rocksdb::SetupEndRange(const Rdb_key_def &kd, const key_range *end_key,
                              bool direction) {
  if (direction) {
    if (end_key) {
      // and a <[=]20
      uint upper_packed_size =
          kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_upper_bound,
                              end_key->key, end_key->keypart_map);

      if (end_key->flag == HA_READ_BEFORE_KEY) {
        // a<20,do nothing
      } else if (end_key->flag == HA_READ_AFTER_KEY) {
        // a<=20
        kd.successor(m_scan_it_upper_bound, upper_packed_size);
      } else {
        assert(0);
        LogError("not support flag:%d", end_key->flag);
        return 1;
      }

      m_scan_it_upper_bound_slice = rocksdb::Slice(
          (const char *)m_scan_it_upper_bound, upper_packed_size);
    } else {
      // end:next_index_id)
      memcpy(m_scan_it_upper_bound, kd.get_index_number_storage_form(),
             Rdb_key_def::INDEX_NUMBER_SIZE);
      kd.successor(m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
      m_scan_it_upper_bound_slice = rocksdb::Slice(
          (const char *)m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
    }
  } else {
    if (end_key) {
      // a>[=]20
      uint lower_packed_size =
          kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_lower_bound,
                              end_key->key, end_key->keypart_map);

      if (end_key->flag == HA_READ_KEY_OR_NEXT ||
          end_key->flag == HA_READ_KEY_EXACT) {
        // a>=20,do nothing
      } else if (end_key->flag == HA_READ_AFTER_KEY) {
        // a>20
        kd.successor(m_scan_it_lower_bound, lower_packed_size);
      } else {
        assert(0);
        LogError("not support flag:%d", end_key->flag);
        return 1;
      }

      m_scan_it_lower_bound_slice = rocksdb::Slice(
          (const char *)m_scan_it_lower_bound, lower_packed_size);
    } else {
      // start:[index_id
      m_scan_it_lower_bound_slice = rocksdb::Slice(
          reinterpret_cast<const char *>(kd.get_index_number_storage_form()),
          Rdb_key_def::INDEX_NUMBER_SIZE);
    }
  }
  return 0;
}

/**
 in:packed_size: size of m_sk_packed_tuple, which is the pack result from key
 in:find_flag,end_key
 out:m_scan_it_upper_bound_slice and m_scan_it_lower_bound_slice
 out:move_forward

 @return
 HA_EXIT_SUCCESS:OK
 HA_ERR_END_OF_FILE: NO RECORD
 HA_ERR_ROCKSDB_CORRUPT_DATA: UNSUPPORT FLAG
*/
int ha_rocksdb::SetupBoundSlice(const Rdb_key_def &kd, uint packed_size,
                                enum ha_rkey_function find_flag,
                                const key_range *end_key, bool *move_forward) {

  DBUG_EXECUTE_IF("unsupport_flag_in_setup_bond", { return HA_ERR_ROCKSDB_CORRUPT_DATA; });

  *move_forward = true;
  switch (find_flag) {
    case HA_READ_PREFIX_LAST: {
      //
      // select a,b,max(c) from t1 group by a,b; //the last row use a,b value
      //
      // select * from t1 where a=10 order by b desc;
      // start:[10,
      // end:10+1)
      *move_forward = false;
      [[fallthrough]];
    }
    case HA_READ_KEY_EXACT: {
      // select * from t1 where a=10;
      // start:[10,
      m_scan_it_lower_bound_slice =
          rocksdb::Slice((const char *)m_sk_packed_tuple, packed_size);

      // end:10+1)
      memcpy(m_scan_it_upper_bound, m_sk_packed_tuple, packed_size);
      kd.successor(m_scan_it_upper_bound, packed_size);
      m_scan_it_upper_bound_slice =
          rocksdb::Slice((const char *)m_scan_it_upper_bound, packed_size);
      break;
    }
    case HA_READ_AFTER_KEY: {
      // select * from t1 where a>10  [and a<20]
      // start:[10+1
      kd.successor(m_sk_packed_tuple, packed_size);
      [[fallthrough]];
    }
    case HA_READ_KEY_OR_NEXT: {
      // select * from t2 where a>=11 [and a<20]
      // start:[11
      m_scan_it_lower_bound_slice =
          rocksdb::Slice((const char *)m_sk_packed_tuple, packed_size);
      if (SetupEndRange(kd, end_key, true)) return HA_ERR_ROCKSDB_CORRUPT_DATA;

      break;
    }
    case HA_READ_PREFIX_LAST_OR_PREV: {
      // select max(a) from t1 where a<=10;
      kd.successor(m_sk_packed_tuple, packed_size);
      [[fallthrough]];
    }
    case HA_READ_BEFORE_KEY: {
      // select * from t1 where a>20 and a<100 order by b desc
      //
      // select max(a) from t1 where a<10;
      *move_forward = false;

      // note:end_key relate to lower_bound
      if (SetupEndRange(kd, end_key, false)) return HA_ERR_ROCKSDB_CORRUPT_DATA;

      // end:10)
      m_scan_it_upper_bound_slice =
          rocksdb::Slice((const char *)m_sk_packed_tuple, packed_size);
      break;
    }
    case HA_READ_KEY_OR_PREV:
    case HA_READ_PREFIX:
    default:
      assert(0);
      LogError("not support flag:%d", find_flag);
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
  }

  LogDebug("SetupBoundSlice, start_key %s end_key %s",
           MemoryDumper::ToHex(m_scan_it_lower_bound_slice.data(),
                               m_scan_it_lower_bound_slice.size()).data(),
           MemoryDumper::ToHex(m_scan_it_upper_bound_slice.data(),
                               m_scan_it_upper_bound_slice.size()).data());

  if (m_scan_it_lower_bound_slice.compare(m_scan_it_upper_bound_slice) >= 0) {
    // no record
    return HA_ERR_END_OF_FILE;
  }

  return HA_EXIT_SUCCESS;
}


// get next record from iterator, and parse into buf
int ha_rocksdb::GetAndParseNextRecord(uchar *const buf,int index) {

  if (only_init_iterator_) return HA_EXIT_SUCCESS;

  int rc = HA_EXIT_SUCCESS;

  set_write_fence(m_td_scan_it);
  rc = m_td_scan_it->Next();
  if (is_error_iterator_no_more_rec(rc)) {
    return HA_ERR_KEY_NOT_FOUND;
  } else if (is_error(rc)) {
    return rc;
  }

  /*
    Now get the data for the row into 'buf'.  If we were using a primary key
    then we have all the rows we need.  For a secondary key we now need to
    lookup the primary key.
  */
  if (is_pk(index, table, m_tbl_def)) {
    rc = read_row_from_primary_key(buf);
  } else if (m_td_scan_it->GetPkPreloadPush()) {
    LogDebug("pk preload is enabled,use pk to decode data");
    rc = read_row_from_primary_key(buf);
  } else {
    const Rdb_key_def &kd = *m_key_descr_arr[index];
    rc = read_row_from_secondary_key(buf, kd, 1 /*no use*/);
  }

  update_row_stats(ROWS_READ);

  return rc;
}

/*
   See storage/rocksdb/rocksdb-range-access.txt for description of how MySQL
   index navigation commands are converted into RocksDB lookup commands.

   This function takes end_key as an argument, and it is set on range scan.
   MyRocks needs to decide whether prefix bloom filter can be used or not.
   To decide to use prefix bloom filter or not, calculating equal condition
   length
   is needed. On equal lookups (find_flag == HA_READ_KEY_EXACT), equal
   condition length is the same as rocksdb::Slice.size() of the start key.
   On range scan, equal condition length is MIN(start_key, end_key) of the
   rocksdb::Slice expression.

   @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_read_map_impl(uchar *const buf, const uchar *const key,
                                    key_part_map keypart_map,
                                    enum ha_rkey_function find_flag,
                                    const key_range *end_key) {

  int rc = 0;

  ha_statistic_increment(&System_status_var::ha_read_key_count);
  const Rdb_key_def &kd = *m_key_descr_arr[active_index];
  const uint actual_key_parts = kd.get_key_parts();
  bool using_full_key = is_using_full_key(keypart_map, actual_key_parts);

  /*
    TDSQL: Save key info for index_next_same().
  */
  m_read_key_info.active_index = active_index;
  m_read_key_info.read_key_len = calculate_key_len(table, active_index, keypart_map);

  if (!end_key)
    end_key = end_range;

  /* By default, we don't need the retrieved records to match the prefix */
  m_sk_match_prefix = nullptr;

  // if only_init_iterator_=1, we need the iterator
  if (active_index == table->s->primary_key && find_flag == HA_READ_KEY_EXACT &&
      using_full_key && !only_init_iterator_) {
    /*
      Equality lookup over primary key, using full tuple.
      This is a special case, use DB::Get.
    */

    //single get do not use push
    m_really_push = 0;
    const uint size = kd.pack_index_tuple(table, m_pack_buffer,
                                          m_pk_packed_tuple, key, keypart_map);

    if (use_parallel_scan()) {
      tdsql::PSJobManagement paral_query_job_mgn(
          table->in_use, this, false /*scan*/,
          std::string((char *)m_pk_packed_tuple, size), "");

      paral_query_job_mgn.Initialize();
      if (!parallel_scan_job()) {
        paral_query_job_mgn.AttachAJob();
      }
      if (paral_query_job_mgn.CurrentScanFinished()) {
        return HA_ERR_KEY_NOT_FOUND;
      }
    }

    rc = get_row_by_rowid(buf, m_pk_packed_tuple, size, false, true);

    if (!rc) {
      update_row_stats(ROWS_READ);
      // we can not push the cond,so we do here ourself
      if (tdstore_pushed_cond_) {
        if (!tdstore_pushed_cond_->val_int()) {
          LogDebug("record do not match tdstore_pushed_cond_,return HA_ERR_KEY_NOT_FOUND");
          rc = HA_ERR_KEY_NOT_FOUND;
        }
      }
    }
    return rc;
  }

  /*
    Unique secondary index performs lookups without the extended key fields
  */
  uint packed_size;
  if (active_index != table->s->primary_key &&
      table->key_info[active_index].flags & HA_NOSAME &&
      find_flag == HA_READ_KEY_EXACT && using_full_key) {
    key_part_map tmp_map = (key_part_map(1) << table->key_info[active_index]
                                                   .user_defined_key_parts) -
                           1;
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, tmp_map);
    if (table->key_info[active_index].user_defined_key_parts !=
        kd.get_key_parts())
      using_full_key = false;
  } else {
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, keypart_map);
  }

  if ((pushed_idx_cond && pushed_idx_cond_keyno == active_index) &&
      (find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_PREFIX_LAST)) {
    /*
      We are doing a point index lookup, and ICP is enabled. It is possible
      that this call will be followed by ha_rocksdb->index_next_same() call.

      Do what InnoDB does: save the lookup tuple now. We will need it in
      index_next_same/find_icp_matching_index_rec in order to stop scanning
      as soon as index record doesn't match the lookup tuple.

      When not using ICP, handler::index_next_same() will make sure that rows
      that don't match the lookup prefix are not returned.
      row matches the lookup prefix.
    */
    m_sk_match_prefix = m_sk_match_prefix_buf;
    m_sk_match_length = packed_size;
    memcpy(m_sk_match_prefix, m_sk_packed_tuple, packed_size);
  }

  bool move_forward = true;
  rc = SetupBoundSlice(kd, packed_size, find_flag, end_key, &move_forward);

  if (rc)
    return rc;

  tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use, rc);
  if (rc != HA_EXIT_SUCCESS)
    return rc;
  assert(!IS_NULLPTR(tx));

  rocksdb::Slice index_prefix(reinterpret_cast<const char*>(kd.get_index_number_storage_form()),
                              Rdb_key_def::INDEX_NUMBER_SIZE);
  assert(kd.get_keyno() == active_index);
  if ((rc = init_tdsql_iterator(const_cast<tdsql::Transaction*&>(tx),
                                index_prefix, m_scan_it_lower_bound_slice, m_scan_it_upper_bound_slice,
                                move_forward, false,
                                get_key_tindex_id(&kd),
                                active_index)) != HA_EXIT_SUCCESS) {
    if (is_error_iterator_no_more_rec(rc)) {
      return HA_ERR_END_OF_FILE;
    }
    return rc;
  }

  rc = GetAndParseNextRecord(buf,active_index);

  return rc;
}

int ha_rocksdb::index_read_idx_map(uchar *buf, uint index, const uchar *key,
                                key_part_map keypart_map,
                                enum ha_rkey_function find_flag) {
  int error = 0, error1 = 0;
  int save_inited = inited;
  if (save_inited == handler::NONE) {
    error = index_init(index, 0);
  }
  if (!error) {
    error = index_read_map(buf, key, keypart_map, find_flag);
    if (save_inited == handler::NONE) {
      error1 = index_end();
    }
  }
  return error ? error : error1;
}


/*
  @brief
  Scan the secondary index until we find an index record that satisfies ICP

  @param move_forward   true  <=> move m_scan_it forward
                        false <=> move m_scan_it backward
  @param buf            Record buffer (must be the same buffer that
                        pushed index condition points to, in practice
                        it is table->record[0])

  @detail
  Move the current iterator m_scan_it until we get an index tuple that
  satisfies the pushed Index Condition.
  (if there is no pushed index condition, return right away)

  @return
    0     - Index tuple satisfies ICP, can do index read.
    other - error code
*/

int ha_rocksdb::find_icp_matching_index_rec(const bool &,
                                            uchar *const buf) {
  assert(buf != nullptr);

  if (pushed_idx_cond && pushed_idx_cond_keyno == active_index) {
    const Rdb_key_def &kd = *m_key_descr_arr[active_index];

    while (1) {

      const rocksdb::Slice rkey = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->key());

      if (!kd.covers_key(rkey)) {
        return HA_ERR_END_OF_FILE;
      }

      if (m_sk_match_prefix) {
        const rocksdb::Slice prefix((const char *)m_sk_match_prefix,
                                    m_sk_match_length);
        if (!kd.value_matches_prefix(rkey, prefix)) {
          return HA_ERR_END_OF_FILE;
        }
      }

      const rocksdb::Slice value = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->value());

      int err = kd.unpack_record(table, buf, &rkey, &value,
                                 0);
      if (err != HA_EXIT_SUCCESS) {
        return err;
      }

      const enum icp_result icp_status = check_index_cond();
      if (icp_status == ICP_NO_MATCH) {
        // Error should be returned in case of failure.
        set_write_fence(m_td_scan_it);
        err = m_td_scan_it->Next();
        if (is_error_iterator_no_more_rec(err)) {
          return HA_ERR_END_OF_FILE;
        } else if (is_error(err)) {
          return err;
        }
        /* TDSQL end */
        continue; /* Get the next (or prev) index tuple */
      } else if (icp_status == ICP_OUT_OF_RANGE) {
        /* We have walked out of range we are scanning */
        return HA_ERR_END_OF_FILE;
      } else /* icp_status == ICP_MATCH */
      {
        /* Index Condition is satisfied. We have rc==0, proceed to fetch the
         * row. */
        break;
      }
      /*
        TODO: should we have this here, or RockDB handles this internally?
        if (my_core::thd_killed(current_thd))
        {
          rc= HA_ERR_INTERNAL_ERROR; // doesn't matter
          break;
        }
      */
    }
  }
  return HA_EXIT_SUCCESS;
}

/**
   @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_read_last_map(uchar *const buf, const uchar *const key,
                                    key_part_map keypart_map) {

  return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
}

/**
   @return
    HA_ADMIN_OK      OK
    other            HA_ADMIN error code
*/
int ha_rocksdb::check(THD *const, HA_CHECK_OPT *const) {
  return HA_ADMIN_OK;
}

int ha_rocksdb::get_for_update(
    tdsql::Transaction *const tx,
    const rocksdb::Slice &key, rocksdb::PinnableSlice *const value) {
  assert(m_lock_rows != RDB_LOCK_NONE);

  set_write_fence();
  return rpc_get_record(tx, key, value);
}

/*
  Given a rowid (i.e. packed PK) as a parameter, get the record.

  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/

int ha_rocksdb::get_row_by_rowid(uchar *const buf, const char *const rowid,
                                 const uint rowid_size,
                                 const bool skip_ttl_check [[maybe_unused]],
                                 const bool pk_point_select) {

  assert(buf != nullptr);
  assert(rowid != nullptr);
  assert(table != nullptr);

  int rc = HA_EXIT_SUCCESS;

  rocksdb::Slice key_slice(rowid, rowid_size);

  tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use, rc);
  if (rc != HA_EXIT_SUCCESS)
    return rc;
  assert(!IS_NULLPTR(tx));

  bool found = false;
  // it is not a bug, value_slice is constructed by non-arg constructor,
  // its internal buffer will be released when it is out of scope.
  rocksdb::PinnableSlice value_slice;
  set_write_fence();
  if (m_lock_rows == RDB_LOCK_NONE) {
    // pk point loopup
    rc = rpc_get_record(tx, key_slice, &value_slice, pk_point_select);
    if (rc == HA_EXIT_SUCCESS) {
      found = true;
      global_stats.queries[QUERIES_POINT].inc();
    }
    else {
      found = false;
    }
  }
  else {
    rc = rpc_get_record(tx, key_slice, &value_slice);
    if (rc == HA_EXIT_SUCCESS) {
      found = true;
    }
    else {
      found = false;
    }
  }

  if (found)
  {
    m_retrieved_record.Reset();
    m_retrieved_record.PinSelf(value_slice);

    m_last_rowkey.copy((const char *)rowid, rowid_size, &my_charset_bin);
    rc = convert_record_from_storage_format(&key_slice, buf);
  }
  // else {
  //   rc = HA_ERR_KEY_NOT_FOUND;
  // }

  return rc;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_next(uchar *const buf) {

  ha_statistic_increment(&System_status_var::ha_read_next_count);

  int rc = index_next_with_direction(buf, true); // TDSQL: in index_next, move_forward should be true.
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  return rc;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_prev(uchar *const buf) {

  ha_statistic_increment(&System_status_var::ha_read_prev_count);

  int rc = index_next_with_direction(buf, false); // TDSQL: in index_prev, move_forward should be false.
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  return rc;
}

/**
   @brief
    Reads the next row matching to the buf value given as the parameter.
   @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/

int ha_rocksdb::index_next_same(uchar *buf, const uchar *key, uint) {
  DBUG_ENTER("index_next_same");
  /* Must be set in index_read_map_impl(). */
  assert(m_read_key_info.active_index == active_index);
  assert(m_read_key_info.read_key_len > 0);
  /*
    keylen passed as parameter maybe not correct,
    use m_read_key_info.read_key_len instead.
    See: MergeRequest 110.
  */
  int rc = handler::index_next_same(buf, key, m_read_key_info.read_key_len);

  DBUG_RETURN(rc);
}

void ha_rocksdb::SetPkPreloadPush(int keyno) {
  if (!current_thd->optimizer_switch_flag(OPTIMIZER_PK_PRELOAD_PUSHDOWN) ||
      UseOldScan(current_thd)) {
    pk_preload_push_ = false;
    return;
  }

  // when enable pk_preload,we will not call check_index_cond
  // so do not enable if there is icp condition
  if (icp_cond_remainder_) {
    pk_preload_push_ = false;
    return;
  }

  bool covered_lookup = table->covering_keys.is_set(keyno) &&
                        m_key_descr_arr[keyno]->can_cover_lookup();
  if (covered_lookup)
    pk_preload_push_ = false;
  else
    pk_preload_push_ = true;
}

void ha_rocksdb::SetProjectionPush() {

  m_projection = 0;

  if (!tdsql_max_projection_pct) return;

  int read_set_num = 0;

  for (uint i = 0; i < table->s->fields; i++) {
    const bool field_requested =
        m_lock_rows == RDB_LOCK_WRITE || bitmap_is_clear_all(table->read_set) ||
        bitmap_is_set(table->read_set, table->field[i]->field_index());
    if (field_requested) {
      read_set_num++;
    }
  }

  if (read_set_num * 100 / table->s->fields <= tdsql_max_projection_pct)
    m_projection = 1;
}

void PrintCondition(const char *info, Item *cond) {
  if (log_error_verbosity < 4) return;

  if (!cond) {
    LogDebug("%s null", info);
    return;
  }

  String str;
  cond->print(current_thd, &str, QT_ORDINARY);
  str.append('\0');

  LogDebug("%s:%s", info, str.c_ptr_safe());
}

// count push
// limit offset push
void ha_rocksdb::SingleSelectPush(const QEP_TAB *qep_tab, int index) {
  THD *const thd = table->in_use;

  if (!tdsql::IsSelectSingleTable(thd)) {
    return ;
  }

  if (is_pk(index, table, m_tbl_def))
    SetProjectionPush();
  else
    SetPkPreloadPush(index);

  LogDebug("set projection:%d,pk_preload:%d", m_projection, pk_preload_push_);

  // all cond can push to tdstore or no cond for current table
  if (!m_all_pushed) return;

  // count with cond, count without cond is optimized in
  // optimize_aggregated_query already
  Item *item_args0 = nullptr;
  if (tdsql::TestCountCondPushDown(qep_tab, &item_args0)) {
    // count(f1), f1 can be null, then need to check f1 can unpack from index
    bool can_unpack = false;
    if (item_args0) {
      Tdstore_cond_traverse_context ctx(table, index);
      ctx.is_hidden_pk = is_hidden_pk(index, table, m_tbl_def);
      if (CanPushToTdstore(item_args0, &ctx)) {
        Item *notnull = new Item_func_isnotnull(item_args0);
        notnull->quick_fix_field();
        notnull->update_used_tables();
        and_conditions(&tdstore_pushed_cond_, notnull);
        LogDebug("field in count can be null,and can unpack from index");
        can_unpack = true;
      } else {
        can_unpack = false;
        LogDebug("field in count can be null,but can not unpack from index");
      }
    } else {
      can_unpack = true;
      LogDebug("field in count can not be null");
    }

    if (can_unpack) {
      PrintCondition("Enable count cond pushdown", tdstore_pushed_cond_);
      JOIN *const join = qep_tab->join();
      join->select_count = true;
    }
  }
  // limit offset push
  else if (tdsql::TestLimitOffsetCondPushDown(qep_tab, index)) {
    tdsql::InitLimitOffsetCondPushDown(qep_tab, index);
  }
}

void ha_rocksdb::IndexMergePush(int index) {
  const QEP_TAB *qep_tab = table->reginfo.qep_tab;

  if (!pushed_cond) return;

  if (!qep_tab || qep_tab->type() != JT_INDEX_MERGE) return;

  if (!CanPushDown(table, m_tbl_def, index)) return;

  SetCondPush(pushed_cond, nullptr, index);
}

// all push relate optimize should come here
// sql layer have fully set QEP_TABs
//
// here we only set tdstore_pushed_cond_ and change condition
// pb will create in BuildPushDownPb before create rpc_cntl
int ha_rocksdb::engine_push(AQP::Table_access *table_aqp) {
  THD *const thd = table->in_use;

  if (!thd->optimizer_switch_flag(OPTIMIZER_SWITCH_ENGINE_CONDITION_PUSHDOWN))
    return 0;

  if (!ReadWithoutTrans(thd)) return 0;

  if (thd->locked_tables_mode) {
    /* Under LOCK TABLES we must not use push. */
    /* libsql will also call open_table for shared mdl, lead to deadlock*/
    return 0;
  }

  const QEP_TAB *qep_tab = table_aqp->get_qep_tab();
  Item *table_cond = table_aqp->get_condition();

  int index = GetPushIndexNo(qep_tab);
  if (!CanPushDown(table, m_tbl_def, index)) return 0;

  // see and split which part of cond can push to tdstore
  SetCondPush(table_cond, pushed_idx_cond, index);

  // for single table select,try more pushdown method
  SingleSelectPush(qep_tab, index);

  // only check the condition not pushdown
  if (tdstore_pushed_cond_) {
    pushed_idx_cond = icp_cond_remainder_;
    table_aqp->set_condition(table_cond_remainder_);
  }

  return 0;
}

bool ha_rocksdb::tdsql_clone_pushed(const handler *from,
                                    Item_clone_context *context,
                                    bool reset_limit_offset) {
  auto *ha = down_cast<const ha_rocksdb *>(from);

  assert(!ha->tdstore_pushed_cond_ ||
         ha->tdstore_pushed_cond_->parallel_safe() == Item_parallel_safe::Safe);

  if (ha->tdstore_pushed_cond_ &&
      !(tdstore_pushed_cond_ = ha->tdstore_pushed_cond_->clone(context)))
    return true;

  tdstore_pushed_cond_idx_ = ha->tdstore_pushed_cond_idx_;

  // Clone Limit OFFSET push down if it is enabled

  if (!ha->limit_offset_cond_pushdown.enabled()) return false;

  limit_offset_cond_pushdown = ha->limit_offset_cond_pushdown;
  if (reset_limit_offset && limit_offset_cond_pushdown.offset_num() != 0) {
    limit_offset_cond_pushdown.set_limit_num(
        ha->limit_offset_cond_pushdown.limit_num() +
        ha->limit_offset_cond_pushdown.offset_num());
    limit_offset_cond_pushdown.set_offset_num(0);
  }

  // pushed_idx_cond cloned in server layer
  return false;
}

int ha_rocksdb::index_next_with_direction(uchar *const buf, bool) {

  int rc;

  if (tdsql::LimitCondMatch(this)) {
    return HA_ERR_END_OF_FILE;
  }

  while (true) {
    rc = GetAndParseNextRecord(buf,active_index);
    if (rc == HA_ERR_KEY_NOT_FOUND) {
      rc = HA_ERR_END_OF_FILE;
    }

    if (rc || !tdsql::EnableLimitCondPushDown(this)) break;

    assert(!rc && tdsql::EnableLimitCondPushDown(this));
    if (pushed_cond->val_int()) {
      // pushed cond match => break
      break;
    }
    // pushed cond not match => continue
  }

  if (!rc && tdsql::EnableLimitCondPushDown(this)) {
    limit_cond_pushdown->IncCurrNum();
  }

  return rc;
}

// direction =1 --> call from index_first
// direction =0 --> call from index_last
int ha_rocksdb::IndexFirstLast(uchar *const buf, bool direction) {
  assert(buf != nullptr);

  int rc = HA_EXIT_SUCCESS;
  const Rdb_key_def &kd = *m_key_descr_arr[active_index];

  int ret = SetupEndRange(kd, end_range, direction);
  if (ret) return ret;

  if (direction) {
    // start:[index_id
    m_scan_it_lower_bound_slice = rocksdb::Slice(
        reinterpret_cast<const char *>(kd.get_index_number_storage_form()),
        Rdb_key_def::INDEX_NUMBER_SIZE);
  } else {
    // end:next_index_id)
    memcpy(m_scan_it_upper_bound, kd.get_index_number_storage_form(),
           Rdb_key_def::INDEX_NUMBER_SIZE);
    kd.successor(m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
    m_scan_it_upper_bound_slice = rocksdb::Slice(
        (const char *)m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
  }

  rocksdb::Slice index_prefix(
      reinterpret_cast<const char *>(kd.get_index_number_storage_form()),
      Rdb_key_def::INDEX_NUMBER_SIZE);

  tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use, rc);

  if ((rc = init_tdsql_iterator(
           const_cast<tdsql::Transaction *&>(tx), index_prefix,
           m_scan_it_lower_bound_slice, m_scan_it_upper_bound_slice, direction,
           false, get_key_tindex_id(&kd), active_index)) != HA_EXIT_SUCCESS) {
    return rc;
  }

  rc = index_next_with_direction(buf, direction);

  return rc;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_first(uchar *const buf) {

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&System_status_var::ha_read_first_count);
  int rc = IndexFirstLast(buf,true);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  return rc;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_last(uchar *const buf) {

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&System_status_var::ha_read_last_count);
  int rc = IndexFirstLast(buf,false);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  return rc;
}

void ha_rocksdb::unlock_row() {
  DBUG_ENTER_FUNC();
/*
  if (m_lock_rows != RDB_LOCK_NONE) {
    tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use);
    tx->release_lock(m_pk_descr->get_cf(),
                     std::string(m_last_rowkey.ptr(), m_last_rowkey.length()));
  }
*/
  DBUG_VOID_RETURN;
}

/*
  Returning true if SingleDelete can be used.
  - Secondary Indexes can always use SingleDelete.
  - If the index is PRIMARY KEY, and if all of the columns of the table
    are covered by the PRIMARY KEY, SingleDelete can be used.
*/
bool ha_rocksdb::can_use_single_delete(const uint &index) const {
  return (index != pk_index(table, m_tbl_def) ||
          (!has_hidden_pk(table) &&
           table->key_info[index].actual_key_parts == table->s->fields));
}

bool ha_rocksdb::skip_unique_check() const {
  /*
    We want to skip unique checks if:
      1) bulk_load is on
      2) the user set unique_checks option to 0, and the table does not have
         any indexes. If the table has secondary keys, then those might becomes
         inconsisted/corrupted
  */
  return THDVAR(table->in_use, bulk_load) ||
         (my_core::thd_test_options(table->in_use,
                                    OPTION_RELAXED_UNIQUE_CHECKS) &&
          m_tbl_def->m_key_count == 1) ||
         (table->correlate_src_table && tdsql::ddl::GetDDLJob(table->in_use));
}

bool ha_rocksdb::commit_in_the_middle(THD *thd) {
  return THDVAR(table->in_use, bulk_load) &&
         thd->query_plan.get_command() == enum_sql_command::SQLCOM_LOAD;
}

/* Returns true if given index number is usable*/
bool ha_rocksdb::index_is_usable(
    tdsql::Transaction *tx, const uint index, const TABLE *const table_arg,
    const std::shared_ptr<Rdb_tbl_def> &tbl_def_arg) {
  assert(table_arg != nullptr);
  assert(table_arg->s != nullptr);
  assert(tbl_def_arg != nullptr);

  uint64_t create_ts = 0;
  Schema_Status schema_status;
  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    create_ts = table_arg->s->create_ts;
    schema_status = (Schema_Status)table_arg->s->schema_status;
  } else {
    create_ts = table_arg->s->key_info[index].create_ts;
    if (is_pk(index, table_arg, tbl_def_arg)) {
      schema_status = (Schema_Status)table_arg->s->schema_status;
    } else {
      schema_status = (Schema_Status)table_arg->key_info[index].schema_status;
    }
  }

  DBUG_EXECUTE_IF("table_or_index_status_unusable", {
    // Don't block system tables.
    if (!tdsql::IsSystemTable(table_arg->s->db.str, table_arg->s->table_name.str)) {
      schema_status = SS_NO_READ_WRITE;
    }
  });

  // TODO: See https://git.code.oa.com/tdsql3.0/SQLEngine/issues/194
  // assert(!IsUnReadable(schema_status));
  // Do not need to check readable during DDL execution
  if (!tdsql::ddl::GetDDLJob(table->in_use) &&
      !table->correlate_src_table &&
      IsUnReadable(schema_status)) {
    LogError("table '%s.%s' -> key: <idx: %d, name: %s> schema_status is %s, can't be used to read",
      table_arg->s->db.str, table_arg->s->table_name.str, index, get_key_name(index, table_arg, m_tbl_def),
      tdsql::ddl::print_schema_status(schema_status).c_str());
    char err_msg[60];
    snprintf(err_msg, sizeof(err_msg), "current schema status is %s",
             tdsql::ddl::print_schema_status(schema_status).c_str());
    my_error(ER_TABLE_DEF_UNUSABLE, MYF(0), table_arg->s->db.str, table_arg->s->table_name.str, err_msg);
    return false;
  }

  if (tx) {
    LogDebug("current trans_id: %lu, trans ts: %lu, table %s.%s create_ts is %lu, "
             "index <idx: %u, name: %s> create_ts is %lu, query: '%s'",
          tx->trans_id(), tx->begin_ts(), table_arg->s->db.str,
          table_arg->s->table_name.str, table_arg->s->create_ts, index,
          get_key_name(index, table_arg, m_tbl_def),
          create_ts, tx->thd()->query().str);

    DBUG_EXECUTE_IF("table_or_index_create_ts_unusable", {
      // Don't block system tables.
      if (!tdsql::IsSystemTable(table_arg->s->db.str, table_arg->s->table_name.str)) {
        create_ts = (~(ulonglong)0); //ULONGLONG_MAX
      }
    });

    if (tx->begin_ts() > 0 && tx->begin_ts() < create_ts) {
      LogError("current trans_id: %lu, trans ts: %lu, table %s.%s create_ts is %lu, "
               "index <idx: %u, name: %s> create_ts is %lu, unusable, query: '%s'",
          tx->trans_id(), tx->begin_ts(), table_arg->s->db.str,
          table_arg->s->table_name.str, table_arg->s->create_ts, index,
          get_key_name(index, table_arg, m_tbl_def),
          create_ts, tx->thd()->query().str);
      my_error(ER_TABLE_DEF_UNUSABLE, MYF(0), table_arg->s->db.str, table_arg->s->table_name.str,
               "begin timestamp of current transaction is too old");
      return false;
    }
  }

  return true;
}

/* Returns index of primary key */
uint ha_rocksdb::pk_index(const TABLE *const table_arg,
                          const std::shared_ptr<Rdb_tbl_def> &tbl_def_arg) {
  assert(tbl_def_arg);
  return tbl_def_arg->pk_index(table_arg);
}

uint ha_rocksdb::max_supported_key_part_length(
    HA_CREATE_INFO *create_info MY_ATTRIBUTE((__unused__))) const {
  DBUG_ENTER_FUNC();
  DBUG_RETURN(rocksdb_large_prefix ? MAX_INDEX_COL_LEN_LARGE
                                   : MAX_INDEX_COL_LEN_SMALL);
}

const char *ha_rocksdb::get_key_comment(
    const uint index, const TABLE *const table_arg,
    const std::shared_ptr<Rdb_tbl_def> &tbl_def_arg) {
  assert(table_arg != nullptr);
  assert(tbl_def_arg != nullptr);

  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    return nullptr;
  }

  assert(table_arg->key_info != nullptr);

  return table_arg->key_info[index].comment.str;
}

const std::string ha_rocksdb::get_table_comment(const TABLE *const table_arg) {
  assert(table_arg != nullptr);
  assert(table_arg->s != nullptr);

  return table_arg->s->comment.str;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::write_row(uchar *const buf) {

  assert(buf != nullptr);
  assert(buf == table->record[0]);
  assert(m_lock_rows == RDB_LOCK_WRITE);

  ha_statistic_increment(&System_status_var::ha_write_count);

  int rv = 0;

  bool need_auto_incr = false;

  if (table->next_number_field && buf == table->record[0]) {
    need_auto_incr = true;
  }

  ulonglong orig_next_number  = 0;
  if (need_auto_incr) {
    orig_next_number = table->next_number_field->val_int();
  }

  int retry_times = 10;
  for (int i = 0; i <= retry_times; ++i) {
    /*
      Note: "buf == table->record[0]" is copied from innodb. I am not aware of
      any use cases where this condition is not true.
    */
    if (need_auto_incr) {
      int err;
      if ((err = update_auto_increment())) {
        return err;
      }
    }

    rv = update_write_row(nullptr, buf, skip_unique_check());

    if (i < retry_times &&
        rv == HA_ERR_FOUND_DUPP_KEY && need_auto_incr && orig_next_number == 0
        && next_insert_id <= table->next_number_field->get_max_int_value() // no overflow
        && !(table->autoinc_field_has_explicit_non_null_value
             && table->in_use->variables.sql_mode & MODE_NO_AUTO_VALUE_ON_ZERO)) {
      /*
        BUG#80718447:
        create table t1(id int primary key auto_increment);
        session1
        insert into t1 values(0)

        session2
        insert into t1 values(2) # used auto increment cached in session1

        session1
        insert into t1 values(0) # duplicate key error

        FIX:
        Get auto increment from mc again
      */
      LogWarn("There is a duplicate error on auto inc value: %lld, index_id: %u, table: %s, "
          "get auto inc again and retry",
          table->next_number_field->val_int(), m_autoinc_generator.index_id(),
          table->to_string().c_str());
      table->next_number_field->store(0, true);
      rv = m_autoinc_generator.generate_next_batch();
      if (is_error(rv)) {
        break;
      }
    } else {
      break;
    }
  }

  if (rv == 0) {
    m_table_handler->AddUpdateCount(1);
    update_row_stats(ROWS_INSERTED);
  }

  if (rv == HA_EXIT_SUCCESS && table->correlate_option
      && unlikely(table->correlate_option->table())) {
    rv = table->correlate_option->WriteRow(buf);

    if (rv) rv = HA_ERR_ROCKSDB_CORRELATE_TABLE;
  }

  return rv;
}

/**
  Constructing m_last_rowkey (MyRocks key expression) from
  before_update|delete image (MySQL row expression).
  m_last_rowkey is normally set during lookup phase, such as
  rnd_next_with_direction() and rnd_pos(). With Read Free Replication,
  these read functions are skipped and update_rows(), delete_rows() are
  called without setting m_last_rowkey. This function sets m_last_rowkey
  for Read Free Replication.
*/
void ha_rocksdb::set_last_rowkey(const uchar *const old_data) {
  if (old_data && table->correlate_src_table) {
    set_last_rowkey_inner(old_data, &m_pk_unpack_info);
  }
#if defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
  if (old_data && use_read_free_rpl()) {
    set_last_rowkey_inner(old_data);
  }
#endif  // defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
}

void ha_rocksdb::set_last_rowkey_inner(const uchar *const old_data,
                                       Rdb_string_writer *pk_unpack_info) {
  if (old_data) {
    const int old_pk_size = m_pk_descr->pack_record(
        table, m_pack_buffer, old_data, m_pk_packed_tuple, pk_unpack_info, false);
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, old_pk_size,
                       &my_charset_bin);
  }
}

int ha_rocksdb::get_pk_for_update(struct update_row_info *const row_info) {
  int size;

  /*
    Get new row key for any insert, and any update where the pk is not hidden.
    Row key for updates with hidden pk is handled below.
  */
  if (!has_hidden_pk(table)) {
    row_info->hidden_pk_id = 0;

    row_info->new_pk_unpack_info = &m_pk_unpack_info;

    size =
        m_pk_descr->pack_record(table, m_pack_buffer, row_info->new_data,
                                m_pk_packed_tuple, row_info->new_pk_unpack_info,
                                false, 0, 0, nullptr, &row_info->ttl_pk_offset);
  } else if (row_info->old_data == nullptr) {
    /* in case of error, row_info->hidden_pk_id is unchanged. */
    int err = get_hidden_pk_val(&(row_info->hidden_pk_id));
    if (err) {
      return err;
    }
    size = m_pk_descr->pack_hidden_pk(row_info->hidden_pk_id, m_pk_packed_tuple);
  } else {
    /*
      If hidden primary key, rowkey for new record will always be the same as
      before
    */
    size = row_info->old_pk_slice.size();
    memcpy(m_pk_packed_tuple, row_info->old_pk_slice.data(), size);
    int err = read_hidden_pk_id_from_rowkey(&row_info->hidden_pk_id);
    if (err) {
      return err;
    }
  }

  row_info->new_pk_slice =
      rocksdb::Slice((const char *)m_pk_packed_tuple, size);

  return HA_EXIT_SUCCESS;
}

int ha_rocksdb::check_and_lock_unique_pk(const uint &key_id [[maybe_unused]],
                                         const struct update_row_info &row_info,
                                         bool *const found,
                                         bool *const pk_changed) {
  assert(found != nullptr);
  assert(pk_changed != nullptr);

  *pk_changed = false;

  /*
    For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
    always require locking.
  */
  if (row_info.old_pk_slice.size() > 0) {
    /*
      If the keys are the same, then no lock is needed
    */
    if (!row_info.new_pk_slice.compare(row_info.old_pk_slice)) {
      *found = false;
      return HA_EXIT_SUCCESS;
    }

    *pk_changed = true;
  }

  /*
    Perform a read to determine if a duplicate entry exists. For primary
    keys, a point lookup will be sufficient.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  /*
    To prevent race conditions like below, it is necessary to
    take a lock for a target row. get_for_update() holds a gap lock if
    target key does not exist, so below conditions should never
    happen.

    1) T1 Get(empty) -> T2 Get(empty) -> T1 Put(insert) -> T1 commit
       -> T2 Put(overwrite) -> T2 commit
    2) T1 Get(empty) -> T1 Put(insert, not committed yet) -> T2 Get(empty)
       -> T2 Put(insert, blocked) -> T1 commit -> T2 commit(overwrite)
  */

  int rc = get_for_update(row_info.tx, row_info.new_pk_slice,
                          &m_retrieved_record);

  if (rc == HA_ERR_KEY_NOT_FOUND) {
    *found = false;
    return HA_EXIT_SUCCESS;
  } else if (rc == HA_EXIT_SUCCESS) {
    *found = true;
  }

  return rc;
}

int ha_rocksdb::check_and_lock_sk(const uint &key_id,
                                  const struct update_row_info &row_info,
                                  bool *const found) {
  assert(found != nullptr);
  *found = false;
  int rc = HA_EXIT_SUCCESS;

  if (IsAbsent((Schema_Status)table->key_info[key_id].schema_status)
      || IsDeleteOnly((Schema_Status)table->key_info[key_id].schema_status)) {
    LogDebug("table '%s.%s' -> index: %s schema_status is %s, skip check unique sk",
        table->s->db.str, table->s->table_name.str, table->key_info[key_id].name,
        tdsql::ddl::print_schema_status(table->key_info[key_id].schema_status).c_str());
    return HA_EXIT_SUCCESS;
  }

  /*
    Can skip checking this key if none of the key fields have changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  KEY *key_info = nullptr;
  uint n_null_fields = 0;
  uint user_defined_key_parts = 1;

  key_info = &table->key_info[key_id];
  user_defined_key_parts = key_info->user_defined_key_parts;
  /*
    If there are no uniqueness requirements, there's no need to obtain a
    lock for this key.
  */
  if (!(key_info->flags & HA_NOSAME)) {
    return HA_EXIT_SUCCESS;
  }

  const Rdb_key_def &kd = *m_key_descr_arr[key_id];

  /*
    Calculate the new key for obtaining the lock

    For unique secondary indexes, the key used for locking does not
    include the extended fields.
  */
  int size =
      kd.pack_record(table, m_pack_buffer, row_info.new_data, m_sk_packed_tuple,
                     nullptr, false, 0, user_defined_key_parts, &n_null_fields);
  if (n_null_fields > 0) {
    /*
      If any fields are marked as NULL this will never match another row as
      to NULL never matches anything else including another NULL.
     */
    return HA_EXIT_SUCCESS;
  }

  const rocksdb::Slice new_slice =
    rocksdb::Slice((const char *)m_sk_packed_tuple, size);

  if (row_info.old_data != nullptr) {
    size = kd.pack_record(table, m_pack_buffer, row_info.old_data,
        m_sk_packed_tuple_old, nullptr, false, 0,
        user_defined_key_parts);
    const rocksdb::Slice old_slice =
      rocksdb::Slice((const char *)m_sk_packed_tuple_old, size);

    /*
       If the old and new keys are the same we're done since we've already taken
       the lock on the old key
       */
    if (!new_slice.compare(old_slice)) {
      return HA_EXIT_SUCCESS;
    }
  }

  const bool all_parts_used = (user_defined_key_parts == kd.get_key_parts());

  /* TDSQL:
     Dont acquire lock on the old key in case of UPDATE under OCC
  */

  /*
    Perform a read to determine if a duplicate entry exists - since this is
    a secondary indexes a range scan is needed.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  /*
    This iterator seems expensive since we need to allocate and free
    memory for each unique index.

    If this needs to be optimized, for keys without NULL fields, the
    extended primary key fields can be migrated to the value portion of the
    key. This enables using Get() instead of Seek() as in the primary key
    case.

    The bloom filter may need to be disabled for this lookup.
  */
  uchar lower_bound_buf[Rdb_key_def::INDEX_NUMBER_SIZE] [[maybe_unused]];
  uchar upper_bound_buf[Rdb_key_def::INDEX_NUMBER_SIZE] [[maybe_unused]];
  rocksdb::Slice lower_bound_slice;
  rocksdb::Slice upper_bound_slice;

/* TDSQL: new_slice is in format of [sk_index_id, sk_value],
   while secondary key's complete format is [sk_index_id, sk_value, pk_value].
   new_slice is a prefix of the complete sk record.
   Tdsql does not yet suppport prefix get_record, use iterator instead. */
/*
  const rocksdb::Status s =
      get_for_update(row_info.tx, kd.get_cf(), new_slice, nullptr);
  if (!s.ok() && !s.IsNotFound()) {
    // return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def);
    return ha_rocksdb::rdb_error_to_mysql(s);
  }
*/

  /* TDSQL: use iterator to see whether new_slice exists. */
  uchar* m_sk_packed_tuple_succ = (uchar*)malloc(size);
  memcpy(m_sk_packed_tuple_succ, m_sk_packed_tuple, size);
  kd.successor(m_sk_packed_tuple_succ, size);
  rocksdb::Slice end_slice((const char*)m_sk_packed_tuple_succ, size);

  rocksdb::Slice index_prefix(reinterpret_cast<const char*>(kd.get_index_number_storage_form()),
                              Rdb_key_def::INDEX_NUMBER_SIZE);

  tdsql::Iterator* sk_iter = nullptr;
  assert(kd.get_keyno() == key_id);

  rc = tdsql::CreateIterator(row_info.tx->thd(), this, 0 /*iterator_id*/,
                             CONVERT_TO_TDSQL_SLICE(index_prefix),
                             CONVERT_TO_TDSQL_SLICE(new_slice),
                             CONVERT_TO_TDSQL_SLICE(end_slice),
                             &sk_iter, true, key_id,
                             m_key_descr_arr[key_id]->get_index_number(),
                             table->key_info[key_id].schema_version,
                             false /*in_parallel_fillback_index*/);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  } else {
    /* found record from [sk_index_id, sk_value] to [sk_index_id, sk_value+1]
       We dont see the possibility that the record in iterator may not be
       the secondary key tuple. In other words, *found=true.
       See what read_key_exact is for, we think invoking read_key_exact is unnecessary.
       However, we invoke it for MyRocks doing so.
    */
    /* read_key_exact to make sure:
       a. iterator record belongs to kd, aka., iterator record's index id prefix equals to kd.index_id.
       b. iterator record has new_slice prefix. */
    *found = !read_key_exact(kd, sk_iter, all_parts_used, new_slice, 0);

    if (!*found) {
      // To solve the problem of uk duplication, we might not found the uk in the case of concurrent transactions
      // Example:
      // create table t1(pk int primary key, uk int, unique key(uk))
      // T1: insert into t1(pk, uk) values(1, 2)
      // T2: insert into t1(pk, uk) values(2, 2)
      // T1, T2 are executed concurrently
      // 1. T1: begin
      // 2. T2: begin
      // 3. T1: scan from [sk_index_id, sk_value] to [sk_index_id, sk_value+1] -> return empty
      // 4. T2: scan from [sk_index_id, sk_value] to [sk_index_id, sk_value+1] -> return empty
      // 5. T1: delete [sk_index_id, sk_value]
      // 6. T2: delete [sk_index_id, sk_value]
      // 7. T1: commit (success)
      // 8. T2: commit (conflict because of T1 and T2 delete [sk_index_id, sk_value])
      set_write_fence(&kd);
      rc = rpc_del_record(row_info.tx, new_slice, false/*with_extra_info*/);
    }
  }
  if (sk_iter) {
    delete sk_iter;
    sk_iter = nullptr;
  }

  free(m_sk_packed_tuple_succ);

  return rc;
}

int ha_rocksdb::check_uniqueness_and_lock(
    const struct update_row_info &row_info, bool *const pk_changed) {
  /*
    Go through each index and determine if the index has uniqueness
    requirements. If it does, then try to obtain a row lock on the new values.
    Once all locks have been obtained, then perform the changes needed to
    update/insert the row.
  */
  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    bool found = false;
    int rc = HA_EXIT_SUCCESS;

    if (is_pk(key_id, table, m_tbl_def)) {
      rc = check_and_lock_unique_pk(key_id, row_info, &found, pk_changed);
    } else {
      rc = check_and_lock_sk(key_id, row_info, &found);
    }

    if (rc != HA_EXIT_SUCCESS) {
      return rc;
    }

    if (found) {
      /* There is a row with this key already, so error out. */
      if (is_hidden_pk(key_id, table, m_tbl_def)) {
        assert(current_thd);
        tdsql::Transaction *tx = current_thd->get_tdsql_trans();
        assert(tx);
        LogError("Duplicate entry '%llu' for key 'hidden pk' "
                        "table '%s'. txid:%lu",
                        (ulonglong )(row_info.hidden_pk_id),
                        m_tbl_def->full_tablename().data(), tx->trans_id());
        errkey = MAX_KEY;
      } else {
        errkey = key_id;
      }
      m_dupp_errkey = errkey;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }

  return HA_EXIT_SUCCESS;
}

int ha_rocksdb::check_duplicate_sk(const TABLE *table_arg,
                                   const Rdb_key_def &index,
                                   const rocksdb::Slice *key,
                                   struct unique_sk_buf_info *sk_info) {
  uint n_null_fields = 0;

  /* Get proper SK buffer. */
  uchar *sk_buf = sk_info->swap_and_get_sk_buf();

  /* Get memcmp form of sk without extended pk tail */
  uint sk_memcmp_size =
      index.get_memcmp_sk_parts(table_arg, *key, sk_buf, &n_null_fields);

  sk_info->sk_memcmp_key =
      rocksdb::Slice(reinterpret_cast<char *>(sk_buf), sk_memcmp_size);

  if (sk_info->sk_memcmp_key_old.size() > 0 && n_null_fields == 0 &&
      sk_info->sk_memcmp_key.compare(sk_info->sk_memcmp_key_old) ==
          0) {
    return 1;
  }

  sk_info->sk_memcmp_key_old = sk_info->sk_memcmp_key;
  return 0;
}

int ha_rocksdb::finalize_bulk_load(bool print_client_error [[maybe_unused]]) {
  DBUG_ENTER_FUNC();

  int res = HA_EXIT_SUCCESS;

  DBUG_RETURN(res);
}

int ha_rocksdb::update_pk(const Rdb_key_def &kd,
                          const struct update_row_info &row_info,
                          const bool &pk_changed) {
  int rc = HA_EXIT_SUCCESS;
  const uint key_id = kd.get_keyno();
  const bool hidden_pk = is_hidden_pk(key_id, table, m_tbl_def);
  // set write fence
  if (hidden_pk) {
    set_write_fence();
  } else {
    set_write_fence(&kd);
  }

  /*
    If the PK has changed, or if this PK uses single deletes and this is an
    update, the old key needs to be deleted. In the single delete case, it
    might be possible to have this sequence of keys: PUT(X), PUT(X), SD(X),
    resulting in the first PUT(X) showing up.
  */
  /* TDSQL: we skip the optimization of single delete */
  if (!hidden_pk && (pk_changed || ((row_info.old_pk_slice.size() > 0) &&
                                    can_use_single_delete(key_id)))) {
    rc = rpc_del_record(row_info.tx, row_info.old_pk_slice, true/*with_extra_info*/);
    if (rc != HA_EXIT_SUCCESS) {
      // return row_info.tx->set_status_error(table->in_use, rc, kd, m_tbl_def);
      return rc;
    }
  }

  /*
    TDSQL: this is the entry point in which m_auto_increment->set_current_val is called to push ai forward.
  */
  if (table->found_next_number_field) {
    rc = update_auto_incr_val_from_field();
    if (rc != HA_EXIT_SUCCESS)
      return rc;
  }

  rocksdb::Slice value_slice;
  /* Prepare the new record to be written into RocksDB */
  if ((rc = convert_record_to_storage_format(row_info, &value_slice))) {
    return rc;
  }

  if (use_stmt_optimize_load_data(row_info.tx)) {

    // TODO: put ValidateSchemaStatusBeforePutRecord in a common path.
    rc = ValidateSchemaStatusBeforePutRecord(row_info.tx, row_info.new_pk_slice,
                                             value_slice);
    if (rc != TDSQL_EXIT_SUCCESS) {
      return rc;
    }

    rc = row_info.tx->StmtOptimizeLoadDataCacheRecord(
        m_part_pos, CONVERT_TO_TDSQL_SLICE(row_info.new_pk_slice),
        CONVERT_TO_TDSQL_SLICE(value_slice), true /*primary_record*/);

  } else if (rocksdb_enable_bulk_load_api &&
      THDVAR(table->in_use, bulk_load) &&
      !tdsql::IsSystemTable(kd.get_index_number())) {
    if (in_parallel_data_fillback_task()) {
      rc = bulk_cache_key_in_parallel(row_info.new_pk_slice, value_slice);
    } else {
      rc = bulk_cache_key(row_info.tx, row_info.new_pk_slice, value_slice);
    }
  } else {
    rc = rpc_put_record(row_info.tx, row_info.new_pk_slice, value_slice,
                        tdstore::PutRecordType::PUT_TYPE_PRIMARY);
  }

  return rc;
}

int ha_rocksdb::update_sk(const TABLE *const table_arg, const Rdb_key_def &kd,
                          const struct update_row_info &row_info,
                          const bool bulk_load_sk) {

  int new_packed_size;
  int old_packed_size;
  int rc = HA_EXIT_SUCCESS;

  rocksdb::Slice new_key_slice;
  rocksdb::Slice new_value_slice;
  rocksdb::Slice old_key_slice;

  const uint key_id = kd.get_keyno();

  if (IsAbsent((Schema_Status)table->key_info[key_id].schema_status)) {
    LogDebug("table '%s.%s' -> index: %s schema_status is %s, skip delete and put index record",
        table->s->db.str, table->s->table_name.str, table->key_info[key_id].name,
        tdsql::ddl::print_schema_status(table->key_info[key_id].schema_status).c_str());
    return HA_EXIT_SUCCESS;
  }


  /*
    Can skip updating this key if none of the key fields have changed and, if
    this table has TTL, the TTL timestamp has not changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  new_packed_size =
      kd.pack_record(table_arg, m_pack_buffer, row_info.new_data,
                     m_sk_packed_tuple, &m_sk_tails, 0,
                     row_info.hidden_pk_id, 0, nullptr, nullptr, m_ttl_bytes);

  // set write fence
  set_write_fence(&kd);

  if (row_info.old_data != nullptr) {
    // The old value
    old_packed_size = kd.pack_record(
        table_arg, m_pack_buffer, row_info.old_data, m_sk_packed_tuple_old,
        &m_sk_tails_old, 0, row_info.hidden_pk_id, 0,
        nullptr, nullptr, m_ttl_bytes);

    /*
      Check if we are going to write the same value. This can happen when
      one does
        UPDATE tbl SET col='foo'
      and we are looking at the row that already has col='foo'.

      We also need to compare the unpack info. Suppose, the collation is
      case-insensitive, and unpack info contains information about whether
      the letters were uppercase and lowercase.  Then, both 'foo' and 'FOO'
      will have the same key value, but different data in unpack_info.

      (note: anyone changing bytewise_compare should take this code into
      account)
    */
    if (old_packed_size == new_packed_size &&
        m_sk_tails_old.get_current_pos() == m_sk_tails.get_current_pos() &&
        memcmp(m_sk_packed_tuple_old, m_sk_packed_tuple, old_packed_size) ==
            0 &&
        memcmp(m_sk_tails_old.ptr(), m_sk_tails.ptr(),
               m_sk_tails.get_current_pos()) == 0) {
      return HA_EXIT_SUCCESS;
    }

    /*
      Deleting entries from secondary index should skip locking, but
      be visible to the transaction.
      (also note that DDL statements do not delete rows, so this is not a DDL
       statement)
    */
    old_key_slice = rocksdb::Slice(
        reinterpret_cast<const char *>(m_sk_packed_tuple_old), old_packed_size);
    rc = rpc_del_record(row_info.tx, old_key_slice, false/*with_extra_info*/);
    if (rc != HA_EXIT_SUCCESS) {
      return rc;
    }
  }

  new_key_slice = rocksdb::Slice(
      reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
  new_value_slice =
      rocksdb::Slice(reinterpret_cast<const char *>(m_sk_tails.ptr()),
                     m_sk_tails.get_current_pos());

  if (IsDeleteOnly((Schema_Status)table->key_info[key_id].schema_status)) {
    LogDebug("table '%s.%s' -> index: %s schema_status is %s, skip put index record <key: %s, value: %s>",
        table->s->db.str, table->s->table_name.str, table->key_info[key_id].name,
        tdsql::ddl::print_schema_status(table->key_info[key_id].schema_status).c_str(),
        MemoryDumper::ToHex(new_key_slice.data(), new_key_slice.size()).c_str(),
        MemoryDumper::ToHex(new_value_slice.data(), new_value_slice.size()).c_str());
    return HA_EXIT_SUCCESS;
  }

  if (use_stmt_optimize_load_data(row_info.tx)) {

    rc = ValidateSchemaStatusBeforePutRecord(row_info.tx, new_key_slice,
                                             new_value_slice);
    if (rc != TDSQL_EXIT_SUCCESS) {
      return rc;
    }

    rc = row_info.tx->StmtOptimizeLoadDataCacheRecord(
        m_part_pos, CONVERT_TO_TDSQL_SLICE(new_key_slice),
        CONVERT_TO_TDSQL_SLICE(new_value_slice), false /*primary_record*/);

  } else if (bulk_load_sk &&
      row_info.old_data == nullptr &&
      !tdsql::IsSystemTable(kd.get_index_number())) {
    if (in_parallel_data_fillback_task()) {
      rc = bulk_cache_key_in_parallel(new_key_slice, new_value_slice);
    } else {
      rc = bulk_cache_key(row_info.tx, new_key_slice, new_value_slice);
    }
  } else {
    rc = rpc_put_record(row_info.tx, new_key_slice, new_value_slice,
                        tdstore::PutRecordType::PUT_TYPE_SECONDARY);
  }

  return rc;
}

int ha_rocksdb::update_indexes(const struct update_row_info &row_info,
                               const bool &pk_changed) {
  int rc;
  bool bulk_load_sk;

  // The PK must be updated first to pull out the TTL value.
  rc = update_pk(*m_pk_descr, row_info, pk_changed);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  DBUG_EXECUTE_IF("upd_sec_idx_fail", { return true; });

  // Update the remaining indexes. Allow bulk loading only if
  // allow_sk is enabled
  bulk_load_sk = rocksdb_enable_bulk_load_api &&
                 THDVAR(table->in_use, bulk_load) &&
                 THDVAR(table->in_use, bulk_load_allow_sk);
  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    if (is_pk(key_id, table, m_tbl_def)) {
      continue;
    }

    rc = update_sk(table, *m_key_descr_arr[key_id], row_info, bulk_load_sk);
    if (rc != HA_EXIT_SUCCESS) {
      return rc;
    }
  }

  return HA_EXIT_SUCCESS;
}

int ha_rocksdb::HandleStmtOptimizeLoadDataDuplicateEntry(int ret) {
  assert(ret == HA_ERR_FOUND_DUPP_KEY);
  THD *thd = table->in_use;
  tdsql::Transaction *tx = thd->get_tdsql_trans();
  assert(tx && tx->stmt_optimize_load_data());

  // If OptimizeLoadData returns HA_ERR_FOUND_DUPP_KEY, it's must be the
  // user-defined primary key and the key number of primary key is 0.
  m_dupp_errkey = table->s->primary_key;
  assert(m_dupp_errkey == 0);
  const Rdb_key_def &kd = *m_key_descr_arr[m_dupp_errkey];

  setup_read_decoders();  // used by convert_record_from_storage_format.
  DuplicateEntry dup_entry;
  while (!tx->StmtOptimizeLoadDataPopDuplicateEntry(m_part_pos, &dup_entry)) {
    rocksdb::Slice dup_entry_key(dup_entry.key.str, dup_entry.key.length);
    rocksdb::Slice dup_entry_val(dup_entry.val.str, dup_entry.val.length);
    if (convert_record_from_storage_format(&dup_entry_key, &dup_entry_val,
                                           table->record[0])) {
      assert(0);
      LogError(
          "[Found duplicate entry but can not unpack it]"
          "[table:%s.%s][index:%s][key:%s][value:%s]",
          table->s->db.str, table->s->table_name.str, kd.get_name().data(),
          dup_entry_key.ToString(true /*hex*/).data(),
          dup_entry_val.ToString(true /*hex*/).data());
      return HA_ERR_ROCKSDB_CORRUPT_DATA;
    }
    if (!table->in_use->is_error()) {
      print_keydup_error(table, &table->key_info[kd.get_keyno()], MYF(0));
    }
  }
  if (tx->StmtOptimizeLoadDataIgnore()) {
    // E.g. INSERT IGNORE INTO t1 VALUES(1,1),(2,2),(3,3),(4,4),(5,5);
    ret = HA_EXIT_SUCCESS;
  }
  return ret;
}

int ha_rocksdb::update_write_row(const uchar *const old_data,
                                 const uchar *const new_data,
                                 const bool skip_unique_check) {

  int rc = HA_EXIT_SUCCESS;
  bool pk_changed = false;
  struct update_row_info row_info;

  row_info.old_data = old_data;
  row_info.new_data = new_data;
  row_info.skip_unique_check = skip_unique_check;
  row_info.new_pk_unpack_info = nullptr;

  set_last_rowkey(old_data);

  if (!in_parallel_data_fillback_task()) {
    row_info.tx = tdsql::get_or_create_tx(table->in_use, rc);
    if (rc != HA_EXIT_SUCCESS)
      return rc;
    assert(!IS_NULLPTR(row_info.tx));
  }

  if (old_data != nullptr) {
    row_info.old_pk_slice =
        rocksdb::Slice(m_last_rowkey.ptr(), m_last_rowkey.length());

    /* Determine which indexes need updating. */
    calc_updated_indexes();
  }

  /*
    Get the new row key into row_info.new_pk_slice
   */
  rc = get_pk_for_update(&row_info);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  if (!skip_unique_check && !use_stmt_optimize_load_data(row_info.tx)) {
    /*
      Check to see if we are going to have failures because of unique
      keys.  Also lock the appropriate key values.
    */
    rc = check_uniqueness_and_lock(row_info, &pk_changed);
    if (rc != HA_EXIT_SUCCESS) {
      return rc;
    }
  }

  DEBUG_SYNC(ha_thd(), "rocksdb.update_write_row_after_unique_check");

  /*
    At this point, all locks have been obtained, and all checks for duplicate
    keys have been performed. No further errors can be allowed to occur from
    here because updates to the transaction will be made and those updates
    cannot be easily removed without rolling back the entire transaction.
  */

  rc = update_indexes(row_info, pk_changed);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  if (unlikely(in_parallel_data_fillback_task())) {
    rc = flush_cache_key_in_parallel();
  } else {
    rc = flush_cache_key(row_info.tx);
  }

  rc = FlushStmtOptimizeLoadDataIfPossible(row_info.tx, false /*force*/);

  if (is_error(rc)) {
     // DBUG_RETURN(HA_ERR_ROCKSDB_BULK_LOAD);
    return rc;
  }

  return HA_EXIT_SUCCESS;
}

void ha_rocksdb::release_td_scan_iterator() {
  if (m_raw_rec_cache_it != nullptr) {
    delete m_raw_rec_cache_it;
    m_raw_rec_cache_it = nullptr;
  }

  if (m_td_scan_it != nullptr) {
    delete m_td_scan_it;
    m_td_scan_it = nullptr;
  }
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::rnd_init(bool scan) {

  int rc = HA_EXIT_SUCCESS;

  tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use, rc);
  if (rc != HA_EXIT_SUCCESS)
    return rc;
  assert(!IS_NULLPTR(tx));

  setup_read_decoders();

  int pk_idx = pk_index(table, m_tbl_def);
  if (!index_is_usable(tx, pk_idx, table, m_tbl_def)) {
    LogError("table %s.%s index <idx: %u, name: %s> is unusable, table definition has changed, need restart transaction",
        table->s->db.str, table->s->table_name.str, pk_idx, get_key_name(pk_idx, table, m_tbl_def));
    return HA_ERR_TABLE_DEF_UNUSABLE;
  }

  if (scan) {

    IndexMergePush(pk_idx);

    uint key_size;
    m_pk_descr->get_first_key(m_pk_packed_tuple, &key_size);
    uchar *tmp_pk_packed_tuple = (uchar*)malloc(key_size);
    memcpy(tmp_pk_packed_tuple, m_pk_packed_tuple, key_size);
    /* get the start_key, aka. table_id */
    rocksdb::Slice it_start_key((const char *)m_pk_packed_tuple, key_size);
    /* get the key next to start_key, it doesn't matter whether it exist or not */
    m_pk_descr->successor(tmp_pk_packed_tuple, key_size);
    rocksdb::Slice it_end_key((const char *)tmp_pk_packed_tuple, key_size);

    rocksdb::Slice index_prefix(reinterpret_cast<const char*>(
                                  m_pk_descr->get_index_number_storage_form()),
                                Rdb_key_def::INDEX_NUMBER_SIZE);

    if ((rc = init_tdsql_iterator(const_cast<tdsql::Transaction*&>(tx),
                                  index_prefix,
                                  (const rocksdb::Slice&)it_start_key,
                                  (const rocksdb::Slice&)it_end_key,
                                  true, false, get_key_tindex_id(),
                                  pk_idx)) != HA_EXIT_SUCCESS) {
      if (is_error_iterator_no_more_rec(rc)) {
        // in case of empty iterator, error should not be returned here.
        rc = HA_EXIT_SUCCESS;
      }
    }

    /* free tmp area */
    free(tmp_pk_packed_tuple);
    tmp_pk_packed_tuple = nullptr;

  } else {
    /* We don't need any preparations for rnd_pos() calls. */
  }

  // If m_lock_rows is on then we will be doing a get_for_update when accessing
  // the index, so don't acquire the snapshot right away.  Otherwise acquire
  // the snapshot immediately.
  //tx->acquire_snapshot(m_lock_rows == RDB_LOCK_NONE);

  return rc;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::rnd_next(uchar *const buf) {

  int rc;
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);
  rc = GetAndParseNextRecord(buf,pk_index(table, m_tbl_def));

  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  return rc;
}

int ha_rocksdb::rnd_end() {
  DBUG_ENTER_FUNC();

  //release_scan_iterator();

  m_ds_mrr.dsmrr_close();

  PushDownDone();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_rocksdb::rnd_range_init(const tdsql::parallel::PartKeyRangeInfo& range_info) {
  int rc = HA_EXIT_SUCCESS;

  setup_read_decoders();

  // Like rnd_init, range scan base on PK.
  int pk_idx = pk_index(table, m_tbl_def);

  const rocksdb::Slice skey = CONVERT_TO_ROCKSDB_SLICE(range_info.start);
  const rocksdb::Slice ekey = CONVERT_TO_ROCKSDB_SLICE(range_info.end);

  rocksdb::Slice index_prefix(reinterpret_cast<const char*>(
                              m_pk_descr->get_index_number_storage_form()),
                              Rdb_key_def::INDEX_NUMBER_SIZE);

  if ((rc = init_tdsql_iterator(index_prefix, skey, ekey, range_info.iterator_id,
                                get_key_tindex_id(),
                                pk_idx)) != HA_EXIT_SUCCESS) {
    if (is_error_iterator_no_more_rec(rc)) {
      // in case of empty iterator, error should not be returned here.
      rc = HA_EXIT_SUCCESS;
    }
  }

  return rc;
}

bool ha_rocksdb::tdsql_keyno_changed(QEP_TAB* tab,uint keyno) {
  switch (tab->type()) {
    case JT_ALL:
      return !is_pk(tdstore_pushed_cond_idx_, table, m_tbl_def);
    case JT_INDEX_SCAN:
      return tdstore_pushed_cond_idx_ != tab->index();
    default:
      return keyno != tdstore_pushed_cond_idx_;
  }
}

// return -1 if not push to tdstore
int ha_rocksdb::GetPushIndexNo(const QEP_TAB *qep_tab) {
  int key = -1;
  switch (qep_tab->type()) {

    case JT_REF:
    case JT_REF_OR_NULL:{
      TABLE_REF *ref = &qep_tab->ref();
      key = ref->key;
      for (unsigned key_part_idx = 0; key_part_idx < ref->key_parts;
           ++key_part_idx) {
        if (ref->cond_guards[key_part_idx] != nullptr) {
          key = -1;
          LogDebug("ref with cond_guards disable pushdown");
          break;
        }
      }
    } break;
    case JT_ALL:
      key = pk_index(table, m_tbl_def);
      break;
    case JT_INDEX_SCAN:
      key = qep_tab->index();
      break;
    case JT_RANGE: {
      QUICK_SELECT_I *quick = qep_tab->quick();
      if (!quick) {
        LogError("quick is NULL");
        return -1;
      }
      key = quick->index;
      LogDebug("[JT_RANGE] key=%d query=%s", key, current_thd->query().str);
    } break;
    case JT_EQ_REF: {
      LogDebug("do not cond push for eq_ref");
      return -1;
    }
    case JT_INDEX_MERGE: {
      LogDebug("JT_INDEX_MERGE,save all conditon here");
      // for index_merge,we can not push cond to tdstore here
      // save pushed_cond here,see IndexMergePush
      pushed_cond = qep_tab->condition();
      and_conditions(&pushed_cond, pushed_idx_cond);
      return -1;
    }
    default:
      LogDebug("type:%d not support cond_push yet", qep_tab->type());
      return -1;
  }

  if (key == MAX_KEY) {
    return -1;
  }
  return key;
}

void ha_rocksdb::SetCondPush(Item *table_cond,Item *icp_cond,int keyno){

  // projection need this even without cond
  tdstore_pushed_cond_idx_ = keyno;

  tdstore_pushed_cond_ = nullptr;

  //no condition
  if(!table_cond && !icp_cond) {
    LogDebug("no condition relate to current table");
    m_all_pushed = true;
    return;
  }

  Tdstore_cond_traverse_context ctx(table, keyno);
  ctx.is_hidden_pk = is_hidden_pk(keyno, table, m_tbl_def);

  if (pushed_idx_cond) {
    icp_cond_tdstore_ = MakeCondForTdstore(pushed_idx_cond, &ctx);
    icp_cond_remainder_ = MakeTdstoreCondRemainder(pushed_idx_cond, true);
    PrintCondition("orig icp", pushed_idx_cond);
    PrintCondition("tdstore icp", icp_cond_tdstore_);
    PrintCondition("remainder icp", icp_cond_remainder_);
  }

  if (table_cond) {
    table_cond_tdstore_ = MakeCondForTdstore(table_cond, &ctx);
    table_cond_remainder_ = MakeTdstoreCondRemainder(table_cond, true);
    PrintCondition("orig table", table_cond);
    PrintCondition("tdstore table", table_cond_tdstore_);
    PrintCondition("remainder table", table_cond_remainder_);
  }

  tdstore_pushed_cond_ = icp_cond_tdstore_;
  and_conditions(&tdstore_pushed_cond_, table_cond_tdstore_);

  PrintCondition("SetCondPush all cond push to tdstore", tdstore_pushed_cond_);

  if(!icp_cond_remainder_ && !table_cond_remainder_)
    m_all_pushed = true;
  else
    m_all_pushed = false;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::index_init(uint idx, bool sorted [[maybe_unused]]) {

  int rc = HA_EXIT_SUCCESS;
  tdsql::Transaction *tx = nullptr;
  if (unlikely(in_parallel_data_fillback_task())) {
    assert(tdsql::IsSystemTable(table->s->db.str, table->s->table_name.str));
  } else {
    tx = tdsql::get_or_create_tx(table->in_use, rc);
    assert(!IS_NULLPTR(tx));
    if (rc != HA_EXIT_SUCCESS)
      return rc;
  }

  setup_read_decoders();

  if (!index_is_usable(tx, idx, table, m_tbl_def)) {
    LogError("table %s.%s index <idx: %u, name: %s> is unusable, table definition has changed, need restart transaction",
        table->s->db.str, table->s->table_name.str, idx, get_key_name(idx, table, m_tbl_def));
    return HA_ERR_TABLE_DEF_UNUSABLE;
  }

  if (!m_keyread_only) {
    m_key_descr_arr[idx]->get_lookup_bitmap(table, &m_lookup_bitmap);
  }

  // If m_lock_rows is not RDB_LOCK_NONE then we will be doing a get_for_update
  // when accessing the index, so don't acquire the snapshot right away.
  // Otherwise acquire the snapshot immediately.
  /* TDSQL: we don't acquire or release */
  //tx->acquire_snapshot(m_lock_rows == RDB_LOCK_NONE);

  active_index = idx;

  // here we know the index,try condition pushdown for index merge
  IndexMergePush(active_index);

  return HA_EXIT_SUCCESS;
}

// called from rnd_end or index_end
// when use index_merge,need clean this before switch to another index
void ha_rocksdb::PushDownDone() {
  // do not clean if use this handler to get start_end_key
  if (only_init_iterator_) return;

  if (tdstore_pushed_cond_pb_) {
    delete tdstore_pushed_cond_pb_;
    tdstore_pushed_cond_pb_ = nullptr;
  }

  m_projection = 0;
  m_really_push = 0;
}
/**
  @return
    HA_EXIT_SUCCESS      OK
*/
int ha_rocksdb::index_end() {

  //release_scan_iterator();

  bitmap_free(&m_lookup_bitmap);

  active_index = MAX_KEY;
  in_range_check_pushed_down = false;

  /* reset m_read_key_info */
  m_read_key_info.active_index = MAX_INDEXES;
  m_read_key_info.read_key_len = 0;

  m_ds_mrr.dsmrr_close();

  PushDownDone();

  return HA_EXIT_SUCCESS;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::truncate(dd::Table *table_def MY_ATTRIBUTE((__unused__))) {
  // See ha_rockspart::truncate_partition_low.
  return HA_EXIT_SUCCESS;
}

/*
  Delete the row we've last read. The row is also passed as parameter.

  @detail
    The caller guarantees table buf points to the row that was just read.
    The row is either table->record[0] or table->record[1].
    (Check out InnoDB: row_update_for_mysql() has "UT_NOT_USED(mysql_rec)"

  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::delete_row(const uchar *const buf) {

  assert(buf != nullptr);
  int rc = HA_EXIT_SUCCESS;

  ha_statistic_increment(&System_status_var::ha_delete_count);
  set_last_rowkey(buf);

  rocksdb::Slice key_slice(m_last_rowkey.ptr(), m_last_rowkey.length());

  tdsql::Transaction *const tx = tdsql::get_or_create_tx(table->in_use, rc);
  if (rc != HA_EXIT_SUCCESS)
    return rc;
  assert(!IS_NULLPTR(tx));

  // Delete primary key record
  set_write_fence();
  if ((rc = rpc_del_record(tx, key_slice, true/*with_extra_info*/)) != HA_EXIT_SUCCESS) {
    return rc;
  }

  longlong hidden_pk_id = 0;
  if (m_tbl_def->m_key_count > 1 && has_hidden_pk(table)) {
    rc = read_hidden_pk_id_from_rowkey(&hidden_pk_id);
    if (rc) {
      return rc;
    }
  }

  // Delete the record for every secondary index
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (!is_pk(i, table, m_tbl_def)) {
      if (IsAbsent((Schema_Status)table->key_info[i].schema_status)) {
        LogDebug("table '%s.%s' -> index: %s schema_status is %s, skip delete index record",
            table->s->db.str, table->s->table_name.str, table->key_info[i].name,
            tdsql::ddl::print_schema_status(table->key_info[i].schema_status).c_str());
        continue;
      }

      int packed_size;
      const Rdb_key_def &kd = *m_key_descr_arr[i];
      packed_size = kd.pack_record(table, m_pack_buffer, buf, m_sk_packed_tuple,
                                   nullptr, false, hidden_pk_id);
      rocksdb::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      set_write_fence(&kd);
      // Deleting on secondary key doesn't need any locks:
      if ((rc = rpc_del_record(tx, secondary_key_slice, false/*with_extra_info*/)) != HA_EXIT_SUCCESS) {
        return rc;
      }
    }
  }

  m_table_handler->AddUpdateCount(1);
  update_row_stats(ROWS_DELETED);

  if (rc == 0 && table->correlate_option
      && unlikely(table->correlate_option->table())) {
    if (table->correlate_option->DeleteRow(buf)) {
      rc = HA_ERR_ROCKSDB_CORRELATE_TABLE;
    }
  }

  return rc;
}

void ha_rocksdb::update_stats(void) {
  DBUG_ENTER_FUNC();

  stats.records = 0;
  stats.index_file_length = 0ul;
  stats.data_file_length = 0ul;
  stats.mean_rec_length = 0;

  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (is_pk(i, table, m_tbl_def)) {
      // stats.data_file_length = m_pk_descr->m_stats.m_actual_disk_size;
      // stats.records = m_pk_descr->m_stats.m_rows;
    } else {
      stats.index_file_length += m_key_descr_arr[i]->m_stats.m_actual_disk_size;
    }
  }

  DBUG_VOID_RETURN;
}


void ha_rocksdb::need_update_table_cache(bool need_persist, bool async) {
  if (IsTmpTableForLogService(table)) return;

  m_table_handler->ResetWhenSubmitTask();

  uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2];
  auto r __attribute__((unused)) = get_range(pk_index(table, m_tbl_def), buf);

  uint row_len = m_pk_descr->max_value_length() + m_pk_descr->max_storage_fmt_length();
  if (__builtin_expect(async, 1)) {
    tdsql::NeedUpdateTableCache(m_table_handler, buf, row_len, need_persist);
  } else {
    tdsql::UpdateTableCacheSync(m_table_handler, buf, row_len, need_persist);
    stats.update_time = std::max(stats.update_time, m_table_handler->m_last_update_time_.load() /1000/1000);
  }
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    HA_EXIT_FAILURE  Error
*/
int ha_rocksdb::info(uint flag) {
  DBUG_ENTER_FUNC();

  THD *thd = current_thd;

  if (IsTmpTableForLogService(table))
    DBUG_RETURN(HA_EXIT_SUCCESS);

  if (!table || !table->in_use)
    DBUG_RETURN(HA_EXIT_FAILURE);

  if (flag & HA_STATUS_VARIABLE) {
    /*
      Test only to simulate corrupted stats
    */
    DBUG_EXECUTE_IF("myrocks_simulate_negative_stats",
                    m_pk_descr->m_stats.m_actual_disk_size =
                        -m_pk_descr->m_stats.m_actual_disk_size;);

    if (unlikely(stats.records == 0)) {
      stats.records = tdsql_debug_table_scan_rows;
    }

    bool need_compute_statistic = false;
    if (flag & (HA_STATUS_FORCE_UPDATE_CACHE_SYNC |
                HA_STATUS_FORCE_UPDATE_CACHE_ASYNC)) {
      need_compute_statistic = true;
    }
    if (likely(rocksdb_force_compute_memtable_stats &&
        rocksdb_debug_optimizer_n_rows == 0)) {
      uint64_t cur_time = my_micro_time();
      if ( m_table_handler->cache_is_expired_no_lock(rocksdb_force_compute_memtable_stats_cachetime, cur_time) ||
          m_table_handler->CheckNeedReComputeAnalyzeWhenUpdate()) {
        need_compute_statistic = true;
      }
    }

    if (need_compute_statistic) {
      bool need_persist = true;
      bool async = true;
      if (flag & HA_STATUS_FORCE_UPDATE_CACHE_SYNC) { //analyze table don't need to do persist,but will use sync mode
        need_persist = false;
        async = false;
      }
      if (unlikely(!m_table_handler->GetCacheTime() || !m_table_handler->m_mtcache_size)) {
        async = false;
      }
      need_update_table_cache(need_persist, async);
    }

    ha_rows table_rows;
    if (!thd->variables.range_estimation_by_histogram ||
        estimate_table_rows_by_histogram(&table_rows)) {
      table_rows = m_table_handler->m_mtcache_count;
    }
    stats.records = table_rows;
    stats.data_file_length = m_table_handler->m_mtcache_size;

    if (rocksdb_debug_optimizer_n_rows > 0)
      stats.records = rocksdb_debug_optimizer_n_rows;

    if (stats.records != 0)
      stats.mean_rec_length = stats.data_file_length / stats.records;
  }
  if (flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST)) {
    ref_length = m_pk_descr->max_storage_fmt_length();
    stats.mrr_length_per_rec = ref_length + sizeof(void *);

    for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
      if (is_hidden_pk(i, table, m_tbl_def)) {
        continue;
      }
      KEY *const k = &table->key_info[i];
      for (uint j = 0; j < k->actual_key_parts; j++) {
        const Rdb_index_stats &k_stats = m_key_descr_arr[i]->m_stats;
        uint x;

        if (k_stats.m_distinct_keys_per_prefix.size() > j &&
            k_stats.m_distinct_keys_per_prefix[j] > 0) {
          x = k_stats.m_rows / k_stats.m_distinct_keys_per_prefix[j];
          /*
            If the number of rows is less than the number of prefixes (due to
            sampling), the average number of rows with the same prefix is 1.
           */
          if (x == 0) {
            x = 1;
          }
        } else {
          x = 0;
        }
        if (x > stats.records)
          x = stats.records;
        if ((x == 0 && rocksdb_debug_optimizer_no_zero_cardinality) ||
            rocksdb_debug_optimizer_n_rows > 0) {
          // Fake cardinality implementation. For example, (idx1, idx2, idx3)
          // index
          // will have rec_per_key for (idx1)=4, (idx1,2)=2, and (idx1,2,3)=1.
          // rec_per_key for the whole index is 1, and multiplied by 2^n if
          // n suffix columns of the index are not used.
          x = 1 << (k->actual_key_parts - j - 1);
        }
        k->rec_per_key[j] = x;
      }
    }
  }

  if (flag & HA_STATUS_ERRKEY) {
    /*
      Currently we support only primary keys so we know which key had a
      uniqueness violation.
    */
    errkey = m_dupp_errkey;
    dup_ref = m_pk_tuple;  // TODO(?): this should store packed PK.
  }

  if (flag & HA_STATUS_AUTO) {
    if (table->found_next_number_field == nullptr) {
      stats.auto_increment_value = 0;
    } else {
      stats.auto_increment_value = m_autoinc_generator.get_current_val();
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

void ha_rocksdb::position(const uchar *const record) {
  DBUG_ENTER_FUNC();

  longlong hidden_pk_id = 0;
  if (has_hidden_pk(table) && read_hidden_pk_id_from_rowkey(&hidden_pk_id))
    assert(false);  // should never reach here

  /*
    Get packed primary key value from the record.

    (Note: m_last_rowkey has the packed PK of last-read-row, which allows to
    handle most cases, but there is an exception to this: when slave applies
    RBR events, it fills the record and then calls position(); rnd_pos())

    Also note that we *can* rely on PK column values being available. This is
    because table_flags() includes HA_PRIMARY_KEY_REQUIRED_FOR_POSITION bit.
    When that is true, table->prepare_for_position() adds PK columns into the
    read set (this may potentially disable index-only access when PK column
    cannot be restored from its mem-comparable form in the secondary indexes).
  */
  const uint packed_size = m_pk_descr->pack_record(
      table, m_pack_buffer, record, ref, nullptr, false, hidden_pk_id);

  /*
    It could be that mem-comparable form of PK occupies less than ref_length
    bytes. Fill the remainder with zeros.
  */
  if (ref_length > packed_size)
    memset(ref + packed_size, 0, ref_length - packed_size);

  DBUG_VOID_RETURN;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::rnd_pos(uchar *const buf, uchar *const pos) {

  int rc;
  size_t len;

  ha_statistic_increment(&System_status_var::ha_read_rnd_count);
  len = m_pk_descr->key_length(table,
                               rocksdb::Slice((const char *)pos, ref_length));
  if (len == size_t(-1)) {
    return HA_ERR_ROCKSDB_CORRUPT_DATA; /* Data corruption? */
  }

  rc = get_row_by_rowid(buf, pos, len);

  if (!rc) {
    update_row_stats(ROWS_READ);
  }

  return rc;
}

/*
  @brief
    Calculate (if needed) the bitmap of indexes that are modified by the
    current query

  @detail
    The calculation done by checking index definitions against the
  table->write_set
    column bitmap.
*/

void ha_rocksdb::calc_updated_indexes() {
  if (!m_update_scope_is_valid) {
    m_update_scope_is_valid = true;
    m_update_scope.clear_all();

    for (uint keynr = 0; keynr < table->s->keys; keynr++) {
      const Rdb_key_def &kd = *m_key_descr_arr[keynr];
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(table) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(table, kp);
        if (bitmap_is_set(table->write_set, field->field_index())) {
          m_update_scope.set_bit(keynr);
          break;
        }
      }
    }
  }
}

/**
  @return
    true      Update changed rows
    false     Update didn't change rows
  @brief
    Compare table's read_set and write_set when doing update.
*/
bool update_change_rows(TABLE *table) {

  bitmap_set_all(table->read_set);
  bitmap_set_all(table->write_set);
  return compare_records(table);
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/
int ha_rocksdb::update_row(const uchar *const old_data, uchar *const new_data) {

  assert(old_data != nullptr);
  assert(new_data != nullptr);
  assert(m_lock_rows == RDB_LOCK_WRITE);

  /*
    old_data points to record we're updating. It is the same as the record
    we've just read (for multi-table UPDATE, too, because SQL layer will make
    an rnd_pos() call to re-read the record before calling update_row())
  */
  assert(new_data == table->record[0]);

  ha_statistic_increment(&System_status_var::ha_update_count);
  int rv = update_write_row(old_data, new_data, false);

  if (rv == 0) {
    m_table_handler->AddUpdateCount(1);
    update_row_stats(ROWS_UPDATED);
  }

  if (rv == 0 && table->correlate_option
      && unlikely(table->correlate_option->table())) {
    rv = table->correlate_option->DeleteRow(old_data);
    if (!rv) {
      rv = table->correlate_option->WriteRow(new_data);
    }

    if (rv) rv = HA_ERR_ROCKSDB_CORRELATE_TABLE;
  }

  /*
    TDSQL: Now check that if we really change the record.
    For example: update set 1 = 1 doesn't really change!
  */
  if ((rv == 0) && !update_change_rows(table))
    rv = HA_ERR_RECORD_IS_THE_SAME;

  return rv;
}

/* The following function was copied from ha_blackhole::store_lock: */
THR_LOCK_DATA **ha_rocksdb::store_lock(THD *const thd, THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type) {
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);
  assert(to != nullptr);

  bool in_lock_tables = my_core::thd_in_lock_tables(thd);

  /* First, make a decision about MyRocks's internal locking */
  if (lock_type >= TL_WRITE_ALLOW_WRITE) {
    m_lock_rows = RDB_LOCK_WRITE;
  } else if (lock_type == TL_READ_WITH_SHARED_LOCKS) {
    m_lock_rows = RDB_LOCK_READ;
  } else {
    m_lock_rows = RDB_LOCK_NONE;
    if (THDVAR(thd, lock_scanned_rows)) {
      /*
        The following logic was copied directly from
        ha_innobase::store_lock_with_x_type() in
        storage/innobase/handler/ha_innodb.cc and causes MyRocks to leave
        locks in place on rows that are in a table that is not being updated.
      */
      const uint sql_command = my_core::thd_sql_command(thd);
      if ((lock_type == TL_READ && in_lock_tables) ||
          (lock_type == TL_READ_HIGH_PRIORITY && in_lock_tables) ||
          lock_type == TL_READ_WITH_SHARED_LOCKS ||
          lock_type == TL_READ_NO_INSERT ||
          (lock_type != TL_IGNORE && sql_command != SQLCOM_SELECT)) {

        ulong tx_isolation = my_core::thd_tx_isolation(thd);
        if (sql_command != SQLCOM_CHECKSUM &&
            ((my_core::thd_test_options(thd, OPTION_BIN_LOG) &&
              tx_isolation > ISO_READ_COMMITTED) ||
             tx_isolation == ISO_SERIALIZABLE ||
             (lock_type != TL_READ && lock_type != TL_READ_NO_INSERT) ||
             (sql_command != SQLCOM_INSERT_SELECT &&
              sql_command != SQLCOM_REPLACE_SELECT &&
              sql_command != SQLCOM_UPDATE && sql_command != SQLCOM_DELETE &&
              sql_command != SQLCOM_CREATE_TABLE))) {
          m_lock_rows = RDB_LOCK_READ;
        }
      }
    }
  }

  /* Then, tell the SQL layer what kind of locking it should use: */
  if (lock_type != TL_IGNORE && m_db_lock.type == TL_UNLOCK) {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) &&
        !in_lock_tables && !my_core::thd_tablespace_op(thd)) {
      lock_type = TL_WRITE_ALLOW_WRITE;
    }

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !in_lock_tables) {
      lock_type = TL_READ;
    }

    m_db_lock.type = lock_type;
  }

//  remove thr_lock,use MDL to do the job
//  *to++ = &m_db_lock;

  DBUG_RETURN(to);
}

void ha_rocksdb::read_thd_vars(THD *const) {
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (cannot be SE-specific)
*/
int ha_rocksdb::external_lock(THD *const thd, int lock_type) {
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);

  int res = HA_EXIT_SUCCESS;
  int binlog_format = my_core::thd_binlog_format(thd);
  if (lock_type == F_WRLCK && !thd->rli_slave &&
      binlog_format != BINLOG_FORMAT_ROW &&
      binlog_format != BINLOG_FORMAT_UNSPEC &&
      my_core::thd_binlog_filter_ok(thd)) {
    my_error(ER_REQUIRE_ROW_BINLOG_FORMAT, MYF(0));
    DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }

  /*
    handler in parallel task takes no responsibility
    to commit or rollback transaction. Make sure get
    proper lock is enough.
  */
  if (unlikely(in_parallel_data_fillback_task())) {
    if (lock_type == F_WRLCK) {
      m_lock_rows = RDB_LOCK_WRITE;
    }
    DBUG_RETURN(res);
  }

  if (lock_type == F_UNLCK) {
    tdsql::Transaction *const tx = tdsql::get_tx_from_thd(thd);

    if (tx) {
      tx->n_mysql_tables_in_use--;
      if (tx->n_mysql_tables_in_use == 0 &&
          !my_core::thd_test_options(thd,
                                     OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
        /*
          Do like InnoDB: when we get here, it's time to commit a
          single-statement transaction.

          If the statement involved multiple tables, this code will be executed
          for each of them, but that's ok because non-first tx->commit() calls
          will be no-ops.
        */
        if (tx->TxnStarted() && tx->commit_or_rollback() != HA_EXIT_SUCCESS) {
          set_lock_type(lock_type);
          res = HA_ERR_INTERNAL_ERROR;
        }
      }
    }
  } else {
    /* TDSQL: we may meet isolation not supported by TDStore duiring init DD. */
    if (my_core::thd_tx_isolation(thd) < ISO_READ_COMMITTED) {
      /*
        TDSQL: Do not check ISO_READ_UNCOMMITTED, or we can't create base table
               because we will meet ISO_READ_UNCOMMITTED when read/write DD tables.
        TODO: Modify logic in DD or make TDStore support ISO_READ_UNCOMMITTED.
      */
    } else if (my_core::thd_tx_isolation(thd) > ISO_REPEATABLE_READ) {
      /* Which means we are not initializing or restarting SQLEngine now. */
      if (!opt_initialize &&
          (dd::bootstrap::DD_bootstrap_ctx::instance().get_stage() ==
           dd::bootstrap::Stage::READY)) {
        my_error(ER_ISOLATION_MODE_NOT_SUPPORTED, MYF(0),
                 tx_isolation_names[my_core::thd_tx_isolation(thd)]);
        DBUG_RETURN(HA_ERR_UNSUPPORTED);
      }
    }
    /*
      It's nice to do the following on start of every statement. The problem
      is, handler->start_stmt() is not called for INSERTs.
      So, we put this code here.
    */
    tdsql::Transaction *const tx = tdsql::get_or_create_tx(thd, res);
    if (res != HA_EXIT_SUCCESS) DBUG_RETURN(res);
    assert(tx);
    // Update glsv that bring back by start tx msg from mc.
    tdsql::update_local_glsv_under_lock(tx->glsv());
    read_thd_vars(thd);
    m_update_scope_is_valid = false;

    if (skip_unique_check() && !table->correlate_src_table) {
      if ((thd->lex->sql_command == SQLCOM_INSERT ||
           thd->lex->sql_command == SQLCOM_LOAD ||
           thd->lex->sql_command == SQLCOM_REPLACE) &&
          (thd->lex->duplicates == DUP_REPLACE ||
           thd->lex->duplicates == DUP_UPDATE)) {
        my_error(ER_ON_DUPLICATE_DISABLED, MYF(0), thd->query().str);
        DBUG_RETURN(HA_ERR_UNSUPPORTED);
      }
    }

    if (lock_type == F_WRLCK) {
      if (tx->tx_read_only()) {
        my_error(ER_UPDATES_WITH_CONSISTENT_SNAPSHOT, MYF(0));
        DBUG_RETURN(HA_ERR_UNSUPPORTED);
      }

      /*
        SQL layer signals us to take a write lock. It does so when starting DML
        statement. We should put locks on the rows we're reading.

        Note: sometimes, external_lock() can be called without a prior
        ::store_lock call.  That's why we need to set lock_* members here, too.
      */
      m_lock_rows = RDB_LOCK_WRITE;
    }
    tx->n_mysql_tables_in_use++;
    res = tdsql::SetTransactionAccordingToTable(thd, table);
    if (res != HA_EXIT_SUCCESS) DBUG_RETURN(res);
    rocksdb_register_tx(rocksdb_hton, thd, tx);
  }

  DBUG_RETURN(res);
}

/**
  @note
  A quote from ha_innobase::start_stmt():
  <quote>
  MySQL calls this function at the start of each SQL statement inside LOCK
  TABLES. Inside LOCK TABLES the ::external_lock method does not work to
  mark SQL statement borders.
  </quote>

  @return
    HA_EXIT_SUCCESS  OK
*/

int ha_rocksdb::start_stmt(THD *const thd, thr_lock_type) {
  DBUG_ENTER_FUNC();

  assert(thd != nullptr);

  int rc = HA_EXIT_SUCCESS;
  tdsql::Transaction *const tx = tdsql::get_or_create_tx(thd, rc);
  if (rc != HA_EXIT_SUCCESS) DBUG_RETURN(rc);
  assert(tx);
  rc = tdsql::SetTransactionAccordingToTable(thd, table);
  if (rc != HA_EXIT_SUCCESS) DBUG_RETURN(rc);

  read_thd_vars(thd);
  rocksdb_register_tx(ht, thd, tx);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

rocksdb::Range get_range(uint32_t i,
                         uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2],
                         int offset1, int offset2) {
  uchar *buf_begin = buf;
  uchar *buf_end = buf + Rdb_key_def::INDEX_NUMBER_SIZE;
  rdb_netbuf_store_index(buf_begin, i + offset1);
  rdb_netbuf_store_index(buf_end, i + offset2);

  return rocksdb::Range(
      rocksdb::Slice((const char *)buf_begin, Rdb_key_def::INDEX_NUMBER_SIZE),
      rocksdb::Slice((const char *)buf_end, Rdb_key_def::INDEX_NUMBER_SIZE));
}

static rocksdb::Range get_range(const Rdb_key_def &kd,
                                uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2],
                                int offset1, int offset2) {
  return get_range(kd.get_index_number(), buf, offset1, offset2);
}

rocksdb::Range get_range(const Rdb_key_def &kd,
                         uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2]) {
  if (kd.m_is_reverse_cf) {
    return myrocks::get_range(kd, buf, 1, 0);
  } else {
    return myrocks::get_range(kd, buf, 0, 1);
  }
}

rocksdb::Range
ha_rocksdb::get_range(const int &i,
                      uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2]) const {
  return myrocks::get_range(*m_key_descr_arr[i], buf);
}

std::string GetIndexIdAddress(uint32 index_id) {
  uchar key_buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2] = {0};
  rocksdb::Range range = get_range(index_id, key_buf, 0, 1);

  std::string start_key(range.start.data(), range.start.size());
  std::string end_key(range.limit.data(), range.limit.size());

  ThreadRegionManager *rm = GetRegionManagerByKey(start_key);
  if (!rm) return "";

  RegionList rgns;  // Regions to be accessed.

  if (rm->GetMultiRegion(start_key, end_key, &rgns)) return "";

  if (!rgns.size()) return "";

  return rgns.begin()->tdstore_addr;
}


/*
 This function is called with total_order_seek=true, but
 upper/lower bound setting is not necessary.
 Boundary set is useful when there is no matching key,
 but in drop_index_thread's case, it means index is marked as removed,
 so no further seek will happen for the index id.
*/
static bool is_myrocks_index_empty(THD* thd,
                                   const bool is_reverse_cf [[maybe_unused]],
                                   const uint index_id) {
  bool index_removed = false;

  uchar key_buf[Rdb_key_def::INDEX_NUMBER_SIZE*2] = {0};
  // rdb_netbuf_store_uint32(key_buf, index_id);
  // const std::string start_key =
  //     std::string(reinterpret_cast<char *>(key_buf), sizeof(key_buf));

  // uchar key_buf_end[Rdb_key_def::INDEX_NUMBER_SIZE] = {0};
  // rdb_netbuf_store_uint32(key_buf_end, index_id);
  // Rdb_key_def::successor(key_buf_end, sizeof(key_buf_end));
  // const std::string end_key =
  //     std::string(reinterpret_cast<char *>(key_buf_end), sizeof(key_buf_end));

  rocksdb::Range range1 = get_range(index_id, key_buf, 0, 1);
  const rocksdb::Slice& start_key = range1.start;
  const rocksdb::Slice& end_key = range1.limit;
  // std::unique_ptr<rocksdb::Iterator> it(rdb->NewIterator(read_opts, cfh));
  // rocksdb_smart_seek(is_reverse_cf, it.get(), key);
  tdsql::Iterator* it = NULL;

  int rc = HA_EXIT_SUCCESS;
  tdsql::Transaction *tx = tdsql::get_or_create_tx(thd, rc);
  if (rc != HA_EXIT_SUCCESS) {
    return false;
  }
  assert(!IS_NULLPTR(tx));

  LogDebug("Seek iterator range (%s) - (%s)",
          MemoryDumper::ToHex(start_key.data(), start_key.size()).data(),
          MemoryDumper::ToHex(end_key.data(), end_key.size()).data());

  uchar index_prefix_arr[Rdb_key_def::INDEX_NUMBER_SIZE] = {0};
  rdb_netbuf_store_uint32(index_prefix_arr, index_id);
  rocksdb::Slice index_prefix(reinterpret_cast<const char*>(index_prefix_arr),
                              Rdb_key_def::INDEX_NUMBER_SIZE);

  rc = tdsql::CreateIterator(thd, nullptr, 0/*iterator_id*/,
                             CONVERT_TO_TDSQL_SLICE(index_prefix),
                             CONVERT_TO_TDSQL_SLICE(start_key),
                             CONVERT_TO_TDSQL_SLICE(end_key), &it,
                             true/*is_forward*/,
                             0/*keyno*/,
                             UINT32_MAX/*tindex_id*/,
                             0/*schema_version*/,
                             false /*in_parallel_fillback_index*/);
  // tdsql.Iterator.schema_obj_id = UINT32_MAX & schema_obj_version = 0 -> Skip checking write fence.
  if (rc != HA_EXIT_SUCCESS) {
    tx->commit();
    if (it) delete it;
    return index_removed;
  }

  rc = it->Next();
  if (is_error_ok(rc)) {
    LogDebug(
        "Iterator seek key (%s), cmp key_buf (%s)",
        MemoryDumper::ToHex(it->key().data(), it->key().size()).data(),
        MemoryDumper::ToHex(key_buf, Rdb_key_def::INDEX_NUMBER_SIZE).data());
    if (memcmp(it->key().data(), key_buf, Rdb_key_def::INDEX_NUMBER_SIZE)) {
      // Key does not have same prefix
      index_removed = true;
    }
  } else if (is_error_iterator_no_more_rec(rc)) {
    index_removed = true;
  }

  if (it) delete it;
  tx->commit();
  DBUG_EXECUTE_IF("after_del_range_index_data_not_empty", {index_removed = false;});
  return index_removed;
}

//return 0 if drop all indexes
//return 1 if something went wrong
int drop_index_internal(const tdsql::ddl::IndexInfoCollector* const indexes)
{
    THD* thd = current_thd;
    assert(thd); // ddl_job_thread_func assigned a thd for background thread

    int ret=0;

    size_t tot_indexes = indexes->Size();
    size_t i = 1;

    for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = indexes->cbegin();
         citer != indexes->cend(); ++citer, ++i) {

        int ret_rpc = 0;

        tdsql::IndexId index_id = citer->index_id();

        // TDSQL
        tdsql::RemoveAutoIncGenerator(index_id);

        if (!citer->HasData()) {
          DDLLogInfo(thd,
                     "[%lu/%lu][Skip index][No data in index][tindex_id:%u(%x)]",
                     i, tot_indexes, index_id, index_id);
          continue;
        }

        const bool is_reverse_cf = 0;
        uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2];
        rocksdb::Range range = get_range(index_id, buf, is_reverse_cf ? 1 : 0,
                is_reverse_cf ? 0 : 1);

        /* TDSQL begin */
        /*
          get_tdstores_by_key_range includes start_key but excludes end_key,
          so that we firstly get the members by [range.start, range.limit).
        */

        const rocksdb::Slice& start_key = range.start;
        const rocksdb::Slice& end_key = range.limit;

        std::string start_key_hex =
            MemoryDumper::ToHex(start_key.data(), start_key.size()).data();
        std::string end_key_hex =
            MemoryDumper::ToHex(end_key.data(), end_key.size()).data();

        DDLLogInfo(thd, "[%lu/%lu][Start DeleteRangeRecord][tindex_id:%u][%s,%s)",
                   i, tot_indexes, index_id, start_key_hex.data(),
                   end_key_hex.data());

        DelRangeRecordCntl cntl(thd, index_id, start_key.ToString(), end_key.ToString());
        assert(cntl.get_thd() == thd);
        ret_rpc = cntl.Execute();

        DDLLogInfo(thd, "[%lu/%lu][End DeleteRangeRecord][ret:%d][tindex_id:%u][%s,%s)",
                   i, tot_indexes, ret_rpc, index_id, start_key_hex.data(),
                   end_key_hex.data());

        // return fail if some rpc fail
        if (ret_rpc) ret = ret_rpc;

        /* TDSQL end */

    }

    return ret;
}

std::shared_ptr<Rdb_tbl_def> ha_rocksdb::get_table_if_exists(
    const char *const tablename) {
  assert(tablename != nullptr);

  std::string str;
  if (rdb_normalize_tablename(tablename, &str) != HA_EXIT_SUCCESS) {
    // We were not passed table name?
    assert(0);
    return nullptr;
  }

  return ddl_manager.find(str);
}

/*
  Note: the following function is called when the table is not open. That is,
  this->table==nullptr, pk_key_descr==nullptr, etc.

  tablename points to line in form "./dbname/tablename".

  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (can be SE-specific)
*/

int ha_rocksdb::delete_table(const char *const tablename,
                             const dd::Table *table_def
                                 MY_ATTRIBUTE((__unused__))) {

  assert(tablename != nullptr);

  /* Find the table in the hash */
  std::shared_ptr<Rdb_tbl_def> const tbl = get_table_if_exists(tablename);
  if (!tbl) {
    LogInfo("Failed to find table '%s' in myrocks dictionary, "
            "it's a sign of inconsistent metadata of myrocks' "
            "and server's data dictionary. But it's OK, "
            "it will be removed anyway.", tablename);
    return HA_EXIT_SUCCESS;
  }

  /*
    Remove the table entry in data dictionary (this will also remove it from
    the persistent data dictionary).
  */
  ddl_manager.remove(tbl, true);

  return HA_EXIT_SUCCESS;
}

/**
  @return
    HA_EXIT_SUCCESS  OK
    other            HA_ERR error code (cannot be SE-specific)
*/
int ha_rocksdb::rename_table(
    const char *const from, const char *const to,
    const dd::Table *from_table_def MY_ATTRIBUTE((__unused__)),
    dd::Table *to_table_def MY_ATTRIBUTE((__unused__))) {

  assert(from != nullptr);
  assert(to != nullptr);

  std::string from_str;
  std::string to_str;
  std::string from_db;
  std::string to_db;

  int rc = rdb_normalize_tablename(from, &from_str);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  rc = rdb_split_normalized_tablename(from_str, &from_db);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  rc = rdb_normalize_tablename(to, &to_str);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  rc = rdb_split_normalized_tablename(to_str, &to_db);
  if (rc != HA_EXIT_SUCCESS) {
    return rc;
  }

  DBUG_EXECUTE_IF("gen_sql_table_name", to_str = to_str + "#sql-test";);


  if (ddl_manager.rename(from_str, to_str)) {
    rc = HA_ERR_ROCKSDB_INVALID_TABLE;
  }

  return rc;
}

/**
  check_if_incompatible_data() called if ALTER TABLE can't detect otherwise
  if new and old definition are compatible

  @details If there are no other explicit signs like changed number of
  fields this function will be called by compare_tables()
  (sql/sql_tables.cc) to decide should we rewrite whole table or only .frm
  file.

*/

bool ha_rocksdb::check_if_incompatible_data(HA_CREATE_INFO *const info [[maybe_unused]],
                                            uint table_changes [[maybe_unused]]) {
  DBUG_ENTER_FUNC();

  assert(info != nullptr);

  // this function is needed only for online alter-table
  DBUG_RETURN(COMPATIBLE_DATA_NO);
}

/**
  @return
    HA_EXIT_SUCCESS  OK
*/
int ha_rocksdb::extra(enum ha_extra_function operation) {
  DBUG_ENTER_FUNC();

  switch (operation) {
  case HA_EXTRA_KEYREAD:
    m_keyread_only = true;
    break;
  case HA_EXTRA_NO_KEYREAD:
    m_keyread_only = false;
    break;
  case HA_EXTRA_FLUSH:
    /*
      If the table has blobs, then they are part of m_retrieved_record.
      This call invalidates them.
    */
    m_retrieved_record.Reset();
    break;
  case HA_EXTRA_PAUSE_PARALLEL_SCAN_INNER:
    m_parallel_scan_enabled = false;
    break;
  case HA_EXTRA_RESUME_PARALLEL_SCAN_INNER:
    m_parallel_scan_enabled = true;
    break;
  default:
    break;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.
*/
ha_rows ha_rocksdb::records_in_range(uint inx, key_range *const min_key,
                                     key_range *const max_key) {
  DBUG_ENTER_FUNC();

  if (!index_is_usable(ha_thd()->get_tdsql_trans(),
                       inx,
                       table, m_tbl_def)) {
    LogError("table %s.%s index <idx: %u, name: %s> is unusable, table definition has changed, need restart transaction",
        table->s->db.str, table->s->table_name.str, inx, get_key_name(inx, table, m_tbl_def));
    DBUG_RETURN(HA_POS_ERROR);
  }

  ha_rows ret = THDVAR(ha_thd(), records_in_range);
  if (ret) {
    DBUG_RETURN(ret);
  }
  if (table->force_index) {
    const ha_rows force_rows = THDVAR(ha_thd(), force_index_records_in_range);
    if (force_rows) {
      DBUG_RETURN(force_rows);
    }
  }

  const Rdb_key_def &kd = *m_key_descr_arr[inx];

  uint size1 = 0;
  if (min_key) {
    size1 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                min_key->key, min_key->keypart_map);
    if (min_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        min_key->flag == HA_READ_PREFIX_LAST ||
        min_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple, size1);
    }
  } else {
    kd.get_infimum_key(m_sk_packed_tuple, &size1);
  }

  uint size2 = 0;
  if (max_key) {
    size2 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple_old,
                                max_key->key, max_key->keypart_map);
    if (max_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        max_key->flag == HA_READ_PREFIX_LAST ||
        max_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple_old, size2);
    }
    // pad the upper key with FFFFs to make sure it is more than the lower
    if (size1 > size2) {
      memset(m_sk_packed_tuple_old + size2, 0xff, size1 - size2);
      size2 = size1;
    }
  } else {
    kd.get_supremum_key(m_sk_packed_tuple_old, &size2);
  }

  const rocksdb::Slice slice1((const char *)m_sk_packed_tuple, size1);
  const rocksdb::Slice slice2((const char *)m_sk_packed_tuple_old, size2);

  // slice1 >= slice2 means no row will match
  if (slice1.compare(slice2) >= 0) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  rocksdb::Range r(kd.m_is_reverse_cf ? slice2 : slice1,
                   kd.m_is_reverse_cf ? slice1 : slice2);


  if (tdsql_get_index_stats_from_tdstore) {
    Index_range_info index_range_info;
    int rc = get_index_statistics(kd, r.start, r.limit, &index_range_info);
    LogInfo("get_index_statistics [ret=%lld] [data_rows=%lu] [data_size=%lu]",
        ret, index_range_info.data_rows, index_range_info.data_size);
    if (is_error_ok(rc)) {
      ret += index_range_info.data_rows;
    } else {
      ret = HA_POS_ERROR;
    }
  }

  // uint64_t sz = 0;
  // auto disk_size = kd.m_stats.m_actual_disk_size;
  // if (disk_size == 0)
  //   disk_size = kd.m_stats.m_data_size;
  // auto rows = kd.m_stats.m_rows;
  // if (rows == 0 || disk_size == 0) {
  //   rows = 1;
  //   disk_size = ROCKSDB_ASSUMED_KEY_VALUE_DISK_SIZE;
  // }

  //#pragma GCC diagnostic push
  //#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  // Getting statistics, including from Memtables
  // uint8_t include_flags = rocksdb::DB::INCLUDE_FILES;
  // rdb->GetApproximateSizes(kd.get_cf(), &r, 1, &sz, include_flags);
  // ret = rows * sz / disk_size;
  // uint64_t memTableCount;
  // rdb->GetApproximateMemTableStats(kd.get_cf(), r, &memTableCount, &sz);
  // ret += memTableCount;

  /*
    GetApproximateSizes() gives estimates so ret might exceed stats.records.
    MySQL then decides to use full index scan rather than range scan, which
    is not efficient for most cases.
    To prevent this, changing estimated records slightly smaller than
    stats.records.
  */
  if (ret >= stats.records) {
    ret = stats.records * 0.99;
    LogInfo("change rows to :%lld due to stats.records:%lld",ret,stats.records);
  }

  if (rocksdb_debug_optimizer_n_rows > 0) {
    ret = rocksdb_debug_optimizer_n_rows;
  } else if (ret == 0) {
    ret = 1;
  }

  DBUG_RETURN(ret);
}

void ha_rocksdb::update_create_info(HA_CREATE_INFO *const create_info) {
  DBUG_ENTER_FUNC();

  assert(create_info != nullptr);

  if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
    /* Partition_base::update_create_info handles auto_increment value for a partitioned table. */
    if (table->found_next_number_field && !is_partition_table()) {
      create_info->auto_increment_value = m_autoinc_generator.get_current_val();
      /* TDSQL: deprecated information, not compatible with origin myrocks. */
      // create_info->max_auto_increment_value = generator()->max_val();
    }
    else { // no auto increment field
      create_info->auto_increment_value = 0; // 0 -> don't show up
      /* TDSQL: deprecated information, not compatible with origin myrocks. */
      // create_info->max_auto_increment_value = 0;
    }
  }

  DBUG_VOID_RETURN;
}

int ha_rocksdb::records(ha_rows *num_rows){
  if (current_thd->variables.tdsql_optimize_table_rows_count &&
      m_lock_rows == RDB_LOCK_NONE)
    return get_row_count_from_key(num_rows, pk_index(table, m_tbl_def));
  else
    return handler::records(num_rows);
}

int ha_rocksdb::records_from_index(ha_rows *num_rows, uint index) {

  if (current_thd->variables.tdsql_optimize_table_rows_count &&
      m_lock_rows == RDB_LOCK_NONE)
    return get_row_count_from_key(num_rows, index);

  int error = 0;
  ha_rows rows = 0;
  uchar *buf = table->record[0];
  start_psi_batch_mode();

  m_only_calc_records = true;

  if (!(error = ha_index_init(index, false))) {
    if (!(error = ha_index_first(buf))) {
      rows = 1;

      while (!table->in_use->killed) {
        DBUG_EXECUTE_IF("bug28079850",
                        table->in_use->killed = THD::KILL_QUERY;);
        if ((error = ha_index_next(buf))) {
          if (error == HA_ERR_RECORD_DELETED)
            continue;
          else
            break;
        }
        ++rows;
      }
    }
  }

  m_only_calc_records = false;

  *num_rows = rows;
  end_psi_batch_mode();
  int ha_index_end_error = 0;
  if (error != HA_ERR_END_OF_FILE) *num_rows = HA_POS_ERROR;

  // Call ha_index_end() only if handler has been initialized.
  if (inited && (ha_index_end_error = ha_index_end())) *num_rows = HA_POS_ERROR;

  return (error != HA_ERR_END_OF_FILE) ? error : ha_index_end_error;
}

/**
  @brief
  Doing manual compaction on OPTIMIZE TABLE in RocksDB.
  Compaction itself is executed by background thread in RocksDB, but
  CompactRange() waits until compaction completes so this function
  may take a long time.
  Since RocksDB dataset is allocated per index id, OPTIMIZE TABLE
  triggers manual compaction for all indexes of the table.
  @details
  Compaction range is from the beginning of the index id to
  the first row of the next index id. When using reverse order
  column family, the first row of the next index id should be
  the last row of the previous index id.

  @return
    HA_ADMIN_OK      OK
    other            HA_ADMIN error code
*/
int ha_rocksdb::optimize(THD *const, HA_CHECK_OPT *const) {
  DBUG_ENTER_FUNC();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

static int calculate_stats(
    const std::unordered_map<GL_INDEX_ID, std::shared_ptr<const Rdb_key_def>>
        &to_recalc MY_ATTRIBUTE((__unused__)),
    bool include_memtables MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  @return
    HA_ADMIN_OK      OK
    other            HA_ADMIN error code
*/
int ha_rocksdb::analyze(THD *const, HA_CHECK_OPT *const) {
  DBUG_ENTER_FUNC();

  if (table) {
    std::unordered_map<GL_INDEX_ID, std::shared_ptr<const Rdb_key_def>>
        ids_to_check;
    for (uint i = 0; i < table->s->keys; i++) {
      ids_to_check.insert(std::make_pair(m_key_descr_arr[i]->get_gl_index_id(),
                                         m_key_descr_arr[i]));
    }

    int res = calculate_stats(ids_to_check, true);
    if (res != HA_EXIT_SUCCESS) {
      DBUG_RETURN(HA_ADMIN_FAILED);
    }
  }

  DBUG_RETURN(HA_ADMIN_OK);
}

void ha_rocksdb::get_auto_increment(ulonglong off, ulonglong inc,
                                    ulonglong nb_desired_values,
                                    ulonglong *const first_value,
                                    ulonglong *const nb_reserved_values) {
  /* Partition_base::get_auto_increment handles partitioned table. */
  assert(!is_partition_table());
  DEBUG_SYNC(ha_thd(), "rocksdb.autoinc_vars");
  m_autoinc_generator.get_auto_increment(off, inc, nb_desired_values,
      first_value, nb_reserved_values);
}

#ifndef NDEBUG

/* Debugger help function */
static char dbug_item_print_buf[512];

const char *dbug_print_item(const THD *thd, Item *const item) {
  char *const buf = dbug_item_print_buf;
  String str(buf, sizeof(dbug_item_print_buf), &my_charset_bin);
  str.length(0);
  if (!item)
    return "(Item*)nullptr";
  item->print(thd, &str, QT_ORDINARY);
  if (str.c_ptr() == buf)
    return buf;
  else
    return "Couldn't fit into buffer";
}

#endif /*NDEBUG*/

// return 1 if fail
int ha_rocksdb::BuildPushDownPb(uint keyno) {
  if (tdstore_pushed_cond_pb_) {
    return 0;
  }

  if (!tdstore_pushed_cond_ && !m_projection && !pk_preload_push_) {
    LogDebug("no conditon,no projection,no pk_preload_push,no need to push");
    return 0;
  }

  if ((table->s->primary_key != MAX_KEY && keyno >= table->s->keys) ||
      (keyno >= m_tbl_def->m_key_count)) {
    LogError("keyno is not valid:%u,s->keys:%u,m_tbl_def->m_key_count:%u",
             keyno, table->s->keys, m_tbl_def->m_key_count);
    return TDSQL_EXIT_FAILURE;
  }

  tdstore_pushed_cond_pb_ = new push_down::PushConditionRequest();
  tdstore_pushed_cond_pb_->set_push_down_version(CUR_PUSH_DOWN_VERSION);
  tdstore_pushed_cond_pb_->set_enable_projection(m_projection);
  tdstore_pushed_cond_pb_->set_sql_mode(current_thd->variables.sql_mode);

  tdstore_pushed_cond_pb_->set_enable_pk_preload(pk_preload_push_);

  if (pk_preload_push_) {
    tdsql::Transaction *tx = current_thd->get_tdsql_trans();
    tdstore_pushed_cond_pb_->set_trans_id(tx->trans_id());
    tdstore_pushed_cond_pb_->set_begin_ts(tx->begin_ts());
  }

  // 1) push field info to tdstore,need this to parse the key part of record
  FillTableInfo(table, tdstore_pushed_cond_pb_->mutable_table_info(), keyno);

  // 2) push cond to tdstore
  if (tdstore_pushed_cond_) {
    tdsql::SerializeContext context(current_thd);
    tdsql::BinaryOutArchive archive_cond(&context);
    tdsql::BinaryOutArchive archive_thd(&context);
    bool ret;
    try {
      ret = archive_cond.Serialize(tdstore_pushed_cond_);

      if (!ret) ret = archive_thd.Serialize(*current_thd);
    } catch (cereal::Exception &e) {
      LogError("Deserialize execption:%s", e.what());
      ret = TDSQL_EXIT_FAILURE;
    }

    DBUG_EXECUTE_IF("build_tdstore_pushed_cond_pb_fail", { ret = true; });

    if (!ret) {
#ifndef NDEBUG
      String str;
      tdstore_pushed_cond_->print(current_thd,&str,QT_ORDINARY);
      LogDebug("push condition to tdstore:%s",str.c_ptr_safe());
#endif
      tdstore_pushed_cond_pb_->set_condition_serialize(archive_cond.str());
      tdstore_pushed_cond_pb_->set_thd_serialize(archive_thd.str());
      LogDebug("serialize size,thd:%lu,cond:%lu",
               tdstore_pushed_cond_pb_->thd_serialize().size(),
               tdstore_pushed_cond_pb_->condition_serialize().size());
    } else {
      // should not come here,CanPushToTdstore should filter at first
      LogError("condtition not support,do not push to tdstore");
      delete tdstore_pushed_cond_pb_;
      tdstore_pushed_cond_pb_ = nullptr;
      return TDSQL_EXIT_FAILURE;
    }
  }

  return 0;

}

/**
  SQL layer calls this function to push an index condition.

  @details
    The condition is for index keyno (only one condition can be pushed at a
    time).
    The caller guarantees that condition refers only to index fields; besides
    that, fields must have

      $field->part_of_key.set_bit(keyno)

    which means that

       (handler->index_flags(keyno, $keypart, 0) & HA_KEYREAD_ONLY) == 1

    which means that field value can be restored from the index tuple.

  @return
    Part of condition we couldn't check (always nullptr).
*/

class Item *ha_rocksdb::idx_cond_push(uint keyno, class Item *const idx_cond) {
  DBUG_ENTER_FUNC();

  assert(keyno != MAX_KEY);

  pushed_idx_cond = idx_cond;
  pushed_idx_cond_keyno = keyno;
  in_range_check_pushed_down = true;

  /* We will check the whole condition */
  DBUG_RETURN(nullptr);
}

/*
  @brief
  Check the index condition.

  @detail
  Check the index condition. (The caller has unpacked all needed index
  columns into table->record[0])

  @return
    ICP_NO_MATCH - Condition not satisfied (caller should continue
                   scanning)
    OUT_OF_RANGE - We've left the range we're scanning (caller should
                   stop scanning and return HA_ERR_END_OF_FILE)

    ICP_MATCH    - Condition is satisfied (caller should fetch the record
                   and return it)
*/

enum icp_result ha_rocksdb::check_index_cond() const {
  assert(pushed_idx_cond);
  assert(pushed_idx_cond_keyno != MAX_KEY);

  if (end_range && compare_key_icp(end_range) > 0) {
    /* caller should return HA_ERR_END_OF_FILE already */
    return ICP_OUT_OF_RANGE;
  }

  return pushed_idx_cond->val_int() ? ICP_MATCH : ICP_NO_MATCH;
}

/*
  Checks if inplace alter is supported for a given operation.
*/
my_core::enum_alter_inplace_result ha_rocksdb::check_if_supported_inplace_alter(
    TABLE *altered_table MY_ATTRIBUTE((__unused__)),
    my_core::Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER_FUNC();
  assert(ha_alter_info != nullptr);

  if (IsSystemTable(table->s->db.str, table->s->table_name.str)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  if (tdsql::ddl::IsAlterTmpTable(ha_alter_info)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* No need for a table rebuild when changing only index metadata */
  if (ha_alter_info->handler_flags == Alter_inplace_info::RENAME_INDEX &&
      ha_alter_info->alter_info->flags == Alter_info::ALTER_INDEX_VISIBILITY &&
      ha_alter_info->create_info->used_fields == 0) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  }

  if (tdsql::ddl::OnlyChangeColumnCompatible(table, ha_alter_info)) {
    // TODO(TDSQL): replace by HA_ALTER_INPLACE_INSTANT
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  }

  if (tdsql::ddl::SupportInstantAddColumn(ha_alter_info)) {
    // TODO(TDSQL): replace by HA_ALTER_INPLACE_INSTANT, handle alter_ctx->error_if_not_empty
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  }

  if (ha_alter_info->handler_flags &
      ~(my_core::Alter_inplace_info::DROP_INDEX |
        my_core::Alter_inplace_info::DROP_UNIQUE_INDEX |
        my_core::Alter_inplace_info::ADD_INDEX |
        my_core::Alter_inplace_info::ADD_UNIQUE_INDEX |
        my_core::Alter_inplace_info::CHANGE_CREATE_OPTION)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* We only support changing auto_increment for table options. */
  if ((ha_alter_info->handler_flags &
        my_core::Alter_inplace_info::CHANGE_CREATE_OPTION) &&
      !(ha_alter_info->create_info->used_fields & HA_CREATE_USED_AUTO)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* Add index except primary key. */
  if (tdsql::ddl::OnlyAddIndex(ha_alter_info) &&
      !(ha_alter_info->handler_flags &
        my_core::Alter_inplace_info::ADD_PK_INDEX)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  }

  if (tdsql::ddl::OnlyDropIndex(ha_alter_info)) {
    DBUG_RETURN(my_core::HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
  }

  DBUG_RETURN(my_core::HA_ALTER_INPLACE_NOT_SUPPORTED);
}

/**
  Allows the storage engine to update internal structures with concurrent
  writes blocked. If check_if_supported_inplace_alter() returns
  HA_ALTER_INPLACE_NO_LOCK_AFTER_PREPARE or
  HA_ALTER_INPLACE_SHARED_AFTER_PREPARE, this function is called with
  exclusive lock otherwise the same level of locking as for
  inplace_alter_table() will be used.

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function reports error, commit_inplace_alter_table()
  will be called with commit= false.

  @note For partitioning, failing to prepare one partition, means that
  commit_inplace_alter_table() will be called to roll back changes for
  all partitions. This means that commit_inplace_alter_table() might be
  called without prepare_inplace_alter_table() having been called first
  for a given partition.

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
                              by ALTER TABLE and holding data used
                              during in-place alter.

  @retval   true              Error
  @retval   false             Success
*/
bool ha_rocksdb::prepare_inplace_alter_table(
    TABLE *altered_table, my_core::Alter_inplace_info *ha_alter_info,
    const dd::Table *old_table_def MY_ATTRIBUTE((__unused__)),
    dd::Table *new_table_def MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  assert(altered_table != nullptr);
  assert(ha_alter_info != nullptr);
  assert(table_id_equals_pk_id(altered_table));

  std::shared_ptr<Rdb_tbl_def> new_tdef(nullptr);
  std::shared_ptr<Rdb_key_def> *old_key_descr = nullptr;
  std::shared_ptr<Rdb_key_def> *new_key_descr = nullptr;
  uint old_n_keys = m_tbl_def->m_key_count;
  uint new_n_keys = altered_table->s->keys;
  std::unordered_set<std::shared_ptr<Rdb_key_def>> added_indexes;
  std::unordered_set<GL_INDEX_ID> dropped_index_ids;
  uint n_dropped_keys = 0;
  uint n_added_keys = 0;
  ulonglong max_auto_incr = 0;

  if (ha_alter_info->handler_flags &
      (my_core::Alter_inplace_info::DROP_INDEX |
       my_core::Alter_inplace_info::DROP_UNIQUE_INDEX |
       my_core::Alter_inplace_info::ADD_INDEX |
       my_core::Alter_inplace_info::ADD_UNIQUE_INDEX)) {
    if (has_hidden_pk(altered_table)) {
      new_n_keys += 1;
    }

    const TABLE *const old_table = table;
    old_key_descr = m_tbl_def->m_key_descr_arr;
    new_key_descr = new std::shared_ptr<Rdb_key_def>[new_n_keys];

    new_tdef = std::make_shared<Rdb_tbl_def>(m_tbl_def->full_tablename());
    new_tdef->m_key_descr_arr = new_key_descr;
    new_tdef->m_key_count = new_n_keys;
    new_tdef->m_tindex_id = m_tbl_def->m_tindex_id;
    new_tdef->m_schema_version = new_table_def->schema_version();

    if (CreateKeyDefs(altered_table, new_tdef, table, m_tbl_def, m_part_pos)) {
      /* Delete the new key descriptors */
      delete[] new_key_descr;

      /*
        Explicitly mark as nullptr so we don't accidentally remove entries
        from data dictionary on cleanup (or cause double delete[]).
        */
      new_tdef->m_key_descr_arr = nullptr;
      new_tdef = nullptr;

      my_error(ER_KEY_CREATE_DURING_ALTER, MYF(0));
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    uint i;
    uint j;

    /* Determine which(if any) key definition(s) need to be dropped */
    for (i = 0; i < ha_alter_info->index_drop_count; i++) {
      const KEY *const dropped_key = ha_alter_info->index_drop_buffer[i];
      for (j = 0; j < old_n_keys; j++) {
        const KEY *const old_key =
            &old_table->key_info[old_key_descr[j]->get_keyno()];

        if (!compare_keys(old_key, dropped_key)) {
          dropped_index_ids.insert(old_key_descr[j]->get_gl_index_id());
          break;
        }
      }
    }

    /* Determine which(if any) key definitions(s) need to be added */
    int identical_indexes_found = 0;
    for (i = 0; i < ha_alter_info->index_add_count; i++) {
      const KEY *const added_key =
          &ha_alter_info->key_info_buffer[ha_alter_info->index_add_buffer[i]];
      for (j = 0; j < new_n_keys; j++) {
        const KEY *const new_key =
            &altered_table->key_info[new_key_descr[j]->get_keyno()];
        if (!compare_keys(new_key, added_key)) {
          /*
            Check for cases where an 'identical' index is being dropped and
            re-added in a single ALTER statement.  Turn this into a no-op as the
            index has not changed.

            E.G. Unique index -> non-unique index requires no change

            Note that cases where the index name remains the same but the
            key-parts are changed is already handled in create_inplace_key_defs.
            In these cases the index needs to be rebuilt.
            */
          if (dropped_index_ids.count(new_key_descr[j]->get_gl_index_id())) {
            dropped_index_ids.erase(new_key_descr[j]->get_gl_index_id());
            identical_indexes_found++;
          } else {
            added_indexes.insert(new_key_descr[j]);
          }

          break;
        }
      }
    }

    n_dropped_keys = ha_alter_info->index_drop_count - identical_indexes_found;
    n_added_keys = ha_alter_info->index_add_count - identical_indexes_found;
    assert(dropped_index_ids.size() == n_dropped_keys);
    assert(added_indexes.size() == n_added_keys);
    assert(new_n_keys == (old_n_keys - n_dropped_keys + n_added_keys));
  }
  if (ha_alter_info->handler_flags &
      my_core::Alter_inplace_info::CHANGE_CREATE_OPTION) {
    if (!new_tdef) {
      new_tdef = m_tbl_def;
    }
  }

  ha_alter_info->handler_ctx = new (*THR_MALLOC)
      Rdb_inplace_alter_ctx(new_tdef, old_key_descr, new_key_descr, old_n_keys,
                            new_n_keys, added_indexes, dropped_index_ids,
                            n_added_keys, n_dropped_keys, max_auto_incr);

  LogDebug("Prepare in-place alter table. TABLE_SHARE:%s.",
           altered_table->to_string().data());
  LogDebug("Prepare in-place alter table. Rdb_tbl_def:%s",
           new_tdef ? new_tdef->to_string().data() : "null");

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
  Alter the table structure in-place with operations specified using
  HA_ALTER_FLAGS and Alter_inplace_info. The level of concurrency allowed
  during this operation depends on the return value from
  check_if_supported_inplace_alter().

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function reports error, commit_inplace_alter_table()
  will be called with commit= false.

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
                              by ALTER TABLE and holding data used
                              during in-place alter.

  @retval   true              Error
  @retval   false             Success
*/
bool ha_rocksdb::inplace_alter_table(
    TABLE *altered_table, my_core::Alter_inplace_info *ha_alter_info,
    const dd::Table *old_table_def MY_ATTRIBUTE((__unused__)),
    dd::Table *new_table_def MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  assert(altered_table != nullptr);
  assert(ha_alter_info != nullptr);
  assert(ha_alter_info->handler_ctx != nullptr);

  Rdb_inplace_alter_ctx *const ctx =
      static_cast<Rdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);

  if ((ha_alter_info->handler_flags &
      (my_core::Alter_inplace_info::ADD_INDEX |
       my_core::Alter_inplace_info::ADD_UNIQUE_INDEX))) {
    /*
      Buffers need to be set up again to account for new, possibly longer
      secondary keys.
    */
    free_key_buffers();

    assert(ctx != nullptr);

    /*
      If adding unique index, allocate special buffers for duplicate checking.
    */
    int err;
    if ((err = alloc_key_buffers(
             altered_table, ctx->m_new_tdef,
             ha_alter_info->handler_flags &
                 my_core::Alter_inplace_info::ADD_UNIQUE_INDEX))) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0));
      DBUG_RETURN(err);
    }

    /* Populate all new secondary keys by scanning the primary key. */
    if ((err = inplace_populate_sk(altered_table, ctx->m_added_indexes))) {
      my_error(ER_SK_POPULATE_DURING_ALTER, MYF(0));
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
  }

  DBUG_EXECUTE_IF("myrocks_simulate_index_create_rollback", {
    dbug_create_err_inplace_alter();
    DBUG_RETURN(HA_EXIT_FAILURE);
  };);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
 Scan the Primary Key index entries and populate the new secondary keys.
*/
int ha_rocksdb::inplace_populate_sk(
    TABLE *const,
    const std::unordered_set<std::shared_ptr<Rdb_key_def>> &) {
  DBUG_ENTER_FUNC();

  int res = HA_EXIT_SUCCESS;

  DBUG_EXECUTE_IF("crash_during_online_index_creation", DBUG_SUICIDE(););
  DBUG_RETURN(res);
}

// TODO:remove this while optimizing index_fillback_sk()
static int find_index_num(TABLE *table, uint32 tindex_id) {

  for (uint keyno=0; keyno < table->s->keys; ++keyno) {
    KEY* key = &table->key_info[keyno];
    if (key->tindex_id == tindex_id) {
      return keyno;
    }
  }

  return -1;
}

// TODO(TDSQL): Optimize this function
int ha_rocksdb::index_fillback_sk() {
  DBUG_ENTER_FUNC();
  //bool is_unique_index;
  int index_found;
  int64 res = 0;
  THD *thd = current_thd;
  ddl::Ddl_job_alter_table *ddl_add_index =
    dynamic_cast<ddl::Ddl_job_alter_table*>(thd->ddl_worker->get_ddl_job());
  const tdsql::ddl::IndexInfoCollector* const indexes_to_be_created =
    ddl_add_index->indexes_to_be_created();
  bool has_partition = indexes_to_be_created->HasPartition();

  // TODO(TDSQL): set by Alter_info
  bool has_unique = false;

  tdsql::ddl::IndexInfoCollector real_indexes_to_be_created;

  // TODO(TDSQL): optimize
  for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = indexes_to_be_created->cbegin();
       citer != indexes_to_be_created->cend(); ++citer) {
    index_found = find_index_num(table, citer->index_id());
    if (index_found < 0 || index_found >= int(table->s->keys)) {
      /*
        For partition table, indexes_to_be_created in ddl job contains
        indexes for other partitions, choose correct indexes here.
      */
      if (has_partition) continue;
      LogError("Error not find new created index for normal table.");
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    real_indexes_to_be_created.Add(*citer);

    if (!has_unique) {
      has_unique = table->key_info[index_found].flags & HA_NOSAME;
    }
  }

#ifndef NDEBUG
  if (has_partition) {
    assert(real_indexes_to_be_created.Size() < indexes_to_be_created->Size());
  } else {
    assert(real_indexes_to_be_created.Size() == indexes_to_be_created->Size());
  }
#endif

  tdsql::Transaction* tx = thd->get_tdsql_trans();
  int retval = tx->commit();
  if (retval != TDSQL_EXIT_SUCCESS) {
    LogError("query: '%s' commit_tx failed, retval=%d, index_fillback_sk failed",
        thd->query().str, retval);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  assert(tx->trans_id() == 0 && tx->begin_ts() == 0);

  retval = tx->start_tx();
  if (unlikely(retval != TDSQL_EXIT_SUCCESS)) {
    LogError("query: '%s' start_tx failed, retval=%d, index_fillback_sk failed",
        thd->query().str, retval);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  tx->set_is_index_fillback_tx(true);

  assert(tx->trans_id() > 0 && tx->begin_ts() > 0 && tx->begin_ts() >= tx->trans_id());

  uint64_t fillback_ts = tx->begin_ts();

  LogInfo("index fillback_ts is %lu, scan record trans_id: %lu", fillback_ts, tx->trans_id());

  tdsql::ThomasRulePutCntl thomas_put_cntl(thd, fillback_ts);

  ha_rnd_init(true/*scan*/);

  res = thomas_write_fillback(has_unique, real_indexes_to_be_created, &thomas_put_cntl);
  if (res != HA_EXIT_SUCCESS) {
    LogError("Data fillback with thomas write failed.");
    DBUG_RETURN(res);
  }

  // we don't care about this return value
  {
    Dummy_error_handler error_handler;
    thd->push_internal_handler(&error_handler);
    DBUG_EXECUTE_IF("ignore_my_error_after_fillback_index", {
      my_error(ER_SQLENGINE_SYSTEM, MYF(0), "ignore_my_error_after_fillback_index");});
    tx->rollback();
    thd->pop_internal_handler();
  }

  // TODO(TDSQL): check unique key
  for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = real_indexes_to_be_created.cbegin();
       citer != real_indexes_to_be_created.cend(); ++citer) {
    index_found = find_index_num(table, citer->index_id());
    if (index_found < 0 || index_found >= int(table->s->keys)) {
      LogError("Error not find new created index.");
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    const Rdb_key_def &kd = *m_tbl_def->m_key_descr_arr[index_found];

    bool is_unique_index = table->key_info[index_found].flags & HA_NOSAME;

    if (!is_unique_index) {
      continue;
    }

    int err = check_index_dup_key(kd);
    if (err) {
      LogError("query: '%s' check_index_dup_key failed, retval=%d, index_fillback_sk failed",
          thd->query().str, err);
      DBUG_RETURN(err);
    }
  }


  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_rocksdb::index_fillback_sk_by_range(const tdsql::Slice &start,
                                           const tdsql::Slice &end,
                                           uint64 iterator_id) {
  int index_found;
  int64 res = 0;
  DBUG_ENTER_FUNC();
  THD *thd = table->in_use;
  ddl::Ddl_job_alter_table *ddl_add_index =
    dynamic_cast<ddl::Ddl_job_alter_table*>(thd->ddl_worker->get_ddl_job());
  const tdsql::ddl::IndexInfoCollector* const indexes_to_be_created =
    ddl_add_index->indexes_to_be_created();
  bool has_partition = indexes_to_be_created->HasPartition();

  // TODO(TDSQL): set by Alter_info
  bool has_unique = false;

  tdsql::ddl::IndexInfoCollector real_indexes_to_be_created;

  // TODO(TDSQL): optimize
  for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = indexes_to_be_created->cbegin();
       citer != indexes_to_be_created->cend(); ++citer) {
    index_found = find_index_num(table, citer->index_id());
    if (index_found < 0 || index_found >= int(table->s->keys)) {
      /*
        For partition table, indexes_to_be_created in ddl job contains
        indexes for other partitions, choose correct indexes here.
      */
      if (has_partition) continue;
      LogError("Error not find new created index for normal table.");
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    real_indexes_to_be_created.Add(*citer);

    if (!has_unique) {
      has_unique = table->key_info[index_found].flags & HA_NOSAME;
    }
  }

#ifndef NDEBUG
  if (has_partition) {
    assert(real_indexes_to_be_created.Size() < indexes_to_be_created->Size());
  } else {
    assert(real_indexes_to_be_created.Size() == indexes_to_be_created->Size());
  }
#endif

  tdsql::parallel::PartKeyRangeInfo range_info(start, end, iterator_id);

  if (ha_rnd_range_init(range_info)) {
    LogError("Error init rnd range scan.");
    ha_rnd_end();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  uint64_t begin_ts = thd->get_tdsql_trans()->begin_ts();
  assert(begin_ts > 0);
  tdsql::ThomasRulePutCntl thomas_put_cntl(thd, begin_ts);

  res = thomas_write_fillback(has_unique, real_indexes_to_be_created, &thomas_put_cntl);
  if (res != HA_EXIT_SUCCESS) {
    LogError("Data fillback with thomas write failed.");
    DBUG_RETURN(res);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_rocksdb::thomas_write_fillback(bool has_unique,
                                      const tdsql::ddl::IndexInfoCollector &indexes_to_be_added,
                                      tdsql::ThomasRulePutCntl *cntl) {
  DBUG_ENTER_FUNC();
  assert(!indexes_to_be_added.Empty());
  assert(inited == RND);

  free_key_buffers();
  if (alloc_key_buffers(table, m_tbl_def, has_unique)) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0));
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  m_lock_rows = RDB_LOCK_NONE;

  int res = 0;
  int index_found;
  /*
    Check if current rnd is empty, used for
    https://git.woa.com/tdsql3.0/SQLEngine/issues/1281
  */
  uint64 record_num = 0;

  for (res = rnd_next(table->record[0]); res == 0; res = rnd_next(table->record[0])) {
    longlong hidden_pk_id = 0;
    if (has_hidden_pk(table) &&
        (res = read_hidden_pk_id_from_rowkey(&hidden_pk_id))) {
      LogError("Error retrieving hidden pk id.");
      ha_rnd_end();
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    record_num++;

    for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = indexes_to_be_added.cbegin();
         citer != indexes_to_be_added.cend(); ++citer) {
      index_found = find_index_num(table, (*citer).index_id());
      if (index_found < 0 || index_found >= int(table->s->keys)) {
        LogError("Error not find new created index.");
        ha_rnd_end();
        DBUG_RETURN(HA_EXIT_FAILURE);
      }

      const Rdb_key_def &fdk = *m_tbl_def->m_key_descr_arr[index_found];

      /* Create new secondary index entry */
      const int new_packed_size = fdk.pack_record(
          table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
          &m_sk_tails, 0, hidden_pk_id, 0,
          nullptr, nullptr, m_ttl_bytes);

      const rocksdb::Slice key = rocksdb::Slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
      const rocksdb::Slice val =
        rocksdb::Slice(reinterpret_cast<const char *>(m_sk_tails.ptr()),
            m_sk_tails.get_current_pos());

      if (cntl->AddKV(CONVERT_TO_TDSQL_SLICE(key), CONVERT_TO_TDSQL_SLICE(val),
                      kInvalidIndexId, kInvalidSchemaVersion)) {
        LogError("Data fillback failed during thomas put, stage AddKV.");
        ha_rnd_end();
        DBUG_RETURN(HA_EXIT_FAILURE);
      }

      if (cntl->RecordNum() >= THDVAR(table->in_use, bulk_load_size)) {
        if (cntl->Execute()) {
          LogError("Data fillback failed during thomas put, stage Execute.");
          ha_rnd_end();
          DBUG_RETURN(HA_EXIT_FAILURE);
        }
      }
    }
  }

  if (cntl->Execute()) {
    LogError("Data fillback failed during thomas put, stage Execute.");
    ha_rnd_end();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  DBUG_EXECUTE_IF("thomas_write_fillback_scan_pk_fail",  {
    my_error(ER_SQLENGINE_SYSTEM, MYF(0), "thomas_write_fillback_scan_pk_fail");
    res = HA_EXIT_FAILURE;
  });

  ha_rnd_end();

  if (res != HA_ERR_END_OF_FILE) {
    LogError("Error retrieving index entry from primary key. retval=%d", res);
    DBUG_RETURN(res);
  }

  if (unlikely(record_num == 0)) {
    LogWarn("[No record found in rnd range scan (%s, %s)]",
        MemoryDumper::ToHex(get_iterator()->start_key()).data(),
        MemoryDumper::ToHex(get_iterator()->end_key()).data());
  } else {
    LogInfo("[%lu records found in rnd range scan (%s, %s)]",
        record_num, MemoryDumper::ToHex(get_iterator()->start_key()).data(),
        MemoryDumper::ToHex(get_iterator()->end_key()).data());
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_rocksdb::check_unique_index_dup_key(uint keyno) {
  if (keyno >= table->s->keys) {
    return HA_EXIT_FAILURE;
  }

  if (!(table->key_info[keyno].flags & HA_NOSAME)) {
    return HA_EXIT_SUCCESS;
  }

  const Rdb_key_def &kd = *m_tbl_def->m_key_descr_arr[keyno];

  int retval = check_index_dup_key(kd);
  if (retval) {
    return retval;
  }

  return HA_EXIT_SUCCESS;
}

/**
  Commit or rollback the changes made during prepare_inplace_alter_table()
  and inplace_alter_table() inside the storage engine.
  Note that in case of rollback the allowed level of concurrency during
  this operation will be the same as for inplace_alter_table() and thus
  might be higher than during prepare_inplace_alter_table(). (For example,
  concurrent writes were blocked during prepare, but might not be during
  rollback).

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function with commit= true reports error, it will be called
  again with commit= false.

  @note In case of partitioning, this function might be called for rollback
  without prepare_inplace_alter_table() having been called first.
  Also partitioned tables sets ha_alter_info->group_commit_ctx to a NULL
  terminated array of the partitions handlers and if all of them are
  committed as one, then group_commit_ctx should be set to NULL to indicate
  to the partitioning handler that all partitions handlers are committed.
  @see prepare_inplace_alter_table().

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
                              by ALTER TABLE and holding data used
                              during in-place alter.
  @param    commit            True => Commit, False => Rollback.

  @retval   true              Error
  @retval   false             Success
*/
bool ha_rocksdb::commit_inplace_alter_table(
    my_core::TABLE *altered_table, my_core::Alter_inplace_info *ha_alter_info,
    bool commit, const dd::Table *old_table_def MY_ATTRIBUTE((__unused__)),
    dd::Table *new_table_def MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  assert(altered_table != nullptr);
  assert(ha_alter_info != nullptr);

  Rdb_inplace_alter_ctx *const ctx0 =
      static_cast<Rdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);

  DEBUG_SYNC(ha_thd(), "rocksdb.commit_in_place_alter_table");

  /*
    IMPORTANT: When rollback is requested, mysql will abort with
    an assertion failure. That means every failed commit during inplace alter
    table will result in a fatal error on the server. Indexes ongoing creation
    will be detected when the server restarts, and dropped.

    For partitioned tables, a rollback call to this function (commit == false)
    is done for each partition.  A successful commit call only executes once
    for all partitions.
  */
  if (!commit) {
    /* If ctx has not been created yet, nothing to do here */
    if (!ctx0) {
      DBUG_RETURN(HA_EXIT_SUCCESS);
    }

    /*
      Cannot call destructor for Rdb_tbl_def directly because we don't want to
      erase the mappings inside the ddl_manager, as the old_key_descr is still
      using them.
    */
    if (ctx0->m_new_key_descr) {
      /* Delete the new key descriptors */
      for (uint i = 0; i < ctx0->m_new_tdef->m_key_count; i++) {
        ctx0->m_new_key_descr[i] = nullptr;
      }

      delete[] ctx0->m_new_key_descr;
      ctx0->m_new_key_descr = nullptr;
      ctx0->m_new_tdef->m_key_descr_arr = nullptr;

      ctx0->m_new_tdef = nullptr;
    }

    /* Remove uncommitted key definitions from ddl_manager */
    ddl_manager.remove_uncommitted_keydefs(ctx0->m_added_indexes);

    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  assert(ctx0);

  /*
    For partitioned tables, we need to commit all changes to all tables at
    once, unlike in the other inplace alter API methods.
  */
  inplace_alter_handler_ctx **ctx_array;
  inplace_alter_handler_ctx *ctx_single[2];

  if (ha_alter_info->group_commit_ctx) {
    DBUG_EXECUTE_IF("crash_during_index_creation_partition", DBUG_SUICIDE(););
    ctx_array = ha_alter_info->group_commit_ctx;
  } else {
    ctx_single[0] = ctx0;
    ctx_single[1] = nullptr;
    ctx_array = ctx_single;
  }

  assert(ctx0 == ctx_array[0]);
  ha_alter_info->group_commit_ctx = nullptr;

  if (ha_alter_info->handler_flags &
      (my_core::Alter_inplace_info::DROP_INDEX |
       my_core::Alter_inplace_info::DROP_UNIQUE_INDEX |
       my_core::Alter_inplace_info::ADD_INDEX |
       my_core::Alter_inplace_info::ADD_UNIQUE_INDEX)) {

    m_tbl_def = ctx0->m_new_tdef;
    m_key_descr_arr = m_tbl_def->m_key_descr_arr;
    m_pk_descr = m_key_descr_arr[pk_index(altered_table, m_tbl_def)];

    for (inplace_alter_handler_ctx **pctx = ctx_array; *pctx; pctx++) {
      Rdb_inplace_alter_ctx *const ctx =
          static_cast<Rdb_inplace_alter_ctx *>(*pctx);

      if (ddl_manager.put(ctx->m_new_tdef)) {
        /*
          Failed to write new entry into data dictionary, this should never
          happen.
        */
        assert(0);
      }

      /*
        Remove uncommitted key definitions from ddl_manager, as they are now
        committed into the data dictionary.
      */
      ddl_manager.remove_uncommitted_keydefs(ctx->m_added_indexes);
    }

  }

  if (ha_alter_info->handler_flags &
      (my_core::Alter_inplace_info::CHANGE_CREATE_OPTION)) {

    ulonglong auto_incr_val = ha_alter_info->create_info->auto_increment_value;

    /*
      Push forward auto_increment value iff there's an auto_increment column.
      TODO: Push forward anyway
            for the purpose of keeping the same with original MyRocks.
    */
    if (!is_partition_table()) {
      if (altered_table->found_next_number_field ) {
        /* ha_rocksdb::open should've created m_generator. */
        int rc = update_auto_incr_val(auto_incr_val);
        if (is_error(rc)) {
          /* Errors have been reported inside update_auto_incr_val. */
          m_autoinc_generator.release_generator();
          DBUG_RETURN(HA_EXIT_FAILURE);
        }
      } else {
        THD *thd = ha_thd();
        push_warning(thd, Sql_condition::SL_WARNING, ER_UNKNOWN_ERROR,
                     "Alter auto_increment_value doesn't work "
                     "for a table without auto_increment definition");
      }
    }

  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

#define DEF_STATUS_VAR_FUNC(name, ptr, option)                                 \
  { name, reinterpret_cast<char *>(ptr), option, SHOW_SCOPE_GLOBAL }

static void myrocks_update_status() {
  export_stats.rows_deleted = global_stats.rows[ROWS_DELETED];
  export_stats.rows_inserted = global_stats.rows[ROWS_INSERTED];
  export_stats.rows_read = global_stats.rows[ROWS_READ];
  export_stats.rows_updated = global_stats.rows[ROWS_UPDATED];
  export_stats.rows_expired = global_stats.rows[ROWS_EXPIRED];
  export_stats.rows_filtered = global_stats.rows[ROWS_FILTERED];

  export_stats.system_rows_deleted = global_stats.system_rows[ROWS_DELETED];
  export_stats.system_rows_inserted = global_stats.system_rows[ROWS_INSERTED];
  export_stats.system_rows_read = global_stats.system_rows[ROWS_READ];
  export_stats.system_rows_updated = global_stats.system_rows[ROWS_UPDATED];

  export_stats.queries_point = global_stats.queries[QUERIES_POINT];
  export_stats.queries_range = global_stats.queries[QUERIES_RANGE];
  export_stats.queries_count_optim = global_stats.queries[QUERIES_COUNT_OPTIM];

  export_stats.covered_secondary_key_lookups =
      global_stats.covered_secondary_key_lookups;
}

static SHOW_VAR myrocks_status_variables[] = {
    DEF_STATUS_VAR_FUNC("rows_deleted", &export_stats.rows_deleted,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_inserted", &export_stats.rows_inserted,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_read", &export_stats.rows_read, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_updated", &export_stats.rows_updated,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_expired", &export_stats.rows_expired,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_filtered", &export_stats.rows_filtered,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_deleted",
                        &export_stats.system_rows_deleted, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_inserted",
                        &export_stats.system_rows_inserted, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_read", &export_stats.system_rows_read,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_updated",
                        &export_stats.system_rows_updated, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("queries_point", &export_stats.queries_point,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("queries_range", &export_stats.queries_range,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("queries_count_optim", &export_stats.queries_count_optim,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("covered_secondary_key_lookups",
                        &export_stats.covered_secondary_key_lookups,
                        SHOW_LONGLONG),

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

static void show_myrocks_vars(THD *, SHOW_VAR *var, char *) {
  myrocks_update_status();
  var->type = SHOW_ARRAY;
  var->value = reinterpret_cast<char *>(&myrocks_status_variables);
}

static SHOW_VAR rocksdb_status_vars[] = {
    // the variables generated by SHOW_FUNC are sorted only by prefix (first
    // arg in the tuple below), so make sure it is unique to make sorting
    // deterministic as quick sort is not stable
    {"rocksdb", reinterpret_cast<char *>(&show_myrocks_vars), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

/****************************************************************************
 * DS-MRR implementation, copy from ha_innodb.cc
 ***************************************************************************/

/**
Multi Range Read interface, DS-MRR calls */
int ha_rocksdb::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                      uint n_ranges, uint mode,
                                      HANDLER_BUFFER *buf) {
  m_ds_mrr.init(table);

  set_write_fence();

  return (m_ds_mrr.dsmrr_init(seq, seq_init_param, n_ranges, mode, buf));
}

int ha_rocksdb::multi_range_read_next(char **range_info) {
  set_write_fence();

  return (m_ds_mrr.dsmrr_next(range_info));
}

ha_rows ha_rocksdb::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                                void *seq_init_param,
                                                uint n_ranges, uint *bufsz,
                                                uint *flags,
                                                Cost_estimate *cost) {
  /* See comments in ha_myisam::multi_range_read_info_const */
  m_ds_mrr.init(table);

  return (m_ds_mrr.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges, bufsz,
                                    flags, cost));
}

ha_rows ha_rocksdb::multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                          uint *bufsz, uint *flags,
                                          Cost_estimate *cost) {
  m_ds_mrr.init(table);

  return (m_ds_mrr.dsmrr_info(keyno, n_ranges, keys, bufsz, flags, cost));
}

void rdb_update_global_stats(const operation_type &type, uint count,
                             bool is_system_table) {
  assert(type < ROWS_MAX);

  if (count == 0) {
    return;
  }

  if (is_system_table) {
    global_stats.system_rows[type].add(count);
  } else {
    global_stats.rows[type].add(count);
  }
}

const char *get_rdb_io_error_string(const RDB_IO_ERROR_TYPE err_type) {
  // If this assertion fails then this means that a member has been either added
  // to or removed from RDB_IO_ERROR_TYPE enum and this function needs to be
  // changed to return the appropriate value.
  static_assert(RDB_IO_ERROR_LAST == 4, "Please handle all the error types.");

  switch (err_type) {
  case RDB_IO_ERROR_TYPE::RDB_IO_ERROR_TX_COMMIT:
    return "RDB_IO_ERROR_TX_COMMIT";
  case RDB_IO_ERROR_TYPE::RDB_IO_ERROR_DICT_COMMIT:
    return "RDB_IO_ERROR_DICT_COMMIT";
  case RDB_IO_ERROR_TYPE::RDB_IO_ERROR_BG_THREAD:
    return "RDB_IO_ERROR_BG_THREAD";
  case RDB_IO_ERROR_TYPE::RDB_IO_ERROR_GENERAL:
    return "RDB_IO_ERROR_GENERAL";
  default:
    assert(false);
    return "(unknown)";
  }
}

// In case of core dump generation we want this function NOT to be optimized
// so that we can capture as much data as possible to debug the root cause
// more efficiently.
#if defined(NDEBUG)
#ifdef __clang__
MY_ATTRIBUTE((optnone))
#else
MY_ATTRIBUTE((optimize("O0")))
#endif
#endif

Rdb_ddl_manager *rdb_get_ddl_manager(void) { return &ddl_manager; }

Rdb_hton_init_state *rdb_get_hton_init_state(void) { return &hton_init_state; }
int mysql_value_to_bool(struct st_mysql_value *value, bool *return_value) {
  int new_value_type = value->value_type(value);
  if (new_value_type == MYSQL_VALUE_TYPE_STRING) {
    char buf[16];
    int len = sizeof(buf);
    const char *str = value->val_str(value, buf, &len);
    if (str && (my_strcasecmp(system_charset_info, "true", str) == 0 ||
                my_strcasecmp(system_charset_info, "on", str) == 0)) {
      *return_value = true;
    } else if (str && (my_strcasecmp(system_charset_info, "false", str) == 0 ||
                       my_strcasecmp(system_charset_info, "off", str) == 0)) {
      *return_value = false;
    } else {
      return 1;
    }
  } else if (new_value_type == MYSQL_VALUE_TYPE_INT) {
    long long intbuf;
    value->val_int(value, &intbuf);
    if (intbuf > 1)
      return 1;
    *return_value = intbuf > 0;
  } else {
    return 1;
  }

  return 0;
}

int rocksdb_check_bulk_load(THD *const thd,
                            struct SYS_VAR *var MY_ATTRIBUTE((__unused__)),
                            void *save, struct st_mysql_value *value) {
  bool new_value;
  if (mysql_value_to_bool(value, &new_value) != 0) {
    return 1;
  }

  tdsql::Transaction *tx = tdsql::get_tx_from_thd(thd);
  if (tx) {
    int rc = tx->FlushBatchRecord();
    if (rc != 0) {
      LogPluginErrMsg(
          ERROR_LEVEL, 0,
          "Error %d finalizing last cached batch record while setting bulk "
          "loading variable",
          rc);
      THDVAR(thd, bulk_load) = 0;
      return 1;
    }
  }

  *static_cast<bool *>(save) = new_value;
  return 0;
}

int rocksdb_check_bulk_load_allow_unsorted(
    THD *const thd, struct SYS_VAR *var MY_ATTRIBUTE((__unused__)), void *save,
    struct st_mysql_value *value) {

  bool new_value;
  if (mysql_value_to_bool(value, &new_value) != 0) {
    return 1;
  }

  if (THDVAR(thd, bulk_load)) {
    LogPluginErrMsg(ERROR_LEVEL, 0,
                    "Cannot change this setting while bulk load is enabled");

    return 1;
  }

  *static_cast<bool *>(save) = new_value;

  return 0;
}

#if defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
void ha_rocksdb::rpl_before_delete_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = true;

  DBUG_VOID_RETURN;
}

void ha_rocksdb::rpl_after_delete_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = false;

  DBUG_VOID_RETURN;
}

void ha_rocksdb::rpl_before_update_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = true;

  DBUG_VOID_RETURN;
}

void ha_rocksdb::rpl_after_update_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = false;

  DBUG_VOID_RETURN;
}

/**
  @brief
  Read Free Replication can be used or not. Returning False means
  Read Free Replication can be used. Read Free Replication can be used
  on UPDATE or DELETE row events, and table must have user defined
  primary key.
*/
bool ha_rocksdb::use_read_free_rpl() {
  DBUG_ENTER_FUNC();

  DBUG_RETURN((m_in_rpl_delete_rows || m_in_rpl_update_rows) &&
              !has_hidden_pk(table) && m_use_read_free_rpl);
}
#endif  // defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR

double ha_rocksdb::read_time(uint index, uint ranges, ha_rows rows) {
  DBUG_ENTER_FUNC();

  if (index != table->s->primary_key) {
    /* Non covering index range scan */
    DBUG_RETURN(handler::read_time(index, ranges, rows));
  }

  DBUG_RETURN((rows / 20.0) + 1);
}

void ha_rocksdb::print_error(int error, myf errflag) {
  if (error == HA_ERR_ROCKSDB_CORRELATE_TABLE) {
    assert(table->correlate_option);
    if (table->correlate_option->table() &&
        table->correlate_option->last_errno() != 0) {
      tdsql::ddl::SetCorrelateTableError(table);
      return;
    }
  }

  if (error == HA_ERR_ROCKSDB_STATUS_BUSY) {
    error = HA_ERR_LOCK_DEADLOCK;
  }

  handler::print_error(error, errflag);
}

void ha_rocksdb::set_write_fence(const Rdb_key_def* kd_in) {
  THD* thd = table->in_use;
  assert(kd_in);
  uint key_no = kd_in->get_keyno();

  tdsql::IndexId index_id_to_set = tdsql::kInvalidIndexId;
  tdsql::SchemaVersion schema_version_to_set = tdsql::kInvalidSchemaVersion;

  if (is_hidden_pk(key_no, table, m_tbl_def) || // hidden pk
      key_no == table->s->primary_key) {        // user-defined pk
    index_id_to_set = get_key_tindex_id();
    schema_version_to_set = table->schema_version;
  } else { // secondary index
    index_id_to_set = get_key_tindex_id(kd_in);
    schema_version_to_set = table->key_info[key_no].schema_version;
  }

  assert(index_id_to_set != tdsql::kInvalidIndexId);
  assert(schema_version_to_set != tdsql::kInvalidSchemaVersion);

  thd->rpc_ctx()->set_schema_id(index_id_to_set);
  thd->rpc_ctx()->set_schema_version(schema_version_to_set);
  thd->rpc_ctx()->set_table(table);

  LogDebug(
      "[set_write_fence(const Rdb_key_def*)][table:%s.%s]"
      "[keyno:%u][key:%s][tindex_id:%u][schema_version:%u][query:%s]",
      table->s->db.str, table->s->table_name.str, key_no,
      kd_in->get_name().c_str(), index_id_to_set, schema_version_to_set,
      thd->query().str);
}

void ha_rocksdb::set_write_fence(Iterator *iter) {
  THD *thd = table->in_use;

  uint key_no = iter->keyno();
  assert(iter->tindex_id() != tdsql::kInvalidIndexId);

  tdsql::SchemaVersion schema_version_to_set = tdsql::kInvalidSchemaVersion;

  if (is_hidden_pk(key_no, table, m_tbl_def) || // hidden pk
      key_no == table->s->primary_key) { // user-defined pk
    assert(iter->tindex_id() == get_key_tindex_id());
    schema_version_to_set = table->schema_version;
  } else { // secondary index
    assert(iter->tindex_id() ==
                m_key_descr_arr[iter->keyno()]->get_index_number());
    schema_version_to_set = table->key_info[key_no].schema_version;
  }

  assert(schema_version_to_set != tdsql::kInvalidSchemaVersion);
  iter->set_write_fence(schema_version_to_set);
  thd->rpc_ctx()->set_table(table);

  LogDebug(
      "[set_write_fence(Iterator*)][table:%s.%s]"
      "[keyno:%u][key:%s][tindex_id:%u][schema_version:%u][query:%s]",
      table->s->db.str, table->s->table_name.str, key_no,
      m_key_descr_arr[key_no]->get_name().c_str(), iter->tindex_id(),
      schema_version_to_set, thd->query().str);
}

void ha_rocksdb::set_write_fence() {
  THD *thd = current_thd;
  assert(thd->get_tdsql_trans());

  assert(get_key_tindex_id() != tdsql::kInvalidIndexId);
  assert(table->schema_version != tdsql::kInvalidSchemaVersion);

  thd->rpc_ctx()->set_schema_id(get_key_tindex_id());
  thd->rpc_ctx()->set_schema_version(table->schema_version);
  thd->rpc_ctx()->set_table(table);

  LogDebug(
      "[set_write_fence(void)][table:%s.%s]"
      "[tindex_id:%u][schema_version:%u][query:%s]",
      table->s->db.str, table->s->table_name.str, get_key_tindex_id(),
      table->schema_version, thd->query().str);
}

/**
 @brief this function will get tindex id for pk or sk.
 */
uint32 ha_rocksdb::get_key_tindex_id(const Rdb_key_def* kd) {
  tdsql::IndexId tindex_id =
      kd ? kd->get_index_number() : m_tbl_def->m_tindex_id;
  assert(tindex_id != tdsql::kInvalidIndexId);
  return tindex_id;
}

/* TDSQL: Put normal record into TDStore */
int ha_rocksdb::rpc_put_record(
    tdsql::Transaction* tx,
    const rocksdb::Slice &key_slice,
    const rocksdb::Slice &value_slice,
    tdstore::PutRecordType &&type) {

  if (tdsql::ddl::HandlerCheckInDDTrans(tx->thd(), table->tindex_id)) {
    return TDSQL_EXIT_FAILURE;
  }

  if (IsDeleteOnly((Schema_Status)(table->schema_status)) ||
      IsNoWrite((Schema_Status)(table->schema_status))) {
    return HA_ERR_TABLE_READONLY;
  }

  if (IsNoWrite((Schema_Status)(table->schema_status))) {
    LogDebug("table '%s.%s' -> schema_status is %s, can't put record <key: %s, value: %s>",
        table->s->db.str, table->s->table_name.str,
        tdsql::ddl::print_schema_status(table->schema_status).c_str(),
        MemoryDumper::ToHex(key_slice.data(), key_slice.size()).c_str(),
        MemoryDumper::ToHex(value_slice.data(), value_slice.size()).c_str());

    return HA_ERR_TABLE_READONLY;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
                                  table->schema_version, table->s->db.str,
                                  table->s->table_name.str, m_tbl_def)) {
    LogError("[check_or_set_schema_version failed] [table: %s.%s]"
             "[tindex_id=%u] [schema_version=%u]", table->s->db.str,
             table->s->table_name.str,  table->tindex_id,
             table->schema_version);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  PutRecordCntl cntl(tx->thd(), key_slice.ToString(), value_slice.ToString(), type);
  assert(cntl.get_thd() == tx->thd());
  assert(cntl.get_tx() == tx);

  return cntl.Execute();
}

int ha_rocksdb::flush_batch_record_in_parallel() {
  DBUG_ENTER_FUNC();
  // Used for parallel task only.
  assert(in_parallel_data_fillback_task());
  assert(m_parallel_thomas_put);

  int retval = TDSQL_EXIT_SUCCESS;

  if (in_parallel_data_fillback_task() && m_parallel_thomas_put) {
    if (m_parallel_thomas_put->RecordNum() > 0) {
      retval = m_parallel_thomas_put->Execute();
      m_parallel_thomas_put->Clear();
    }
  } else {
    retval = TDSQL_EXIT_FAILURE;
    LogError("[ParallelThomasRulePutCntl not inited] [error happened]");
  }

  DBUG_RETURN(retval);
}

int ha_rocksdb::bulk_cache_key(tdsql::Transaction *tx,
                               const rocksdb::Slice &key_slice,
                               const rocksdb::Slice &value_slice) {
  assert(tx);
  return tx->AddKvToBatchPut(CONVERT_TO_TDSQL_SLICE(key_slice),
                             CONVERT_TO_TDSQL_SLICE(value_slice));
}

int ha_rocksdb::bulk_cache_key_in_parallel(const rocksdb::Slice &key_slice,
                                           const rocksdb::Slice &value_slice) {
  // Used for parallel task only.
  assert(in_parallel_data_fillback_task());
  assert(m_parallel_thomas_put);

  int rc = TDSQL_EXIT_SUCCESS;

  if (in_parallel_data_fillback_task() && m_parallel_thomas_put) {
    rc = m_parallel_thomas_put->AddKV(CONVERT_TO_TDSQL_SLICE(key_slice),
                                      CONVERT_TO_TDSQL_SLICE(value_slice),
                                      kInvalidIndexId, kInvalidSchemaVersion);
  } else {
    rc = TDSQL_EXIT_FAILURE;
    LogError("[ParallelThomasRulePutCntl not inited] [error happened]");
  }

  return rc;
}

// flush cached batch key-value to tdstore
int ha_rocksdb::flush_cache_key(tdsql::Transaction *tx) {
  // TODO: consider commit_in_the_middle
  // return commit_in_the_middle() &&
  assert(tx);
  int rc = TDSQL_EXIT_SUCCESS;
  if (tx->BatchRecordCount() >= THDVAR(table->in_use, bulk_load_size) ||
      tx->BatchRecordSizeExceed()) {
    rc = tx->FlushBatchRecord();
    if (commit_in_the_middle(table->in_use)) {
      rc = tx->commit();
    }
  }
  return rc;
}

int ha_rocksdb::flush_cache_key_in_parallel() {
  DBUG_ENTER_FUNC();
  // Used for parallel task only.
  assert(in_parallel_data_fillback_task());
  assert(m_parallel_thomas_put);

  int rc = TDSQL_EXIT_SUCCESS;

  if (in_parallel_data_fillback_task() && m_parallel_thomas_put) {
    if (m_parallel_thomas_put->RecordNum() >=
            THDVAR(table->in_use, bulk_load_size) ||
        m_parallel_thomas_put->BatchSizeExceed())
      rc = flush_batch_record_in_parallel();
  } else {
    rc = TDSQL_EXIT_FAILURE;
    LogError("[ParallelThomasRulePutCntl not inited] [error happened]");
  }

  DBUG_RETURN(rc);
}

/* TDSQL: Get normal records from TDStore */
int ha_rocksdb::rpc_get_record(
    tdsql::Transaction* tx,
    const rocksdb::Slice &key_slice,
    rocksdb::PinnableSlice* value_slice,
    const bool pk_point_select MY_ATTRIBUTE((unused))) {

  if (!tdsql::ddl::GetDDLJob(current_thd) &&
      !table->correlate_src_table &&
      IsUnReadable((Schema_Status)(table->schema_status))) {
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
          table->schema_version, table->s->db.str, table->s->table_name.str)) {
    LogError("[check_or_set_schema_version failed] [table: %s.%s]"
             "[tindex_id=%u] [schema_version=%u]", table->s->db.str,
             table->s->table_name.str,  table->tindex_id,
             table->schema_version);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  int ret = TDSQL_EXIT_SUCCESS;
  auto SetValueSlice =
      [](rocksdb::PinnableSlice* val, int exe_ret, GetRecordCntlBase* cntl) {
        if (exe_ret == TDSQL_EXIT_SUCCESS && val) {
          if (!val->empty() || val->IsPinned()) {
            val->Reset();
          }
          val->PinSelf(cntl->value());
        }
      };

  THD* thd = tx->thd();
  if (ReadWithoutTrans(thd)) {
    GetSingleRecordCntl cntl(thd, key_slice.ToString());
    assert(cntl.get_thd() == thd);
    assert(cntl.get_tx() == tx);
    ret = cntl.Execute();
    SetValueSlice(value_slice, ret, &cntl);
  } else {
    GetRecordCntl cntl(thd, key_slice.ToString());
    assert(cntl.get_thd() == thd);
    assert(cntl.get_tx() == tx);
    ret = cntl.Execute();
    SetValueSlice(value_slice, ret, &cntl);
  }

  return ret;
}

/* TDSQL: Delete normal records from TDStore */
int ha_rocksdb::rpc_del_record(
    tdsql::Transaction* tx,
    const rocksdb::Slice &key_slice,
    bool with_extra_info) {

  if (tdsql::ddl::HandlerCheckInDDTrans(tx->thd(), table->tindex_id)) {
    return TDSQL_EXIT_FAILURE;
  }

  if (IsNoWrite((Schema_Status)(table->schema_status))) {
    LogDebug("table '%s.%s' -> schema_status is %s, can't delete record <key: %s>",
        table->s->db.str, table->s->table_name.str,
        tdsql::ddl::print_schema_status(table->schema_status).c_str(),
        MemoryDumper::ToHex(key_slice.data(), key_slice.size()).c_str());

    return HA_ERR_TABLE_READONLY;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
                                  table->schema_version, table->s->db.str,
                                  table->s->table_name.str, m_tbl_def)) {
    LogError("[check_or_set_schema_version failed] [table: %s.%s]"
             "[tindex_id=%u] [schema_version=%u]", table->s->db.str,
             table->s->table_name.str,  table->tindex_id,
             table->schema_version);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  DelRecordCntl cntl(tx->thd(), key_slice.ToString());

  /*
    Skip system tables currently, delete rpc can be called directly
    with out get rpc in some scenes. Remember to get record again
    just like correlate tables do once the restriction for system
    tables is removed.
  */
  int ret = HA_EXIT_SUCCESS;
  if (with_extra_info && !tdsql::IsSystemTable(table->tindex_id)) {
    /*
      A little tough to handle correlate table, since it would not be here
      in a normar way(read and delete). m_retrieved_record might be empty here.
      For example,
      |-ha_rocksdb::update_row // src table
      |--CorrelateOption::DeleteRow // correlate table
      |---delete rpc // m_retrieved_record is not set
    */
    if (m_retrieved_record.size() == 0 && table->correlate_src_table) {
      ret = m_tbl_def->ConvertRecordToStorageFormat(table, &m_pk_unpack_info, m_storage_record);
      if (ret != HA_EXIT_SUCCESS) return ret;
      // m_retrieved_record is incomplete, but enough for us.
      m_retrieved_record.PinSelf(rocksdb::Slice(m_storage_record.ptr(), m_storage_record.length()));
    }

    if (ret == HA_EXIT_SUCCESS) {
      uint extra_info_size = 0;
      ret = m_tbl_def->GetExtraInfo(table, &key_slice, &m_retrieved_record, &extra_info_size);

      if (ret) return ret;
      cntl.SetExtraInfo(m_retrieved_record.data(), extra_info_size);
    } else {
      return ret;
    }
  }
  assert(cntl.get_thd() == tx->thd());
  assert(cntl.get_tx() == tx);

  return cntl.Execute();
}

int ha_rocksdb::init_tdsql_iterator(
    tdsql::Transaction* &tx,
    const rocksdb::Slice& index_prefix,
    const rocksdb::Slice& start_slice,
    const rocksdb::Slice& end_slice,
    bool is_forward,
    bool system_table [[maybe_unused]],
    uint32 tindex_id,
    uint keyno) {

  int rc = 0;
  uint32 write_fence;

  release_td_scan_iterator();

  if (is_pk(keyno, table, m_tbl_def))
    write_fence = table->schema_version;
  else
    write_fence = table->key_info[keyno].schema_version;

  table->in_use->rpc_ctx()->set_table(table);

  // Do not need to check readable during DDL execution
  if (!tdsql::ddl::GetDDLJob(table->in_use) &&
      !table->correlate_src_table &&
      IsUnReadable((Schema_Status)(table->schema_status))) {
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
          table->schema_version, table->s->db.str, table->s->table_name.str)) {
    LogError("[check_or_set_schema_version failed] [table: %s.%s]"
             "[tindex_id=%u] [schema_version=%u]", table->s->db.str,
             table->s->table_name.str,  table->tindex_id,
             table->schema_version);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  rc = tdsql::CreateIterator(
      tx->thd(), this, 0 /*iterator_id*/, CONVERT_TO_TDSQL_SLICE(index_prefix),
      CONVERT_TO_TDSQL_SLICE(start_slice), CONVERT_TO_TDSQL_SLICE(end_slice),
      &m_td_scan_it, is_forward, keyno, tindex_id, write_fence,
      false /*in_parallel_fillback_index*/);

  if (rc) return rc;

  rc = BuildPushDownPb(keyno);

  if (!rc && tdstore_pushed_cond_pb_) {
    LogDebug("iterator with pushed condition");
    // plan change should not happen any more
    assert(keyno == tdstore_pushed_cond_idx_);

    DBUG_EXECUTE_IF("plan_changed_after_build_pb",
                    { keyno = tdstore_pushed_cond_idx_ + 1; });

    // we have remove the condition from table,if we can not push,need return error
    if (keyno != tdstore_pushed_cond_idx_) {
      LogError("plan changed after build tdstore_pushed_cond_pb_,return fail");
      delete tdstore_pushed_cond_pb_;
      tdstore_pushed_cond_pb_ = NULL;
      m_projection = 0;
      return TDSQL_EXIT_FAILURE;
    } else {
      m_td_scan_it->push_cond(tdstore_pushed_cond_pb_);
      m_really_push = 1;
    }
  }

  if (!rc && tdsql::EnableLimitCondPushDown(this)) {
    limit_cond_pushdown->set_curr_num(0);
  }

  return rc;
}

int ha_rocksdb::init_tdsql_iterator(
    const rocksdb::Slice& index_prefix,
    const rocksdb::Slice& start_slice,
    const rocksdb::Slice& end_slice,
    uint64 iterator_id,
    uint32 tindex_id, uint keyno) {
  int rc = 0;
  uint32 write_fence;

  release_td_scan_iterator();

  if (is_pk(keyno, table, m_tbl_def))
    write_fence = table->schema_version;
  else
    write_fence = table->key_info[keyno].schema_version;

  rc = tdsql::CreateIterator(
      current_thd, this, iterator_id, CONVERT_TO_TDSQL_SLICE(index_prefix),
      CONVERT_TO_TDSQL_SLICE(start_slice), CONVERT_TO_TDSQL_SLICE(end_slice),
      &m_td_scan_it, true /*is_forward*/, keyno, tindex_id, write_fence,
      true /*in_parallel_fillback_index*/);

  return rc;
}

std::string ha_rocksdb::partition_tindex_ids_to_string() {
#ifndef NDEBUG
  dbg_assert_part_elem(table);
#endif

  char buff[256];
  snprintf(buff, sizeof(buff),
           "<[tindex_id:%u][hidden_pk_autoinc_tindex_id:%u][indexes:%s]>",
           m_part_elem->tindex_id, m_part_elem->hidden_pk_autoinc_tindex_id,
           m_part_elem->partition_index_tindex_id_array.ToString().c_str());

  return buff;
}

/*
  Release m_tbl_def if create_tbl_def failed.
*/
void ha_rocksdb::delete_tbl_def_cache() {
  assert(m_tbl_def);
  m_tbl_def = nullptr;
  m_key_descr_arr = nullptr;
}

void ha_rocksdb::start_bulk_insert(ha_rows) {
  THD* thd = table->in_use;
  /* Do nothing if tdsql_enable_bulk_insert not set. */
  if (!thd->variables.tdsql_enable_bulk_insert)
    return;

  assert(m_bulk_load_closure == nullptr);
  m_bulk_load_closure = new Bulk_load_closure(table->in_use);

  if (in_parallel_data_fillback_task()) {
    uint64_t begin_ts = thd->get_tdsql_trans()->begin_ts();
    assert(begin_ts > 0);
    m_parallel_thomas_put.reset(new tdsql::ThomasRulePutCntl(thd, begin_ts));
  }
}

int ha_rocksdb::end_bulk_insert() {
  DBUG_ENTER_FUNC();
  int retval = HA_EXIT_SUCCESS;
  /* Do nothing if tdsql_enable_bulk_insert not set. */
  if (!table->in_use->variables.tdsql_enable_bulk_insert)
    DBUG_RETURN(retval);

  if (in_parallel_data_fillback_task()) {
    retval = flush_batch_record_in_parallel();
    m_parallel_thomas_put.reset(nullptr);
  } else {
    tdsql::Transaction* tx = table->in_use->get_tdsql_trans();
    if (tx) {
      retval = tx->FlushBatchRecord();
    }
  }

  if (m_bulk_load_closure) {
    delete m_bulk_load_closure;
    m_bulk_load_closure = nullptr;
  }

  /*
    We don't use bulk insert for unique key yet,
    so we don't check uniqueness for unique key.
    TODO: Check uniqueness in phase2.
  */
  DBUG_RETURN(retval);
}

int ha_rocksdb::open_init_hidden_pk(TABLE *const table_arg) {
  assert(has_hidden_pk(table_arg));
  uint32 index_id = 0;
  if (is_partition_table()) {
#ifndef NDEBUG
    dbg_assert_part_elem(table_arg);
#endif
    index_id = m_part_elem->hidden_pk_autoinc_tindex_id;
    assert(index_id != tdsql::kInvalidIndexId);
  } else {
    index_id = table_arg->s->hidden_pk_autoinc_tindex_id;
  }

  int ret = m_hidden_pk_generator.init_on_open_table(index_id);
  if (ret != TDSQL_EXIT_SUCCESS) {
    LogError(
        "[m_hidden_pk_generator.init_on_open_table() "
        "failed][table:%s.%s][hidden_pk_autoinc_tindex_id:%u]",
        table_arg->s->db.str, table_arg->s->table_name.str, index_id);
  }

  return ret;
}

int ha_rocksdb::open_init_auto_incr(TABLE *const table_arg) {
   if (IsTmpTableForLogService(table)) return 0;
  assert(table_arg);
  assert(table_arg->found_next_number_field);
  assert(table_arg->s);
  assert(!is_partition_table());
  int ret = m_autoinc_generator.init_on_open_table(table_arg->s->autoinc_tindex_id,
      table_arg->found_next_number_field->key_type());

  if (ret != 0) {
    LogError(
        "[m_autoinc_generator.init_on_open_table failed][table:%s.%s]"
        "[autoinc_tindex_id:%u]",
        table_arg->s->db.str, table_arg->s->table_name.str,
        table_arg->s->autoinc_tindex_id);
  }

  return ret;
}

/*
  Check if given index has duplicated record.
*/
int ha_rocksdb::check_index_dup_key(const Rdb_key_def &kd) {
  DBUG_ENTER_FUNC();
  int rc = 0;

  THD* thd = table->in_use;
  tdsql::Transaction *tx = tdsql::get_or_create_tx(thd, rc);
  if (rc != HA_EXIT_SUCCESS) {
    DBUG_RETURN(rc);
  }

  free_key_buffers();
  if (alloc_key_buffers(table, m_tbl_def, true)) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0));
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  struct unique_sk_buf_info sk_info;
  sk_info.dup_sk_buf = m_dup_sk_packed_tuple;
  sk_info.dup_sk_buf_old = m_dup_sk_packed_tuple_old;

  uchar buf[Rdb_key_def::INDEX_NUMBER_SIZE * 2];
  rocksdb::Range range = get_range(kd.get_keyno(), buf);

  const rocksdb::Slice& start_key = range.start;
  const rocksdb::Slice& end_key = range.limit;

  tdsql::ddl::ReginfoSetLockReadClosure set_read_lock(table);

  if ((rc = init_tdsql_iterator(tx,
                                start_key, /* index_prefix */
                                start_key,
                                end_key,
                                true,     /* is_forward */
                                false,    /* system_table */
                                get_key_tindex_id(&kd),
                                kd.get_keyno())) != HA_EXIT_SUCCESS) {
  }

  while(true) {
    rc = m_td_scan_it->Next();
    if (is_error_iterator_no_more_rec(rc)) {
      break;
    } else if (is_error(rc)) {
      DBUG_RETURN(rc);
    }

    rocksdb::Slice key = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->key());
    rocksdb::Slice value = CONVERT_TO_ROCKSDB_SLICE(m_td_scan_it->value());

    if (check_duplicate_sk(table, kd, &key, &sk_info)) {
      /*
         Duplicate entry found when trying to create unique secondary key.
         We need to unpack the record into new_table_arg->record[0] as it
         is used inside print_keydup_error so that the error message shows
         the duplicate record.
         */
      if (kd.unpack_record(table, table->record[0], &key, &value, 0)) {
        /* Should never reach here */
        assert(0);
      }

      print_keydup_error(table,
          &table->key_info[kd.get_keyno()],
          MYF(0));
      DBUG_RETURN(ER_DUP_ENTRY);
    }
  }

  // we don't care about this return value
  {
    Dummy_error_handler error_handler;
    thd->push_internal_handler(&error_handler);
    tx->rollback();
    thd->pop_internal_handler();
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

bool GetStartEndKeyTableScan(ha_rocksdb *handler,
                             StartEndKeys *start_end_keys) {
  int ret = handler->ha_rnd_init(true);
  if (ret) {
    LogError("rnd_init fail");
    return ret;
  }

  tdsql::Iterator *it = handler->get_iterator();
  if (!it) {
    LogError("get_iterator fail");
    return true;
  }

  start_end_keys->emplace_back(it->start_key(), it->end_key());

  return false;
}

bool GetStartEndKeyIndexScan(ha_rocksdb *handler, int index,
                             StartEndKeys *start_end_keys) {
  int ret = handler->ha_index_init(index, 0);
  TABLE *table = handler->get_table();
  if (ret) {
    LogError("index_init fail");
    return ret;
  }

  ret = handler->ha_index_first(table->record[0]);
  if (ret) {
    LogError("index_first fail");
    return ret;
  }
  tdsql::Iterator *it = handler->get_iterator();
  if (!it) {
    LogError("get_iterator fail");
    return true;
  }

  start_end_keys->emplace_back(it->start_key(), it->end_key());
  LogDebug("start_key:%s,end_key:%s",
           MemoryDumper::ToHex(it->start_key()).data(),
           MemoryDumper::ToHex(it->end_key()).data());

  return false;
}

bool GetStartEndKeyRef(ha_rocksdb *handler, TABLE_REF *ref,
                       StartEndKeys *start_end_keys) {
  int ret = handler->ha_index_init(ref->key, 0);
  if (ret) {
    LogError("index_init fail");
    return true;
  }

  ret = handler->ha_index_read_map(nullptr, ref->key_buff,
                                make_prev_keypart_map(ref->key_parts),
                                HA_READ_KEY_EXACT);
  if (ret) {
    LogError("index_read_map fail");
    return true;
  }

  tdsql::Iterator *it = handler->get_iterator();
  if (!it) {
    LogError("get_iterator fail");
    return true;
  }

  start_end_keys->emplace_back(it->start_key(), it->end_key());
  return false;
}

bool GetStartEndKeyRefORNull(ha_rocksdb *handler, TABLE_REF *ref,
                             StartEndKeys *start_end_keys) {
  bool ret = GetStartEndKeyRef(handler, ref, start_end_keys);

  if (ret) {
    LogError("GetStartEndKeyRef for not-null fail");
    return true;
  }

  handler->ha_index_end();

  // add null
  *ref->null_ref_key = true;

  ret = GetStartEndKeyRef(handler, ref, start_end_keys);

  if (ret) {
    LogError("GetStartEndKeyRef for null fail");
    return true;
  }

  return false;
}

bool GetStartEndKeyRange(ha_rocksdb *handler, QUICK_SELECT_I *quick,
                         StartEndKeys *start_end_keys) {
  if (!quick || quick->get_type() != QUICK_SELECT_I::QS_TYPE_RANGE) {
    LogError("quick is NULL");
    return true;
  }

  int ret = handler->ha_index_init(quick->index, 0);
  if (ret) {
    LogError("index_init fail");
    return true;
  }

  KEY_MULTI_RANGE mrr_cur_range;
  range_seq_t mrr_iter = quick_range_seq_init(quick, 0, 0);

  int range_res = 0;
  while (!(range_res = quick_range_seq_next(mrr_iter, &mrr_cur_range))) {
    ret = handler->ha_read_range_first(
        mrr_cur_range.start_key.keypart_map ? &mrr_cur_range.start_key
                                            : nullptr,
        mrr_cur_range.end_key.keypart_map ? &mrr_cur_range.end_key : nullptr,
        mrr_cur_range.range_flag & EQ_RANGE, 0);
    if (ret) {
      LogError("read_range_first fail");
      return true;
    }

    tdsql::Iterator *it = handler->get_iterator();
    if (!it) {
      LogError("get_iterator fail");
      return true;
    }
    start_end_keys->emplace_back(it->start_key(), it->end_key());
  }

  return false;
}

// return true if fail
bool GetStartEndKey(ha_rocksdb *handler, StartEndKeys *start_end_keys) {
  TABLE *table = handler->get_table();

  const QEP_TAB *qep_tab = table->reginfo.qep_tab;

  handler->SetOnlyInitIterator(true);
  auto release_handler = create_scope_guard([&]() {
    handler->ha_index_or_rnd_end();
    handler->SetOnlyInitIterator(false);
  });

  // without qep_tab,just use table scan ,ddl_execute use this
  if (!qep_tab) return GetStartEndKeyTableScan(handler, start_end_keys);

  int ret = 0;

  switch (qep_tab->type()) {
    case JT_REF: {
      TABLE_REF *ref = &qep_tab->ref();
      ret = GetStartEndKeyRef(handler, ref, start_end_keys);
      break;
    }
    case JT_REF_OR_NULL: {
      TABLE_REF *ref = &qep_tab->ref();
      ret = GetStartEndKeyRefORNull(handler, ref, start_end_keys);
      break;
    }
    case JT_RANGE: {
      QUICK_SELECT_I *quick = qep_tab->quick();
      ret = GetStartEndKeyRange(handler, quick, start_end_keys);
      break;
    }
    case JT_ALL:
      ret = GetStartEndKeyTableScan(handler, start_end_keys);
      break;
    case JT_INDEX_SCAN: {
      int key = qep_tab->index();
      ret = GetStartEndKeyIndexScan(handler, key, start_end_keys);
      break;
    }
    default:
      LogDebug("type:%d should not use count pushdown", qep_tab->type());
      ret = true;
  }

  return ret;
}

int ha_rocksdb::get_row_count(ha_rows *num_rows) {
  return get_row_count_from_key(num_rows, pk_index(table, m_tbl_def));
}

int ha_rocksdb::get_row_count_from_index(ha_rows *num_rows, uint index) {
  assert(index < m_tbl_def->m_key_count);
  return get_row_count_from_key(num_rows, index);
}

int ha_rocksdb::request_row_count(THD *thd, uint index, uint64_t *rows,
                                  const std::string &start,
                                  const std::string &end) {
  tdsql::Transaction *const tx = tdsql::get_tx_from_thd(thd);
  assert(tx);
  uint64_t iterator_id = tx->GenerateIteratorID();
  tdsql::RowCountCntl cntl(tx->thd(), start, end, iterator_id,
                           tdstore_pushed_cond_pb_);
  int ret = cntl.Execute();
  if (ret != TDSQL_EXIT_SUCCESS) {
    LogError(
        "[ha_rocksdb::get_row_count_from_key] RowCountCntl.Execute() "
        "failed. "
        "trans_id:%lu, ret=%d",
        tx->trans_id(), ret);
    return ret;
  }
  *rows = cntl.rows();

  const Rdb_key_def& key =
      is_pk(index, table, m_tbl_def) ? *m_pk_descr : *m_key_descr_arr[index];
  LogInfo(
      "[ha_rocksdb::get_row_count_from_key] "
      "trans_id:%lu, table=%s, index=%s, rows=%lu",
      tx->trans_id(), m_tbl_def->full_tablename().c_str(),
      key.get_name().c_str(), *rows);

  return ret;
}

int ha_rocksdb::get_row_count_from_key(ha_rows *num_rows, uint index) {
  DBUG_EXECUTE_IF("myrocks_get_row_count_fail", {
    *num_rows = HA_POS_ERROR;
    return TDSQL_EXIT_FAILURE;
  });

  DBUG_EXECUTE_IF("bug28079850", {
    table->in_use->killed = THD::KILL_QUERY;
    *num_rows = HA_POS_ERROR;
    return 0;
  });

  assert(num_rows);
  assert(table);
  assert(table->s);

  THD* thd = current_thd;
  assert(thd);

  int ret = TDSQL_EXIT_SUCCESS;

  tdsql::Transaction* const tx = tdsql::get_or_create_tx(thd, ret);
  if (ret != TDSQL_EXIT_SUCCESS) {
    *num_rows = HA_POS_ERROR;
    return ret;
  }
  assert(tx);

  pk_preload_push_ = false;

  ret = BuildPushDownPb(index);

  if (ret) return ret;

  if(tdstore_pushed_cond_pb_ && index != tdstore_pushed_cond_idx_) {
    *num_rows = HA_POS_ERROR;
    LogError("ha_rocksdb::get_row_count_from_key] plan changed."
            "trans_id:%lu", tx->trans_id());
    return TDSQL_EXIT_FAILURE;
  }

  if (!index_is_usable(tx, index, table, m_tbl_def)) {
    LogError("table %s.%s index <idx: %u, name: %s> is unusable, "
             "table definition has changed, need restart transaction",
             table->s->db.str, table->s->table_name.str,
             index, get_key_name(index, table, m_tbl_def));
    *num_rows = HA_POS_ERROR;
    return HA_ERR_TABLE_DEF_UNUSABLE;
  }

  const Rdb_key_def& key =
      is_pk(index, table, m_tbl_def) ? *m_pk_descr : *m_key_descr_arr[index];
  set_write_fence(&key);

  if (!tdsql::ddl::GetDDLJob(current_thd) &&
      !table->correlate_src_table &&
      IsUnReadable((Schema_Status)(table->schema_status))) {
    *num_rows = HA_POS_ERROR;
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
          table->schema_version, table->s->db.str, table->s->table_name.str)) {
    LogError("[check_or_set_schema_version failed] [table:%s.%s]"
             "[tindex_id=%u] [schema_version=%u]",
             table->s->db.str, table->s->table_name.str,
             table->tindex_id, table->schema_version);
    *num_rows = HA_POS_ERROR;
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  StartEndKeys start_end_keys;
  if (use_parallel_scan()) {
    AttachAParallelScanJob();
    auto *cur_scan_job = parallel_scan_job();
    while (cur_scan_job) {
      uint64_t rows = HA_POS_ERROR;
      if ((ret = ha_rocksdb::request_row_count(thd, index, &rows,
                                        cur_scan_job->lower_bound,
                                               cur_scan_job->upper_bound)) != TDSQL_EXIT_SUCCESS)
      {
        *num_rows = HA_POS_ERROR;
        return TDSQL_EXIT_FAILURE;
      }
      *num_rows += rows;

      AttachAParallelScanJob();
      cur_scan_job = parallel_scan_job();
    }

    return ret;
  } else if (GetStartEndKey(this, &start_end_keys)) {
    LogError("GetStartEndKey error");
    *num_rows = HA_POS_ERROR;
    return TDSQL_EXIT_FAILURE;
  }

  for (uint i = 0; i < start_end_keys.size(); i++) {
    std::string &start = start_end_keys[i].first;
    std::string &end = start_end_keys[i].second;

    uint64_t rows = HA_POS_ERROR;

    if (current_thd->variables.tdsql_parallel_optim) {
      uint64_t iterator_id = tx->GenerateIteratorID();
      tdsql::parallel::ParallelRowCount paral_count(
          start, end, iterator_id, tdstore_pushed_cond_pb_, tx,
          current_thd->variables.tdsql_parallel_thread_number);
      ret = paral_count.Run();
      if (ret != TDSQL_EXIT_SUCCESS) {
        *num_rows = HA_POS_ERROR;
        LogError(
            "[ha_rocksdb::get_row_count_from_key] ParallelRowCount.Run() "
            "failed. "
            "trans_id:%lu, ret:%d",
            tx->trans_id(), ret);
        return ret;
      }
      ret = paral_count.GetResult(&rows);
      if (ret != TDSQL_EXIT_SUCCESS) {
        *num_rows = HA_POS_ERROR;
        LogError(
            "[ha_rocksdb::get_row_count_from_key] ParallelRowCount.GetResult() "
            "failed. trans_id:%lu, ret:%d",
            tx->trans_id(), ret);
        return ret;
      }
      LogInfo(
          "[ha_rocksdb::get_row_count_from_key][parallel] "
          "trans_id:%lu, table=%s, index=%s, rows=%lu",
          tx->trans_id(), m_tbl_def->full_tablename().c_str(),
          key.get_name().c_str(), rows);
    } else {
      if ((ret = request_row_count(thd, index, &rows, start, end)) != TDSQL_EXIT_SUCCESS) {
        *num_rows = HA_POS_ERROR;
        return ret;
      }
    }

    *num_rows += (ha_rows)rows;
    LogDebug("get count for range:%s-%s,rows:%lu", MemoryDumper::ToHex(start).data(),
             MemoryDumper::ToHex(end).data(),rows);
  }

  global_stats.queries[QUERIES_COUNT_OPTIM].inc();

  return ret;
}

void ha_rocksdb::set_partition_pos(uint pos) {
  assert(m_part_pos == UINT32_MAX);
  assert(m_part_elem == nullptr);

  m_part_pos = pos;
  m_part_elem = table->get_partition_element(m_part_pos);
}

#ifndef NDEBUG
void ha_rocksdb::dbg_assert_part_elem(const TABLE* table) const {
  assert(m_part_elem);
  assert(m_part_elem ==
              table->part_info->get_part_elem_by_pos(m_part_pos));
  assert(m_part_elem->tindex_id != tdsql::kInvalidIndexId);
  if (table->s->keys) {
    assert(!m_part_elem->partition_index_tindex_id_array.Empty());
  }

  assert(m_part_elem->partition_index_tindex_id_array.Valid());
}
#endif

bool ha_rocksdb::use_stmt_optimize_load_data(tdsql::Transaction *tx) {
  if (in_parallel_data_fillback_task()) {
    return false;
  }
  assert(tx);
  return tx->stmt_optimize_load_data();
}

int ha_rocksdb::ValidateSchemaStatusBeforePutRecord(
    tdsql::Transaction *tx,
    const rocksdb::Slice &key_slice,  // for log
    const rocksdb::Slice &value_slice) {

  assert(tx);
  assert(table);
  assert(table->s);

  if (tdsql::ddl::HandlerCheckInDDTrans(tx->thd(), table->tindex_id)) {
    return TDSQL_EXIT_FAILURE;
  }

  if (IsDeleteOnly((Schema_Status)(table->schema_status)) ||
      IsNoWrite((Schema_Status)(table->schema_status))) {
    LogDebug(
        "[Can not put record][Table is read only][table:%s.%s][tindex_id:%u]"
        "[schema_version:%u][schema_status:%s][key:%s][value:%s]",
        table->s->db.str, table->s->table_name.str, table->tindex_id,
        table->schema_version,
        tdsql::ddl::print_schema_status(table->schema_status).c_str(),
        key_slice.ToString(true /*hex*/).c_str(),
        value_slice.ToString(true /*hex*/).c_str());
    return HA_ERR_TABLE_READONLY;
  }

  if (check_or_set_schema_version(tx->thd(), table->tindex_id,
                                  table->schema_version, table->s->db.str,
                                  table->s->table_name.str)) {
    LogError(
        "[check_or_set_schema_version failed][Table definition changed]"
        "[table:%s.%s][tindex_id:%u][schema_version:%u][schema_status:%s]"
        "[key:%s][value:%s]",
        table->s->db.str, table->s->table_name.str, table->tindex_id,
        table->schema_version,
        tdsql::ddl::print_schema_status(table->schema_status).c_str(),
        key_slice.ToString(true /*hex*/).c_str(),
        value_slice.ToString(true /*hex*/).c_str());
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  return TDSQL_EXIT_SUCCESS;
}

void ha_rocksdb::FixParallelScanRangeIfCrossIndex(const Rdb_key_def& kd) {
  if (RegionKey::KeyRangeCrossIndexId(
          CONVERT_TO_TDSQL_SLICE(m_scan_it_lower_bound_slice),
          CONVERT_TO_TDSQL_SLICE(m_scan_it_upper_bound_slice))) {
    // max_key may cross tindex id after Rdb_key_def.successor.
    // Adjust it to the next tindex id.
    memcpy(m_scan_it_upper_bound, kd.get_index_number_storage_form(),
           Rdb_key_def::INDEX_NUMBER_SIZE);
    kd.successor(m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
    m_scan_it_upper_bound_slice = rocksdb::Slice(
        (const char *)m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
  }
  assert(!RegionKey::KeyRangeCrossIndexId(
      CONVERT_TO_TDSQL_SLICE(m_scan_it_lower_bound_slice),
      CONVERT_TO_TDSQL_SLICE(m_scan_it_upper_bound_slice)));
}

int ha_rocksdb::GetParallelScanRegionListRefOrNull(
    uint keynr, key_range *min_key, key_range *max_key, RegionList *ref_rgns,
    RegionList *null_rgns, MyRocksParallelScan *paral_scan_handle) {
  // In RefOrNull, e.g., SELECT a FROM t WHERE a = 1 OR a IS NULL; min_key
  // stands for ref (a = 1) max_key stands for null (a IS NULL)
  const Rdb_key_def &kd =
      *m_key_descr_arr[keynr != MAX_KEY ? keynr : pk_index(table, m_tbl_def)];

  // Convert the null(a IS NULL) to storage format.
  assert(max_key && max_key->flag == HA_READ_KEY_EXACT && max_key->key);
  uint32_t packed_size =
      kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_lower_bound,
                          max_key->key, max_key->keypart_map);
  memcpy(m_scan_it_upper_bound, m_scan_it_lower_bound, packed_size);
  kd.successor(m_scan_it_upper_bound, packed_size);
  m_scan_it_lower_bound_slice =
      rocksdb::Slice((const char *)m_scan_it_lower_bound, packed_size);
  m_scan_it_upper_bound_slice =
      rocksdb::Slice((const char *)m_scan_it_upper_bound, packed_size);
  assert(m_scan_it_lower_bound_slice.compare(m_scan_it_upper_bound_slice) < 0);
  // myrocks key encoding guarantees that 'IS NULL' cant cross tindex id.
  // encoding:[tindex id][null bitmap][fields] --> null bitmap is 0 if IS NULL.
  assert(!RegionKey::KeyRangeCrossIndexId(
      CONVERT_TO_TDSQL_SLICE(m_scan_it_lower_bound_slice),
      CONVERT_TO_TDSQL_SLICE(m_scan_it_upper_bound_slice)));
  if (paral_scan_handle) {
    paral_scan_handle->set_null_lower_bound(m_scan_it_lower_bound_slice);
    paral_scan_handle->set_null_upper_bound(m_scan_it_upper_bound_slice);
  }

  ThreadRegionManager *rgn_mgr = tdsql::GetRegionManagerByKey(
      CONVERT_TO_TDSQL_SLICE(m_scan_it_lower_bound_slice));
  assert(rgn_mgr);

  // Get route of null.
  int ret = rgn_mgr->GetMultiRegion(
      m_scan_it_lower_bound_slice.ToString(false /*hex*/),
      m_scan_it_upper_bound_slice.ToString(false /*hex*/), null_rgns);
  if (ret != TDSQL_EXIT_SUCCESS) {
    LogError(
        "[GetParallelScanRegionListRefOrNull failed]"
        "[GetMultiRegion failed]"
        "[null_lower_bound:%s][null_upper_bound:%s]",
        m_scan_it_lower_bound_slice.ToString(true /*hex*/).data(),
        m_scan_it_upper_bound_slice.ToString(true /*hex*/).data());
    return ret;
  }

  // Convert the ref(a=1) to storage format.
  assert(min_key && min_key->flag == HA_READ_KEY_EXACT && min_key->key);
  packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_lower_bound,
                                    min_key->key, min_key->keypart_map);
  memcpy(m_scan_it_upper_bound, m_scan_it_lower_bound, packed_size);
  kd.successor(m_scan_it_upper_bound, packed_size);
  m_scan_it_lower_bound_slice =
      rocksdb::Slice((const char *)m_scan_it_lower_bound, packed_size);
  m_scan_it_upper_bound_slice =
      rocksdb::Slice((const char *)m_scan_it_upper_bound, packed_size);
  FixParallelScanRangeIfCrossIndex(kd);
  assert(m_scan_it_lower_bound_slice.compare(m_scan_it_upper_bound_slice) < 0);
  if (paral_scan_handle) {
    paral_scan_handle->set_lower_bound(m_scan_it_lower_bound_slice);
    paral_scan_handle->set_upper_bound(m_scan_it_upper_bound_slice);
  }

  // Get route of ref.
  ret = rgn_mgr->GetMultiRegion(
      m_scan_it_lower_bound_slice.ToString(false /*hex*/),
      m_scan_it_upper_bound_slice.ToString(false /*hex*/), ref_rgns);
  if (ret != TDSQL_EXIT_SUCCESS) {
    LogError(
        "[GetParallelScanRegionListRefOrNull failed]"
        "[GetMultiRegion failed]"
        "[ref_lower_bound:%s][ref_upper_bound:%s]",
        m_scan_it_lower_bound_slice.ToString(true /*hex*/).data(),
        m_scan_it_upper_bound_slice.ToString(true /*hex*/).data());
    return ret;
  }

  return ret;
}

// Get key range and regions of parallel scan.
int ha_rocksdb::GetParallelScanRegionList(
    uint keynr, key_range *min_key, key_range *max_key, RegionList *rgns,
    MyRocksParallelScan *paral_scan_handle) {
  // parallel_scan_desc_t denotes the scanned range in SQL layer format and a
  // conversion to storage format is necessary.
  const Rdb_key_def &kd =
      *m_key_descr_arr[keynr != MAX_KEY
                           ? keynr
                           : pk_index(table, m_tbl_def)];  // hidden pk;

  if (!min_key) {
    memcpy(m_scan_it_lower_bound, kd.get_index_number_storage_form(),
           Rdb_key_def::INDEX_NUMBER_SIZE);
    m_scan_it_lower_bound_slice = rocksdb::Slice(
        (const char *)m_scan_it_lower_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
  } else {
    uint32_t lower_packed_size =
        kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_lower_bound,
                            min_key->key, min_key->keypart_map);

    if (min_key->flag == HA_READ_KEY_OR_NEXT) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a, b));
      //      SELECT * FROM t WHERE a = 2 AND b >= 1;
      // noop.
    } else if (min_key->flag == HA_READ_KEY_EXACT) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a), INDEX idxb(b));
      //      SELECT b FROM t FORCE INDEX(idxb) WHERE b = 1;
      // noop.
    } else if (min_key->flag == HA_READ_AFTER_KEY) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a, b));
      //      SELECT * FROM t WHERE a = 2 AND b > 1;
      kd.successor(m_scan_it_lower_bound, lower_packed_size);
    } else {
      LogError(
          "[min_key only supports HA_READ_KEY_OR_NEXT | HA_READ_KEY_EXACT | "
          "HA_READ_AFTER_KEY][Unsupported read key function:%d]",
          min_key->flag);
      assert(0);
      return HA_ERR_ROCKSDB_PARALLEL_SCAN_FAILED;
    }

    m_scan_it_lower_bound_slice =
        rocksdb::Slice((const char *)m_scan_it_lower_bound, lower_packed_size);
  }

  if (!max_key) {
    memcpy(m_scan_it_upper_bound, kd.get_index_number_storage_form(),
           Rdb_key_def::INDEX_NUMBER_SIZE);
    kd.successor(m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
    m_scan_it_upper_bound_slice = rocksdb::Slice(
        (const char *)m_scan_it_upper_bound, Rdb_key_def::INDEX_NUMBER_SIZE);
  } else {
    uint32_t upper_packed_size =
        kd.pack_index_tuple(table, m_pack_buffer, m_scan_it_upper_bound,
                            max_key->key, max_key->keypart_map);

    if (max_key->flag == HA_READ_BEFORE_KEY) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a, b));
      //      SELECT * FROM t WHERE a = 2 AND b < 3;
      // noop.
    } else if (max_key->flag == HA_READ_KEY_EXACT) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a), INDEX idxb(b));
      //      SELECT b FROM t FORCE INDEX(idxb) WHERE b = 1;
      kd.successor(m_scan_it_upper_bound, upper_packed_size);
    } else if (max_key->flag == HA_READ_AFTER_KEY) {
      // E.g. CREATE TABLE t (a INT, b INT, PRIMARY KEY(a, b));
      //      SELECT * FROM t WHERE a = 2 AND b <= 3;
      kd.successor(m_scan_it_upper_bound, upper_packed_size);
    } else {
      LogError(
          "[max_key only supports HA_READ_BEFORE_KEY | HA_READ_KEY_EXACT | "
          "HA_READ_AFTER_KEY][Unsupported read key function:%d]",
          max_key->flag);
      assert(0);
      return HA_ERR_ROCKSDB_PARALLEL_SCAN_FAILED;
    }
    m_scan_it_upper_bound_slice =
        rocksdb::Slice((const char *)m_scan_it_upper_bound, upper_packed_size);
    FixParallelScanRangeIfCrossIndex(kd);
  }
  assert(m_scan_it_lower_bound_slice.compare(m_scan_it_upper_bound_slice) < 0);
  if (paral_scan_handle) {
    paral_scan_handle->set_lower_bound(m_scan_it_lower_bound_slice);
    paral_scan_handle->set_upper_bound(m_scan_it_upper_bound_slice);
  }

  // Get route.
  ThreadRegionManager *rgn_mgr = tdsql::GetRegionManagerByKey(
      CONVERT_TO_TDSQL_SLICE(m_scan_it_lower_bound_slice));
  assert(rgn_mgr);

  int ret = rgn_mgr->GetMultiRegion(
      m_scan_it_lower_bound_slice.ToString(false /*hex*/),
      m_scan_it_upper_bound_slice.ToString(false /*hex*/), rgns);
  if (ret != TDSQL_EXIT_SUCCESS) {
    LogError(
        "[GetParallelScanRegionList failed][GetMultiRegion failed]"
        "[lower_bound:%s][upper_bound:%s]",
        m_scan_it_lower_bound_slice.ToString(true /*hex*/).data(),
        m_scan_it_upper_bound_slice.ToString(true /*hex*/).data());
  }

  return ret;
}

int ha_rocksdb::init_parallel_scan(parallel_scan_handle_t *handle,
                                   ulong *nranges,
                                   parallel_scan_desc_t *scan_desc) {
  *handle = nullptr;
  // Get a snapshot.
  int ret = TDSQL_EXIT_SUCCESS;
  tdsql::Transaction *main_txn = tdsql::get_or_create_tx(table->in_use, ret);
  if (ret != TDSQL_EXIT_SUCCESS) {
    // TODO: a meaningful retval
    return ret;
  }
  assert(main_txn);
  assert(main_txn->trans_id());
  assert(scan_desc);

  // Take the concurrency of DDL and parallel query into consideration.
  table->in_use->rpc_ctx()->set_table(table);
  assert(!tdsql::ddl::GetDDLJob(table->in_use) && !table->correlate_src_table);
  if (IsUnReadable((Schema_Status)(table->schema_status))) {
    LogError(
        "[init_parallel_scan failed][IsUnReadable][HA_ERR_TABLE_DEF_CHANGED]"
        "[table:%s.%s][tindex_id:%u][schema_version:%u][schema_status:%u]",
        table->s->db.str, table->s->table_name.str, table->tindex_id,
        table->schema_version, table->schema_status);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  if (check_or_set_schema_version(main_txn->thd(), table->tindex_id,
                                  table->schema_version, table->s->db.str,
                                  table->s->table_name.str)) {
    LogError(
        "[init_parallel_scan failed][check_or_set_schema_version failed]"
        "[HA_ERR_TABLE_DEF_CHANGED][table:%s.%s][tindex_id:%u]"
        "[schema_version:%u][schema_status:%u]",
        table->s->db.str, table->s->table_name.str, table->tindex_id,
        table->schema_version, table->schema_status);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

  uint32_t keynr = scan_desc->keynr != MAX_KEY
                       ? scan_desc->keynr
                       : pk_index(table, m_tbl_def);  // hidden pk;
  const Rdb_key_def &kd = *m_key_descr_arr[keynr];

  MyRocksParallelScan *parallel_scan = new (std::nothrow) MyRocksParallelScan(
      main_txn, keynr, scan_desc->key_used, scan_desc->is_asc,
      scan_desc->is_ref_or_null, kd.get_index_number(),
      is_pk(keynr, table, m_tbl_def) ? table->schema_version
                                     : table->key_info[keynr].schema_version,
      ParallelScanTableSchemaInfo{table->s->db, table->s->table_name,
                                  table->tindex_id, table->schema_version,
                                  table->schema_status});
  if (!parallel_scan) {
    LogError("[init_parallel_scan failed][OOM]");
    return HA_ERR_OUT_OF_MEM;
  }

  if ((ret = parallel_scan->Init(scan_desc, this)) != TDSQL_EXIT_SUCCESS) {
    delete parallel_scan;
    return ret;
  }

  *handle = parallel_scan;
  *nranges = parallel_scan->parallel_scan_jobs().size();

  return TDSQL_EXIT_SUCCESS;
}

int ha_rocksdb::attach_parallel_scan(parallel_scan_handle_t scan_handle) {
  assert(scan_handle);
  assert(!m_parallel_scan_handle);
  assert(!m_parallel_scan_enabled);

  if ((static_cast<MyRocksParallelScan *>(scan_handle))
          ->CheckWorkerTable(table)) {
    LogError(
        "[attach_parallel_scan failed]"
        "[MyRocksParallelScan::CheckWorkerTable failed][table:%s.%s]",
        table->s->db.str, table->s->table_name.str);
    return HA_ERR_TABLE_DEF_CHANGED;
  }

#ifndef NDEBUG
  if ((static_cast<MyRocksParallelScan *>(scan_handle))
          ->WorkerHandlerAttach()) {
    return TDSQL_EXIT_FAILURE;
  }
#endif

  m_parallel_scan_handle = static_cast<MyRocksParallelScan *>(scan_handle);
  m_parallel_scan_enabled = true;

  return TDSQL_EXIT_SUCCESS;
}

void ha_rocksdb::detach_parallel_scan(
    parallel_scan_handle_t scan_handle MY_ATTRIBUTE((__unused__))) {
#ifndef NDEBUG
  if (m_parallel_scan_handle) {
    assert(m_parallel_scan_handle == scan_handle);
    m_parallel_scan_handle->WorkerHandlerDetach();
  }
#endif
  m_parallel_scan_handle = nullptr;
  m_parallel_scan_job = nullptr;
  m_parallel_scan_enabled = false;
}

void ha_rocksdb::end_parallel_scan(
    parallel_scan_handle_t scan_handle MY_ATTRIBUTE((__unused__))) {
  assert(scan_handle);
  // TODO: assert(scan_handle == m_parallel_scan_handle);
  MyRocksParallelScan *parallel_scan =
      static_cast<MyRocksParallelScan *>(scan_handle);
  parallel_scan->UpdateRouteAfterScanIfNecessary();
#ifndef NDEBUG
  parallel_scan->MainHandlerEnd();
#endif
  m_parallel_scan_handle = nullptr;
  m_parallel_scan_job = nullptr;
  delete parallel_scan;
}

int ha_rocksdb::restart_parallel_scan(
    parallel_scan_handle_t scan_handle MY_ATTRIBUTE((__unused__))) {
  return HA_ERR_UNSUPPORTED;
}

int ha_rocksdb::estimate_parallel_scan_ranges(uint keynr, key_range *min_key,
                                              key_range *max_key,
                                              bool type_ref_or_null,
                                              ulong *nranges, ha_rows *nrows) {
  int ret;
  if (type_ref_or_null) {
    RegionList ref_rgns, null_rgns;
    if ((ret = GetParallelScanRegionListRefOrNull(
             keynr, min_key, max_key, &ref_rgns, &null_rgns, nullptr)) !=
        TDSQL_EXIT_SUCCESS) {
      return ret;
    }
    *nrows = 1;
    if ((*nranges = ref_rgns.size() + null_rgns.size()) > 0) {
      *nrows = stats.records / *nranges;
    }
  } else {
    RegionList rgns;
    if ((ret = GetParallelScanRegionList(keynr, min_key, max_key, &rgns,
                                         nullptr)) != TDSQL_EXIT_SUCCESS)
      return ret;

    // Now one range each TDStore Region
    *nrows = 1;
    if ((*nranges = rgns.size()) > 0) *nrows = stats.records / *nranges;
  }

  return ret;
}

int ha_rocksdb::FlushStmtOptimizeLoadDataIfPossible(tdsql::Transaction *txn,
                                                    bool force) {
  int ret = HA_EXIT_SUCCESS;
  if (txn && use_stmt_optimize_load_data(txn) &&
      (force || txn->StmtOptimizeLoadDataBatchSizeExceed(m_part_pos))) {
    ret = txn->StmtOptimizeLoadDataFlush(m_part_pos);
    if (ret == HA_ERR_FOUND_DUPP_KEY) {
      ret = HandleStmtOptimizeLoadDataDuplicateEntry(ret);
    } else if (ret != HA_EXIT_SUCCESS) {
      LogError("[OptimizeLoadDataBatch::Flush failed][ret:%d][query:%s]", ret,
               table->in_use->query().str);
      my_error(
          ER_SQLENGINE_SYSTEM, MYF(0),
          "Failed to flush optimize load data batch, please check error log");
    }
  }
  return ret;
}

void ha_rocksdb::FreeRecordBufferIfNecessary() {
  if (m_storage_record.length() > tdsql_handler_record_buffer_size) {
    m_storage_record.mem_free();
  }

  if (m_last_rowkey.length() > tdsql_handler_record_buffer_size) {
    m_last_rowkey.mem_free();
  }
}

int ha_rocksdb::get_distribution_info_by_scan_range(
    uint keynr, key_range *min_key, key_range *max_key, bool is_ref_or_null,
    Fill_dist_info_cbk fill_dist_unit) {
  int res;
  RegionList rgns, null_rgns;
  if (unlikely(is_ref_or_null))
    res = GetParallelScanRegionListRefOrNull(keynr, min_key, max_key, &rgns,
                                             &null_rgns, nullptr);
  else
    res = GetParallelScanRegionList(keynr, min_key, max_key, &rgns, nullptr);

  if (res != TDSQL_EXIT_SUCCESS) return res;

  std::unordered_map<std::string, table_node_storage_info> aggregated_rgninfo;

  for (auto &rgn : rgns) {
    table_node_storage_info &tnsi = aggregated_rgninfo[rgn.tdstore_addr];
    ++tnsi.num_region;
  }

  for (auto &rgn : null_rgns) {
    table_node_storage_info &tnsi = aggregated_rgninfo[rgn.tdstore_addr];
    ++tnsi.num_region;
  }

  for (auto &pair : aggregated_rgninfo)
    fill_dist_unit(pointer_cast<Dist_unit_info>(&pair.second));

  return res;
}

std::string rdb_corruption_marker_file_name() {
  std::string ret(rocksdb_datadir);
  ret.append("/ROCKSDB_CORRUPTED");
  return ret;
}

int get_index_statistics(const Rdb_key_def& kd,
                         const rocksdb::Slice& start_key,
                         const rocksdb::Slice& end_key,
                         Index_range_info* index_range_info) {
  if (tdsql::ddl::InInitStage()) {
    // Data objects may not be created, yet.
    return TDSQL_EXIT_SUCCESS;
  }

  if (!index_range_info) { return TDSQL_EXIT_FAILURE; }
  index_range_info->clear();

  assert(current_thd);
  RpcRangeStatsCntl cntl(current_thd, start_key.ToString(), end_key.ToString());
  int ret = cntl.Execute();
  if (ret == TDSQL_EXIT_SUCCESS) {
    index_range_info->data_size = cntl.data_bytes();
    index_range_info->data_rows = cntl.data_rows();

    if (!tdsql_use_tdstore_estimated_rows || index_range_info->data_rows == 0) {
      uint row_len = kd.max_value_length() + kd.max_storage_fmt_length();
      if (row_len != 0) {
        index_range_info->data_rows = index_range_info->data_size/row_len;
      }
    }
  }

  return ret;
}

}  // namespace myrocks

//only return 0 if we sure data is gone after recheck
int drop_index(const tdsql::ddl::IndexInfoCollector* const indexes)
{
    int ret=myrocks::drop_index_internal(indexes);

    if (ret) {
      return ret;
    }

    THD* thd = current_thd;

    DDLLogInfo(thd, "[Check empty after drop_index_internal]");

    for (tdsql::ddl::IndexInfoCollector::ConstIterator citer = indexes->cbegin();
         citer != indexes->cend(); ++citer) {

      if (!citer->HasData()) {
        continue;
      }

      //we now only support normal cf
      const bool is_reverse_cf = 0;
      if (!myrocks::is_myrocks_index_empty(thd, is_reverse_cf, citer->index_id())) {
        DDLLogError(thd, "[Index is not empty][tindex_id:%u]", citer->index_id());
        return 1;
      }
    }

    return 0;
}

/*
  Register the storage engine plugin outside of myrocks namespace
  so that mysql_declare_plugin does not get confused when it does
  its name generation.
*/

struct st_mysql_storage_engine rocksdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(rocksdb_se){
    MYSQL_STORAGE_ENGINE_PLUGIN,       /* Plugin Type */
    &rocksdb_storage_engine,           /* Plugin Descriptor */
    "ROCKSDB",                         /* Plugin Name */
    "Monty Program Ab",                /* Plugin Author */
    "RocksDB storage engine",          /* Plugin Description */
    PLUGIN_LICENSE_GPL,                /* Plugin Licence */
    myrocks::rocksdb_init_func,        /* Plugin Entry Point */
    nullptr,                           /* Plugin Check Uninstall */
    myrocks::rocksdb_done_func,        /* Plugin Deinitializer */
    0x0001,                            /* version number (0.1) */
    myrocks::rocksdb_status_vars,      /* status variables */
    myrocks::rocksdb_system_variables, /* system variables */
    nullptr,                           /* config options */
    0,                                 /* flags */
} mysql_declare_plugin_end;
