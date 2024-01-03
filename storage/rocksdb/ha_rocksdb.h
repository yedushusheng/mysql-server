/*
   Copyright (c) 2012,2013 Monty Program Ab

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
#pragma once

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

#define ROCKSDB_INCLUDE_RFR 1

/* C++ standard header files */
#include <cinttypes>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <map>

/* MySQL header files */
#include "ib_ut0counter.h"
#include "my_icp.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/sql_bitmap.h"
#include "sql_string.h"
#include "my_base.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_rwlock.h"
#include "sql/tdsql/common.h"

/* MyRocks header files */
#include "./ib_ut0counter.h"
#include "./rdb_utils.h"

#include <string>
#include <map>

/* TDSQL include */
#include "sql/tdsql/tdstore.h" //for table scan
#include "sql/tdsql/mc.h"
#include "sql/tdsql/trans.h"
#include "sql/tdsql/slice.h"
#include "sql/tdsql/ddl/ddl_job.h" //IndexInfoCollector
#include "sql/tdsql/optimizer/aux_iterator.h" // RawRecKvCacheIterator
#include "storage/rocksdb/tdsql/parallel_query.h"

using namespace tdsql;

extern PSI_memory_key key_memory_myrocks_estimated_info;

/**
  @note MyRocks Coding Conventions:
  MyRocks code follows the baseline MySQL coding conventions, available at
  http://dev.mysql.com/doc/internals/en/coding-guidelines.html, with several
  refinements (@see /storage/rocksdb/README file).
*/

/**
  @note MyRocks Coding Conventions:
  MyRocks code follows the baseline MySQL coding conventions, available at
  http://dev.mysql.com/doc/internals/en/coding-guidelines.html, with several
  refinements (@see /storage/rocksdb/README file).
*/

namespace myrocks {


/*
 * class for exporting transaction information for
 * information_schema.rocksdb_trx
 */
struct Rdb_trx_info {
  std::string name;
  ulonglong trx_id;
  ulonglong write_count;
  ulonglong lock_count;
  int timeout_sec;
  std::string state;
  std::string waiting_key;
  ulonglong waiting_cf_id;
  int is_replication;
  int skip_trx_api;
  int read_only;
  int deadlock_detect;
  int num_ongoing_bulk_load;
  ulong thread_id;
  std::string query_str;
};

std::vector<Rdb_trx_info> rdb_get_all_trx_info();

/*
 * class for exporting deadlock transaction information for
 * information_schema.rocksdb_deadlock
 */
struct Rdb_deadlock_info {
  struct Rdb_dl_trx_info {
    ulonglong trx_id;
    std::string cf_name;
    std::string waiting_key;
    bool exclusive_lock;
    std::string index_name;
    std::string table_name;
  };
  std::vector<Rdb_dl_trx_info> path;
  ulonglong victim_trx_id;
};

std::vector<Rdb_deadlock_info> rdb_get_deadlock_info();

/*
  TDSQL: struct to save key info when reading index
*/
struct Read_key_info {
  uint active_index;
  uint read_key_len;
  Read_key_info()
    : active_index(MAX_INDEXES),
      read_key_len(0) {}
};

/*
  This is
  - the name of the default Column Family (the CF which stores indexes which
    didn't explicitly specify which CF they are in)
  - the name used to set the default column family parameter for per-cf
    arguments.
*/
extern const std::string DEFAULT_CF_NAME;

/*
  This is the name of the Column Family used for storing the data dictionary.
*/
extern const std::string DEFAULT_SYSTEM_CF_NAME;

/*
  Column family name which means "put this index into its own column family".
  DEPRECATED!!!
*/
extern const std::string PER_INDEX_CF_NAME;

/*
  Name for the background thread.
*/
const char *const BG_THREAD_NAME = "myrocks-bg";

/*
  Name for the drop index thread.
*/
const char *const INDEX_THREAD_NAME = "myrocks-index";

/*
  Separator between partition name and the qualifier. Sample usage:

  - p0_cfname=foo
  - p3_tts_col=bar
*/
constexpr char RDB_PER_PARTITION_QUALIFIER_NAME_SEP = '_';

/*
  Separator between qualifier name and value. Sample usage:

  - p0_cfname=foo
  - p3_tts_col=bar
*/
constexpr char RDB_QUALIFIER_VALUE_SEP = '=';

/*
  Separator between multiple qualifier assignments. Sample usage:

  - p0_cfname=foo;p1_cfname=bar;p2_cfname=baz
*/
constexpr char RDB_QUALIFIER_SEP = ';';

/*
  Qualifier name for a custom per partition column family.
*/
const char *const RDB_CF_NAME_QUALIFIER = "cfname";

/*
  Qualifier name for a custom per partition ttl duration.
*/
const char *const RDB_TTL_DURATION_QUALIFIER = "ttl_duration";

/*
  Qualifier name for a custom per partition ttl duration.
*/
const char *const RDB_TTL_COL_QUALIFIER = "ttl_col";

/*
  Default, minimal valid, and maximum valid sampling rate values when collecting
  statistics about table.
*/
#define RDB_DEFAULT_TBL_STATS_SAMPLE_PCT 10
#define RDB_TBL_STATS_SAMPLE_PCT_MIN 1
#define RDB_TBL_STATS_SAMPLE_PCT_MAX 100

/*
  Default and maximum values for rocksdb-compaction-sequential-deletes and
  rocksdb-compaction-sequential-deletes-window to add basic boundary checking.
*/
#define DEFAULT_COMPACTION_SEQUENTIAL_DELETES 0
#define MAX_COMPACTION_SEQUENTIAL_DELETES 2000000

#define DEFAULT_COMPACTION_SEQUENTIAL_DELETES_WINDOW 0
#define MAX_COMPACTION_SEQUENTIAL_DELETES_WINDOW 2000000

/*
  Default and maximum values for various compaction and flushing related
  options. Numbers are based on the hardware we currently use and our internal
  benchmarks which indicate that parallelization helps with the speed of
  compactions.

  Ideally of course we'll use heuristic technique to determine the number of
  CPU-s and derive the values from there. This however has its own set of
  problems and we'll choose simplicity for now.
*/
constexpr int MAX_BACKGROUND_JOBS = 64;

constexpr int DEFAULT_SUBCOMPACTIONS = 1;
constexpr int MAX_SUBCOMPACTIONS = 64;

/*
  Default value for rocksdb_sst_mgr_rate_bytes_per_sec = 0 (disabled).
*/
constexpr uint64_t DEFAULT_SST_MGR_RATE_BYTES_PER_SEC = 0;

/*
  Defines the field sizes for serializing XID object to a string representation.
  string byte format: [field_size: field_value, ...]
  [
    8: XID.formatID,
    1: XID.gtrid_length,
    1: XID.bqual_length,
    XID.gtrid_length + XID.bqual_length: XID.data
  ]
*/
#define RDB_FORMATID_SZ 8
#define RDB_GTRID_SZ 1
#define RDB_BQUAL_SZ 1
#define RDB_XIDHDR_LEN (RDB_FORMATID_SZ + RDB_GTRID_SZ + RDB_BQUAL_SZ)

/*
  To fix an unhandled exception we specify the upper bound as LONGLONGMAX
  instead of ULONGLONGMAX because the latter is -1 and causes an exception when
  cast to jlong (signed) of JNI

  The reason behind the cast issue is the lack of unsigned int support in Java.
*/
#define MAX_RATE_LIMITER_BYTES_PER_SEC static_cast<uint64_t>(LLONG_MAX)

/*
  Hidden PK column (for tables with no primary key) is a longlong (aka 8 bytes).
  static_assert() in code will validate this assumption.
*/
#define ROCKSDB_SIZEOF_HIDDEN_PK_COLUMN sizeof(longlong)

/*
  Bytes used to store TTL, in the beginning of all records for tables with TTL
  enabled.
*/
#define ROCKSDB_SIZEOF_TTL_RECORD sizeof(longlong)

/*
  Maximum index prefix length in bytes.
*/
constexpr uint MAX_INDEX_COL_LEN_LARGE = 3072;
constexpr uint MAX_INDEX_COL_LEN_SMALL = 767;

/*
  MyRocks specific error codes. NB! Please make sure that you will update
  HA_ERR_ROCKSDB_LAST when adding new ones.  Also update the strings in
  rdb_error_messages to include any new error messages.
*/
#define HA_ERR_ROCKSDB_FIRST (HA_ERR_LAST + 1)
#define HA_ERR_ROCKSDB_PK_REQUIRED (HA_ERR_ROCKSDB_FIRST + 0)
#define HA_ERR_ROCKSDB_TABLE_DATA_DIRECTORY_NOT_SUPPORTED                      \
  (HA_ERR_ROCKSDB_FIRST + 1)
#define HA_ERR_ROCKSDB_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED                     \
  (HA_ERR_ROCKSDB_FIRST + 2)
#define HA_ERR_ROCKSDB_COMMIT_FAILED (HA_ERR_ROCKSDB_FIRST + 3)
#define HA_ERR_ROCKSDB_BULK_LOAD (HA_ERR_ROCKSDB_FIRST + 4)
#define HA_ERR_ROCKSDB_CORRUPT_DATA (HA_ERR_ROCKSDB_FIRST + 5)
#define HA_ERR_ROCKSDB_CHECKSUM_MISMATCH (HA_ERR_ROCKSDB_FIRST + 6)
#define HA_ERR_ROCKSDB_INVALID_TABLE (HA_ERR_ROCKSDB_FIRST + 7)
#define HA_ERR_ROCKSDB_PROPERTIES (HA_ERR_ROCKSDB_FIRST + 8)
#define HA_ERR_ROCKSDB_MERGE_FILE_ERR (HA_ERR_ROCKSDB_FIRST + 9)
/*
  Each error code below maps to a RocksDB status code found in:
  rocksdb/include/rocksdb/status.h
*/
#define HA_ERR_ROCKSDB_STATUS_NOT_FOUND (HA_ERR_LAST + 10)
#define HA_ERR_ROCKSDB_STATUS_CORRUPTION (HA_ERR_LAST + 11)
#define HA_ERR_ROCKSDB_STATUS_NOT_SUPPORTED (HA_ERR_LAST + 12)
#define HA_ERR_ROCKSDB_STATUS_INVALID_ARGUMENT (HA_ERR_LAST + 13)
#define HA_ERR_ROCKSDB_STATUS_IO_ERROR (HA_ERR_LAST + 14)
#define HA_ERR_ROCKSDB_STATUS_NO_SPACE (HA_ERR_LAST + 15)
#define HA_ERR_ROCKSDB_STATUS_MERGE_IN_PROGRESS (HA_ERR_LAST + 16)
#define HA_ERR_ROCKSDB_STATUS_INCOMPLETE (HA_ERR_LAST + 17)
#define HA_ERR_ROCKSDB_STATUS_SHUTDOWN_IN_PROGRESS (HA_ERR_LAST + 18)
#define HA_ERR_ROCKSDB_STATUS_TIMED_OUT (HA_ERR_LAST + 19)
#define HA_ERR_ROCKSDB_STATUS_ABORTED (HA_ERR_LAST + 20)
#define HA_ERR_ROCKSDB_STATUS_LOCK_LIMIT (HA_ERR_LAST + 21)
#define HA_ERR_ROCKSDB_STATUS_BUSY (HA_ERR_LAST + 22)
#define HA_ERR_ROCKSDB_STATUS_DEADLOCK (HA_ERR_LAST + 23)
#define HA_ERR_ROCKSDB_STATUS_EXPIRED (HA_ERR_LAST + 24)
#define HA_ERR_ROCKSDB_STATUS_TRY_AGAIN (HA_ERR_LAST + 25)
#define HA_ERR_ROCKSDB_CORRELATE_TABLE (HA_ERR_LAST + 26)
#define HA_ERR_ROCKSDB_LAST HA_ERR_ROCKSDB_CORRELATE_TABLE

/**
  @brief
  Rdb_table_handler is a reference-counted structure storing information for
  each open table. All the objects are stored in a global hash map.

  //TODO: join this with Rdb_tbl_def ?
*/
struct Rdb_table_handler {
  char *m_table_name;
  uint m_table_name_length;
  std::atomic_int m_ref_count;

  my_core::THR_LOCK m_thr_lock;  ///< MySQL latch needed by m_db_lock

  /* Stores cached memtable estimate statistics */
  std::atomic_uint m_mtcache_lock;
  /* EstimatedThread, 0: not in queue, 1: in queue */
  std::atomic_int m_mtcache_in_queue;
  uint64_t m_mtcache_count;
  uint64_t m_mtcache_size;
  uint64_t m_mtcache_last_update;

  void get_handler() { m_ref_count++; }

  void release_handler();

  bool enqueue_if_not_in() {
    if (m_mtcache_in_queue++ == 0) {
      return true;
    }

    m_mtcache_in_queue--;
    return false;
  }

  void dequeue() { m_mtcache_in_queue--; }
};

class Rdb_key_def;
class Rdb_tbl_def;
class Rdb_field_encoder;
class Bulk_load_closure;

const char *const rocksdb_hton_name = "ROCKSDB";

enum operation_type : int {
  ROWS_DELETED = 0,
  ROWS_INSERTED,
  ROWS_READ,
  ROWS_UPDATED,
  ROWS_EXPIRED,
  ROWS_FILTERED,
  ROWS_HIDDEN_NO_SNAPSHOT,
  ROWS_MAX
};

enum query_type : int {
  QUERIES_POINT = 0,
  QUERIES_RANGE,
  QUERIES_COUNT_OPTIM,
  QUERIES_MAX
};

#if defined(HAVE_SCHED_GETCPU)
#define RDB_INDEXER get_sched_indexer_t
#else
#define RDB_INDEXER thread_id_indexer_t
#endif

/* Global statistics struct used inside MyRocks */
struct st_global_stats {
  ib_counter_t<ulonglong, 64, RDB_INDEXER> rows[ROWS_MAX];

  // system_rows_ stats are only for system
  // tables. They are not counted in rows_* stats.
  ib_counter_t<ulonglong, 64, RDB_INDEXER> system_rows[ROWS_MAX];

  ib_counter_t<ulonglong, 64, RDB_INDEXER> queries[QUERIES_MAX];

  ib_counter_t<ulonglong, 64, RDB_INDEXER> covered_secondary_key_lookups;
};

/* Struct used for exporting status to MySQL */
struct st_export_stats {
  ulonglong rows_deleted;
  ulonglong rows_inserted;
  ulonglong rows_read;
  ulonglong rows_updated;
  ulonglong rows_expired;
  ulonglong rows_filtered;
  ulonglong rows_hidden_no_snapshot;

  ulonglong system_rows_deleted;
  ulonglong system_rows_inserted;
  ulonglong system_rows_read;
  ulonglong system_rows_updated;

  ulonglong queries_point;
  ulonglong queries_range;

  ulonglong covered_secondary_key_lookups;

  ulonglong queries_count_optim;
};

/* Struct used for exporting RocksDB memory status */
struct st_memory_stats {
  ulonglong memtable_total;
  ulonglong memtable_unflushed;
};

/* Struct used for exporting RocksDB IO stalls stats */
struct st_io_stall_stats {
  ulonglong level0_slowdown;
  ulonglong level0_slowdown_with_compaction;
  ulonglong level0_numfiles;
  ulonglong level0_numfiles_with_compaction;
  ulonglong stop_for_pending_compaction_bytes;
  ulonglong slowdown_for_pending_compaction_bytes;
  ulonglong memtable_compaction;
  ulonglong memtable_slowdown;
  ulonglong total_stop;
  ulonglong total_slowdown;

  st_io_stall_stats()
      : level0_slowdown(0), level0_slowdown_with_compaction(0),
        level0_numfiles(0), level0_numfiles_with_compaction(0),
        stop_for_pending_compaction_bytes(0),
        slowdown_for_pending_compaction_bytes(0), memtable_compaction(0),
        memtable_slowdown(0), total_stop(0), total_slowdown(0) {}
};

}  // namespace myrocks

#include "./rdb_buff.h"

#include "./rdb_datadic.h"
namespace myrocks {

class rdb_increment_base {
 public:
  inline void release_generator()
  { m_generator = nullptr; }

  inline bool valid() { return !!generator(); }
  inline uint32 index_id() { return generator() ? generator()->index_id() : 0; }

 protected:
  rdb_increment_base() {}
  virtual ~rdb_increment_base() {
    release_generator();
  }

  tdsql::AutoIncGeneratorPtr generator();
  int init_on_open_table_base(uint32 index_id, enum ha_base_keytype type);

  AutoIncGeneratorPtr m_generator;

#if defined(EXTRA_CODE_FOR_UNIT_TESTING)
 public:
  AutoIncGeneratorPtr generator_test() { return m_generator; }
#endif
};

class rdb_autoinc_generator : public rdb_increment_base {
 public:
  rdb_autoinc_generator() = default;
  virtual ~rdb_autoinc_generator() = default;

  int init_on_open_table(uint32 index_id, enum ha_base_keytype type);
  int init_on_create_table(uint32 index_id, ulonglong val,
      enum ha_base_keytype type);

  /* Get current auto increment value and don't fetch from meta-cluster. */
  ulonglong get_current_val();
  /* Get auto increment value and fetch from meta-cluster if necessary. */
  void get_auto_increment(ulonglong off, ulonglong inc,
      ulonglong nb_desired_values, ulonglong *const first_value,
      ulonglong *const nb_reserved_values);
  /*
    Push forward auto_increment value on local cache and meta-cluster.
    e.g. ALTER TABLE t1 AUTO_INCREMENT = 10;
         INSERT INTO t1 (autoinc_col) values (10);
  */
  int upd_auto_increment(ulonglong val);
  /*
     Generate a batch of auto increment value from meta-cluster.
     see ha_rocksdb::write_row().
  */
  int generate_next_batch();

#if defined(EXTRA_CODE_FOR_UNIT_TESTING)
  int init_on_open_table_test(uint32 index_id, enum ha_base_keytype type) {
    return init_on_open_table_base(index_id, type);
  }
#endif
};

class rdb_hidden_pk_generator : public rdb_increment_base {
 public:
  rdb_hidden_pk_generator() = default;
  virtual ~rdb_hidden_pk_generator() = default;

  int init_on_open_table(uint32 index_id);
  int generate_next(longlong* val);
 private:
  /* The type of hidden pk is longlong. */
  static const ha_base_keytype m_type = HA_KEYTYPE_LONGLONG;
};

class Bulk_load_closure {
 public:
  Bulk_load_closure(THD* thd);
  virtual ~Bulk_load_closure();
  Bulk_load_closure() = delete;
 private:
  THD *m_thd{nullptr};
  bool m_true_lvalue{true};
  ulong m_default_size;
  bool m_bulk_load;
  bool m_bulk_load_allow_sk;
  bool m_tdsql_disable_trans_conflict_detect;
  ulong m_bulk_load_size;
};

/**
  @brief
  Class definition for ROCKSDB storage engine plugin handler
*/

class ha_rocksdb : public my_core::handler {
  my_core::THR_LOCK_DATA m_db_lock;  ///< MySQL database lock

  Rdb_table_handler *m_table_handler;  ///< Open table handler

  /* TDStore scan iterator, used for range scans and for full table/index scans */
  tdsql::Iterator *m_td_scan_it;

  /* Raw record cache for preload PK, has the same life cycle as m_td_scan_it. */
  tdsql::RawRecKvCacheIterator *m_raw_rec_cache_it;

  //index used to build tdstore_pushed_cond_pb_
  uint tdstore_pushed_cond_idx_;
  push_down::PushConditionRequest *tdstore_pushed_cond_pb_{nullptr};

  //actually pushed tdstore condition
  Item* tdstore_pushed_cond_ = nullptr;

  //set to 1 if all cond from sql layer is pushed to tdstore
  //used by count,limit..
  bool m_all_pushed = false;

  bool m_projection = false;

  //build pushed condition may not used due to other reason
  bool m_really_push = false;
  std::vector<uint> m_projection_fields;

  Item *icp_cond_tdstore_{nullptr};
  Item *icp_cond_remainder_{nullptr};

  Item *table_cond_tdstore_{nullptr};
  Item *table_cond_remainder_{nullptr};

  // set true if do not call next to iterator
  bool only_init_iterator_{false};

  // Limit offset cond.
  tdsql::LimitOffsetCondPushDown limit_offset_cond_pushdown;

  // TDSQL: set by ha_rocksdb::records_from_index and
  // ha_rockspart::records_from_index, to inform secondary_index_read don't call
  // get_row_by_rowid when calculate number of rows in table.
  bool m_only_calc_records = false;

  /* Whether m_scan_it was created with skip_bloom=true */
  bool m_scan_it_skips_bloom;

  /* Buffers used for upper/lower bounds for m_scan_it. */
  uchar *m_scan_it_lower_bound;
  uchar *m_scan_it_upper_bound;
  rocksdb::Slice m_scan_it_lower_bound_slice;
  rocksdb::Slice m_scan_it_upper_bound_slice;

  Rdb_tbl_def *m_tbl_def;

  /* Primary Key encoder from KeyTupleFormat to StorageFormat */
  std::shared_ptr<Rdb_key_def> m_pk_descr;

  /* Array of index descriptors */
  std::shared_ptr<Rdb_key_def> *m_key_descr_arr;

  static bool check_keyread_allowed(bool &pk_can_be_decoded,
                                    const TABLE_SHARE *table_share, uint inx,
                                    uint part, bool all_parts);

  /*
    Number of key parts in PK. This is the same as
      table->key_info[table->s->primary_key].keyparts
  */
  uint m_pk_key_parts;

  /*
    true <=> Primary Key columns can be decoded from the index
  */
  mutable bool m_pk_can_be_decoded;

  /*
   true <=> Some fields in the PK may require unpack_info.
  */
  bool m_maybe_unpack_info;

  uchar *m_pk_tuple;        /* Buffer for storing PK in KeyTupleFormat */
  uchar *m_pk_packed_tuple; /* Buffer for storing PK in StorageFormat */
  // ^^ todo: change it to 'char*'? TODO: ^ can we join this with last_rowkey?

  /*
    Temporary buffers for storing the key part of the Key/Value pair
    for secondary indexes.
  */
  uchar *m_sk_packed_tuple;

  /*
    Temporary buffers for storing end key part of the Key/Value pair.
    This is used for range scan only.
  */
  uchar *m_end_key_packed_tuple;

  Rdb_string_writer m_sk_tails;
  Rdb_string_writer m_pk_unpack_info;

  /*
    ha_rockdb->index_read_map(.. HA_READ_KEY_EXACT or similar) will save here
    mem-comparable form of the index lookup tuple.
  */
  uchar *m_sk_match_prefix;
  uint m_sk_match_length;

  /* Buffer space for the above */
  uchar *m_sk_match_prefix_buf;

  /* Second buffers, used by UPDATE. */
  uchar *m_sk_packed_tuple_old;
  Rdb_string_writer m_sk_tails_old;

  /* Buffers used for duplicate checking during unique_index_creation */
  uchar *m_dup_sk_packed_tuple;
  uchar *m_dup_sk_packed_tuple_old;

  /*
    Temporary space for packing VARCHARs (we provide it to
    pack_record()/pack_index_tuple() calls).
  */
  uchar *m_pack_buffer;

  /*
    Pointer to the original TTL timestamp value (8 bytes) during UPDATE.
  */
  char m_ttl_bytes[ROCKSDB_SIZEOF_TTL_RECORD];
  /*
    The TTL timestamp value can change if the explicit TTL column is
    updated. If we detect this when updating the PK, we indicate it here so
    we know we must always update any SK's.
  */
  bool m_ttl_bytes_updated;

  /* rowkey of the last record we've read, in StorageFormat. */
  String m_last_rowkey;

  /* Buffer used by convert_record_to_storage_format() */
  String m_storage_record;

  /*
    Last retrieved record, in table->record[0] data format.

    This is used only when we get the record with rocksdb's Get() call (The
    other option is when we get a rocksdb::Slice from an iterator)
  */
  rocksdb::PinnableSlice m_retrieved_record;

  /* Type of locking to apply to rows */
  enum { RDB_LOCK_NONE, RDB_LOCK_READ, RDB_LOCK_WRITE } m_lock_rows;

  /* true means we're doing an index-only read. false means otherwise. */
  bool m_keyread_only;

  bool m_skip_scan_it_next_call;

  /* true means the replication slave will use Read Free Replication */
  bool m_use_read_free_rpl;

  /**
    @brief
    This is a bitmap of indexes (i.e. a set) whose keys (in future, values) may
    be changed by this statement. Indexes that are not in the bitmap do not need
    to be updated.
    @note Valid inside UPDATE statements, IIF(m_update_scope_is_valid == true).
  */
  my_core::Bitmap<((MAX_INDEXES + 7) / 8 * 8)> m_update_scope;
  bool m_update_scope_is_valid;

  /*
    MySQL index number for duplicate key error
  */
  int m_dupp_errkey;

  int create_key_defs(const TABLE *const table_arg,
                      Rdb_tbl_def *const tbl_def_arg,
                      const TABLE *const old_table_arg = nullptr,
                      const Rdb_tbl_def *const old_tbl_def_arg = nullptr) const
      MY_ATTRIBUTE((__warn_unused_result__));

  bool is_ascending(const Rdb_key_def &keydef,
                    enum ha_rkey_function find_flag) const
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  /* TDSQL:release m_td_scan_it */
  void release_td_scan_iterator(void);

  // TDSQL
  int get_for_update(tdsql::Transaction *const tx,
                     const rocksdb::Slice &key,
                     rocksdb::PinnableSlice *value);

  int get_row_by_rowid(uchar *const buf, const char *const rowid,
                       const uint rowid_size, const bool skip_ttl_check = true,
                       const bool pk_point_select = false)
      MY_ATTRIBUTE((__warn_unused_result__));
  int get_row_by_rowid(uchar *const buf, const uchar *const rowid,
                       const uint rowid_size, const bool skip_ttl_check = true,
                       const bool pk_point_select = false)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__)) {
    return get_row_by_rowid(buf, reinterpret_cast<const char *>(rowid),
                            rowid_size, skip_ttl_check, pk_point_select);
  }

  int get_row_from_raw_rec_cache(uint keyno, uchar *const buf);

  int update_auto_incr_val(ulonglong val);
  int update_auto_incr_val_from_field();
  int get_hidden_pk_val(longlong *val);
  int read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id)
      MY_ATTRIBUTE((__warn_unused_result__));
  bool can_use_single_delete(const uint &index) const
      MY_ATTRIBUTE((__warn_unused_result__));
  bool skip_unique_check() const MY_ATTRIBUTE((__warn_unused_result__));
  bool commit_in_the_middle() MY_ATTRIBUTE((__warn_unused_result__));

  void update_row_stats(const operation_type &type);

  void set_last_rowkey(const uchar *const old_data);

  void set_last_rowkey_inner(const uchar *const old_data);

  bool tdsql_keyno_changed(QEP_TAB* tab,uint keyno) override;

  static const std::string SCHEMA_INDEX_ID;  /* TDSQL TODO: synchronize with record key */
public:
  /*
    Array of table->s->fields elements telling how to store fields in the
    record.
  */
  Rdb_field_encoder *m_encoder_arr;

  /*
    This tells which table fields should be decoded (or skipped) when
    decoding table row from (pk, encoded_row) pair. (Secondary keys are
    just always decoded in full currently)
  */
  std::vector<READ_FIELD> m_decoders_vect;

  /*
    This tells if any field which is part of the key needs to be unpacked and
    decoded.
   */
  bool m_key_requested = false;

  /* Setup field_decoders based on type of scan and table->read_set */
  void setup_read_decoders();

  /*
    For the active index, indicates which columns must be covered for the
    current lookup to be covered. If the bitmap field is null, that means this
    index does not cover the current lookup for any record.
   */
  MY_BITMAP m_lookup_bitmap;

  /*
    Number of bytes in on-disk (storage) record format that are used for
    storing SQL NULL flags.
  */
  uint m_null_bytes_in_rec;

  void setup_field_converters();
  int alloc_key_buffers(const TABLE *const table_arg,
                        const Rdb_tbl_def *const tbl_def_arg,
                        bool alloc_alter_buffers = false)
      MY_ATTRIBUTE((__warn_unused_result__));
  void free_key_buffers();

  // the buffer size should be at least 2*Rdb_key_def::INDEX_NUMBER_SIZE
  rocksdb::Range get_range(const int &i, uchar buf[]) const;

  /*
    A counter of how many row checksums were checked for this table. Note that
    this does not include checksums for secondary index entries.
  */
  my_core::ha_rows m_row_checksums_checked;

  /*
    TDSQL: save key info when reading index
  */
  Read_key_info m_read_key_info;

  /*
    Update stats
  */
  void update_stats(void);

 private:
  /*
    Member to control enbale/disable TDSQL batch put.
  */
  Bulk_load_closure *m_bulk_load_closure{nullptr};

 public:

  ha_rocksdb(my_core::handlerton *const hton,
             my_core::TABLE_SHARE *const table_arg);
  ~ha_rocksdb() {
    int err MY_ATTRIBUTE((__unused__));
    err = finalize_bulk_load(false);
    if (err != 0) {
      LogPluginErrMsg(ERROR_LEVEL, 0,
                      "Error %d finalizing bulk load while closing handler.",
                      err);
    }

    if (tdstore_pushed_cond_pb_) {
      delete tdstore_pushed_cond_pb_;
      tdstore_pushed_cond_pb_ = nullptr;
    }
  }

  void set_write_fence(const Rdb_key_def *kd);
  void set_write_fence(tdsql::Iterator *iter);
  void set_write_fence();
  uint32 get_key_tindex_id(const Rdb_key_def *kd = nullptr);
  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(rocksdb_hton_name);
  }

  /*
    Returns the name of the table's base name
  */
  const std::string &get_table_basename() const;

  /*
    Returns the name of the table's full name
  */
  const std::string &get_table_fullname() const;

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override {
    DBUG_ENTER_FUNC();

    /*
      HA_BINLOG_STMT_CAPABLE
        We are saying that this engine is just statement capable to have
        an engine that can only handle statement-based logging. This is
        used in testing.
    */
    DBUG_RETURN(HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
                HA_CAN_INDEX_BLOBS |
                (m_pk_can_be_decoded ? HA_PRIMARY_KEY_IN_READ_INDEX : 0) |
                HA_PRIMARY_KEY_REQUIRED_FOR_POSITION | HA_NULL_IN_KEY |
                HA_PARTIAL_COLUMN_READ | HA_ONLINE_ANALYZE |
                HA_ATTACHABLE_TRX_COMPATIBLE | HA_SUPPORTS_DEFAULT_EXPRESSION |
                HA_CAN_PARALLEL_SCAN);
  }

  bool init_with_fields() override;

  static ulong index_flags(bool &pk_can_be_decoded,
                           const TABLE_SHARE *table_share, uint inx, uint part,
                           bool all_parts);

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

    @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx, uint part, bool all_parts) const override;

  bool rpl_can_handle_stm_event() const noexcept override;

  bool primary_key_is_clustered() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(true);
  }

 public:
  int rename_table(const char *const from, const char *const to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int convert_record_from_storage_format(const rocksdb::Slice *const key,
                                         const rocksdb::Slice *const value,
                                         uchar *const buf)
      MY_ATTRIBUTE((__warn_unused_result__));

  int convert_record_from_storage_format(const rocksdb::Slice *const key,
                                         uchar *const buf)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  static const std::vector<std::string> parse_into_tokens(const std::string &s,
                                                          const char delim);

  static const std::string generate_schema_key(const std::string& db_name,
                                                const std::string& table_name);

  static const char *get_key_comment(const uint index,
                                     const TABLE *const table_arg,
                                     const Rdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__warn_unused_result__));

  static const std::string get_table_comment(const TABLE *const table_arg)
      MY_ATTRIBUTE((__warn_unused_result__));

  static uint pk_index(const TABLE *const table_arg,
                       const Rdb_tbl_def *const tbl_def_arg)
      MY_ATTRIBUTE((__warn_unused_result__));

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(HA_MAX_REC_LENGTH);
  }

  uint max_supported_keys() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(MAX_INDEXES);
  }

  uint max_supported_key_parts() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(MAX_REF_PARTS);
  }

  uint
  max_supported_key_part_length(HA_CREATE_INFO *create_info) const override;

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(16 * 1024); /* just to return something*/
  }

  /**
    TODO: return actual upper bound of number of records in the table.
    (e.g. save number of records seen on full table scan and/or use file size
    as upper bound)
  */
  ha_rows estimate_rows_upper_bound() override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(HA_POS_ERROR);
  }

  int SetupEndRange(const Rdb_key_def &kd,const key_range *end_key, bool direction);

  int SetupBoundSlice(const Rdb_key_def &kd, uint packed_size,
                      enum ha_rkey_function find_flag, const key_range *end_key,
                      bool *move_forward);
  /* At the moment, we're ok with default handler::index_init() implementation.
   */
  int index_read_map(uchar *const buf, const uchar *const key,
                     key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_next_same(uchar *buf, const uchar *key MY_ATTRIBUTE((unused)),
                      uint keylen MY_ATTRIBUTE((unused))) override;

  int index_read_map_impl(uchar *const buf, const uchar *const key,
                          key_part_map keypart_map,
                          enum ha_rkey_function find_flag,
                          const key_range *end_key)
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_read_last_map(uchar *const buf, const uchar *const key,
                          key_part_map keypart_map) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_read_idx_map(uchar *buf, uint index, const uchar *key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag) override;

  int read_range_first(const key_range *const start_key,
                       const key_range *const end_key, bool eq_range,
                       bool sorted) override
      MY_ATTRIBUTE((__warn_unused_result__));

  virtual double scan_time() override {
    DBUG_ENTER_FUNC();

    DBUG_RETURN(
        static_cast<double>((stats.records + stats.deleted) / 20.0 + 10));
  }

  virtual double read_time(uint, uint, ha_rows rows) override;
  virtual void print_error(int error, myf errflag) override;

  int open(const char *const name, int mode, uint test_if_locked,
           const dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int close(void) override MY_ATTRIBUTE((__warn_unused_result__));

  int write_row(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int update_row(const uchar *const old_data, uchar *const new_data) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int delete_row(const uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_next(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_next_with_direction(uchar *const buf, bool move_forward)
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_prev(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int index_first(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_last(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_fillback_sk() override;
  int index_fillback_sk_by_range(const tdsql::Slice &start,
                                 const tdsql::Slice &end,
                                 uint64 iterator_id) override;
  int thomas_write_fillback(bool has_unique,
                            const tdsql::ddl::IndexInfoCollector &indexes_to_be_added,
                            tdsql::ThomasRulePutCntl *cntl);

  bool can_push_down(uint keyno, class Item *const idx_cond);
  void fill_field_info(uint keyno, push_down::PushConditionRequest* request) ;

  void SetCondPush(Item *table_cond,Item *icp_cond,int keyno);
  int BuildPushDownPb(uint keyno);
  void SetProjectionPush();
  int GetPushIndexNo(const QEP_TAB *qep_tab);
  class Item *idx_cond_push(uint keyno, class Item *const idx_cond) override;

  void SetOnlyInitIterator(bool arg) { only_init_iterator_ = arg; }
  tdsql::Iterator *get_iterator() { return m_td_scan_it; }

  /*
    Default implementation from cancel_pushed_idx_cond() suits us
  */
  void cancel_pushed_cond() override {
      if(tdstore_pushed_cond_pb_) {
          delete tdstore_pushed_cond_pb_;
          tdstore_pushed_cond_pb_=NULL;
      }

      icp_cond_tdstore_ = nullptr;
      icp_cond_remainder_ = nullptr;
      table_cond_tdstore_ = nullptr;
      table_cond_remainder_ = nullptr;

      tdstore_pushed_cond_ = NULL;
      m_all_pushed = false;
      m_projection = false;
      m_really_push = 0;
      m_projection_fields.clear();
      handler::cancel_pushed_cond();
      limit_offset_cond_pushdown.Reset();
  }

 private:
  struct key_def_cf_info {
    bool is_reverse_cf;
    bool is_per_partition_cf;
  };

  struct update_row_info {
    tdsql::Transaction *tx;
    const uchar *new_data;
    const uchar *old_data;
    rocksdb::Slice new_pk_slice;
    rocksdb::Slice old_pk_slice;
    rocksdb::Slice old_pk_rec;

    // "unpack_info" data for the new PK value
    Rdb_string_writer *new_pk_unpack_info;

    longlong hidden_pk_id;
    bool skip_unique_check;

    // In certain cases, TTL is enabled on a table, as well as an explicit TTL
    // column.  The TTL column can be part of either the key or the value part
    // of the record.  If it is part of the key, we store the offset here.
    //
    // Later on, we use this offset to store the TTL in the value part of the
    // record, which we can then access in the compaction filter.
    //
    // Set to UINT_MAX by default to indicate that the TTL is not in key.
    uint ttl_pk_offset = UINT_MAX;
  };

  /*
    Used to check for duplicate entries during fast unique secondary index
    creation.
  */
  struct unique_sk_buf_info {
    bool sk_buf_switch = false;
    rocksdb::Slice sk_memcmp_key;
    rocksdb::Slice sk_memcmp_key_old;
    uchar *dup_sk_buf;
    uchar *dup_sk_buf_old;

    /*
      This method is meant to be called back to back during inplace creation
      of unique indexes.  It will switch between two buffers, which
      will each store the memcmp form of secondary keys, which are then
      converted to slices in sk_memcmp_key or sk_memcmp_key_old.

      Switching buffers on each iteration allows us to retain the
      sk_memcmp_key_old value for duplicate comparison.
    */
    inline uchar *swap_and_get_sk_buf() {
      sk_buf_switch = !sk_buf_switch;
      return sk_buf_switch ? dup_sk_buf : dup_sk_buf_old;
    }
  };

  int create_key_def(const TABLE *const table_arg, const uint &i,
                     const Rdb_tbl_def *const tbl_def_arg,
                     std::shared_ptr<Rdb_key_def> *const new_key_def) const
      MY_ATTRIBUTE((__warn_unused_result__));

  int convert_record_to_storage_format(const struct update_row_info &row_info,
                                       rocksdb::Slice *const packed_rec)
      MY_ATTRIBUTE((__nonnull__));

  bool should_hide_ttl_rec(const Rdb_key_def &kd,
                           const rocksdb::Slice &ttl_rec_val,
                           const int64_t curr_ts)
      MY_ATTRIBUTE((__warn_unused_result__));
  //void rocksdb_skip_expired_records(const Rdb_key_def &kd,
  //                                  tdsql::Iterator *const iter,
  //                                  bool seek_backward);

  int IndexFirstLast(uchar *const buf, bool direction);

  enum icp_result check_index_cond() const;
  int find_icp_matching_index_rec(const bool &move_forward, uchar *const buf)
      MY_ATTRIBUTE((__warn_unused_result__));

  void calc_updated_indexes();
  int update_write_row(const uchar *const old_data, const uchar *const new_data,
                       const bool skip_unique_check)
      MY_ATTRIBUTE((__warn_unused_result__));
  int get_pk_for_update(struct update_row_info *const row_info);
  int check_and_lock_unique_pk(const uint &key_id,
                               const struct update_row_info &row_info,
                               bool *const found, bool *const pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));
  int check_and_lock_sk(const uint &key_id,
                        const struct update_row_info &row_info,
                        bool *const found)
      MY_ATTRIBUTE((__warn_unused_result__));
  int check_uniqueness_and_lock(const struct update_row_info &row_info,
                                bool *const pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));
  bool over_bulk_load_threshold(int *err)
      MY_ATTRIBUTE((__warn_unused_result__));
  int check_duplicate_sk(const TABLE *table_arg, const Rdb_key_def &index,
                         const rocksdb::Slice *key,
                         struct unique_sk_buf_info *sk_info)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));
  int update_pk(const Rdb_key_def &kd, const struct update_row_info &row_info,
                const bool &pk_changed) MY_ATTRIBUTE((__warn_unused_result__));
  int update_sk(const TABLE *const table_arg, const Rdb_key_def &kd,
                const struct update_row_info &row_info, const bool bulk_load_sk)
      MY_ATTRIBUTE((__warn_unused_result__));
  int update_indexes(const struct update_row_info &row_info,
                     const bool &pk_changed)
      MY_ATTRIBUTE((__warn_unused_result__));

  int read_key_exact(const Rdb_key_def &kd, tdsql::Iterator *const iter,
                     const bool &using_full_key,
                     const rocksdb::Slice &key_slice,
                     const int64_t ttl_filter_ts)
      MY_ATTRIBUTE((__warn_unused_result__));

  int read_row_from_primary_key(uchar *const buf)
      MY_ATTRIBUTE((__warn_unused_result__));
  int read_row_from_secondary_key(uchar *const buf, const Rdb_key_def &kd,
                                  bool move_forward)
      MY_ATTRIBUTE((__warn_unused_result__));

  Rdb_tbl_def *get_table_if_exists(const char *const tablename)
      MY_ATTRIBUTE((__warn_unused_result__));
  void read_thd_vars(THD *const thd) MY_ATTRIBUTE((__nonnull__));

  bool contains_foreign_key(THD *const thd)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int inplace_populate_sk(
      TABLE *const table_arg,
      const std::unordered_set<std::shared_ptr<Rdb_key_def>> &indexes)
      MY_ATTRIBUTE((__nonnull__, __warn_unused_result__));

  int finalize_bulk_load(bool print_client_error = true)
      MY_ATTRIBUTE((__warn_unused_result__));

 public:
  bool projection_is_pushed() override { return m_projection; }

  Item *tdsql_push_cond() override { return tdstore_pushed_cond_; }

  virtual handler *clone(const char *name, MEM_ROOT *mem_root) override {
    ha_rocksdb *new_handler = (ha_rocksdb *)handler::clone(name, mem_root);

    new_handler->pushed_cond = pushed_cond;
    new_handler->tdstore_pushed_cond_ = tdstore_pushed_cond_;
    new_handler->tdstore_pushed_cond_idx_ = tdstore_pushed_cond_idx_;
    return new_handler;
  }
  void SingleSelectPush(const QEP_TAB *qep_tab, int index);
  void IndexMergePush(int index);

  void PushDownDone();

  int engine_push(AQP::Table_access *table) override;

  bool tdsql_clone_pushed(const handler *from,
                          Item_clone_context *context) override;

  uint tdsql_push_cond_keyno() override { return tdstore_pushed_cond_idx_; }

  tdsql::LimitOffsetCondPushDown* tdsql_limit_offset_cond() override {
    return &limit_offset_cond_pushdown;
  }

  virtual int tdsql_convert_record_from_storage_format(const std::string &key,
                                                       const std::string &value,
                                                       uchar *const buf) override;

  virtual int tdsql_get_storage_format_pk(tdsql::parallel::StorageFormatKeys* pk) const override;

  virtual bool tdsql_key_in_pk(const std::string &key) const override;

  void set_pk_can_be_decoded(bool flag) { m_pk_can_be_decoded = flag; }
  int index_init(uint idx, bool sorted) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int index_end() override MY_ATTRIBUTE((__warn_unused_result__));

  void unlock_row() override;

  int records_from_index(ha_rows *num_rows, uint index) override;

  int records(ha_rows *num_rows);

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override MY_ATTRIBUTE((__warn_unused_result__));
  int rnd_end() override MY_ATTRIBUTE((__warn_unused_result__));

  int rnd_next(uchar *const buf) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int GetAndParseNextRecord(uchar *const buf, int indext);
  int rnd_pos(uchar *const buf, uchar *const pos) override
      MY_ATTRIBUTE((__warn_unused_result__));
  void position(const uchar *const record) override;
  int info(uint) override;
  void need_update_table_cache(bool async);

  /*
    Just like rnd_init but not the same, rnd_range_init will
    scan record according to given range info.
    By calling rnd_range_init, rnd_next/rnd_end can be used.
  */
  int rnd_range_init(const tdsql::parallel::PartKeyRangeInfo& range_info) override;

  bool in_parallel_task() { return current_thd->is_cloned(); }

  /* This function will always return success, therefore no annotation related
   * to checking the return value. Can't change the signature because it's
   * required by the interface. */
  int extra(enum ha_extra_function operation) override;

  int start_stmt(THD *const thd, thr_lock_type lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int external_lock(THD *const thd, int lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int truncate(dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));

  int reset() override {
    DBUG_ENTER_FUNC();

    /* Free blob data */
    m_retrieved_record.Reset();

    /* Free scan data */
    release_td_scan_iterator();

    /* Free MRR data */
    m_ds_mrr.reset();

    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  int check(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));
  ha_rows records_in_range(uint inx, key_range *const min_key,
                           key_range *const max_key) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int delete_table(const char *const from, const dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int create(const char *const name, TABLE *const form,
             HA_CREATE_INFO *const create_info, dd::Table *table_def) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int create_tbl_def(const char *const name, TABLE *const form,
                     HA_CREATE_INFO *const create_info, const dd::Table *table_def)
      MY_ATTRIBUTE((__warn_unused_result__));
  bool check_if_incompatible_data(HA_CREATE_INFO *const info,
                                  uint table_changes)
      override MY_ATTRIBUTE((__warn_unused_result__));

  THR_LOCK_DATA **store_lock(THD *const thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override
      MY_ATTRIBUTE((__warn_unused_result__));

  bool get_error_message(const int error, String *const buf) override;

  static int rdb_error_to_mysql(const rocksdb::Status &s,
                                const char *msg = nullptr)
      MY_ATTRIBUTE((__warn_unused_result__));

  void get_auto_increment(ulonglong offset, ulonglong increment,
                          ulonglong nb_desired_values,
                          ulonglong *const first_value,
                          ulonglong *const nb_reserved_values) override;
  void update_create_info(HA_CREATE_INFO *const create_info) override;
  int optimize(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));
  int analyze(THD *const thd, HA_CHECK_OPT *const check_opt) override
      MY_ATTRIBUTE((__warn_unused_result__));

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table,
      my_core::Alter_inplace_info *ha_alter_info) override;

  bool prepare_inplace_alter_table(TABLE *altered_table,
                                   my_core::Alter_inplace_info *ha_alter_info,
                                   const dd::Table *old_table_def,
                                   dd::Table *new_table_def) override;

  bool inplace_alter_table(TABLE *altered_table,
                           my_core::Alter_inplace_info *ha_alter_info,
                           const dd::Table *old_table_def,
                           dd::Table *new_table_def) override;

  bool
  commit_inplace_alter_table(TABLE *altered_table,
                             my_core::Alter_inplace_info *const ha_alter_info,
                             bool commit, const dd::Table *old_table_def,
                             dd::Table *new_table_def) override;


  bool index_is_usable(tdsql::Transaction* tx,
                       const uint index,
                       const TABLE *const table_arg,
                       const Rdb_tbl_def *const tbl_def_arg);

#if defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR
 public:
  virtual void rpl_before_delete_rows() override;
  virtual void rpl_after_delete_rows() override;
  virtual void rpl_before_update_rows() override;
  virtual void rpl_after_update_rows() override;
  virtual bool use_read_free_rpl();

 private:
  /* Flags tracking if we are inside different replication operation */
  bool m_in_rpl_delete_rows;
  bool m_in_rpl_update_rows;
#endif  // defined(ROCKSDB_INCLUDE_RFR) && ROCKSDB_INCLUDE_RFR

 private:
  // Get region, transaction timestamp from MC,
  // send transaction start to TDStore,
  // put/get/del to/from TDStore
  int rpc_put_record(tdsql::Transaction* tx,
                     const rocksdb::Slice &key_slice,
                     const rocksdb::Slice &value_slice,
                     tdstore::PutRecordType &&type);
  int rpc_get_record(tdsql::Transaction* tx,
                     const rocksdb::Slice &key_slice,
                     rocksdb::PinnableSlice* value_slice,
                     const bool pk_point_select = false);
  int rpc_del_record(tdsql::Transaction* tx,
                     const rocksdb::Slice &key_slice);
  //int rpc_drop_index(uint32_t index_id);
  //int rpc_del_range(const regions::Members members,
  //                  const rocksdb::Range &range);
  //send start txn requests to multiple tdstores involved in this transaction
  //get a new tdsql::Iterator
  int init_tdsql_iterator(tdsql::Transaction* &tx,
                          const rocksdb::Slice& index_prefix,
                          const rocksdb::Slice& start_slice,
                          const rocksdb::Slice& end_slice, bool is_forward,
                          bool system_table, uint32 tindex_id, uint keyno);

  // Used by index fillback task only.
  int init_tdsql_iterator(const rocksdb::Slice& index_prefix,
                          const rocksdb::Slice& start_slice,
                          const rocksdb::Slice& end_slice,
                          uint64 iterator_id,
                          uint32 tindex_id, uint keyno);

  int flush_batch_record_in_parallel();

  int bulk_cache_key(tdsql::Transaction* tx,
                     const rocksdb::Slice &key_slice,
                     const rocksdb::Slice &value_slice);

  int bulk_cache_key_in_parallel(const rocksdb::Slice &key_slice,
                                 const rocksdb::Slice &value_slice);

  int flush_cache_key(tdsql::Transaction* tx);

  int flush_cache_key_in_parallel();

 public:
  void set_partition_pos(uint pos) override;

  bool clone_partition_info(handler* dst) override {
    assert(dst);
    dst->set_partition_pos(m_part_pos);
    return false;
  }

  bool is_partition_table() { return m_part_pos != UINT32_MAX; }

  void start_bulk_insert(ha_rows rows) override;

  int end_bulk_insert() override;

  Rdb_tbl_def* get_tbl_def() { return m_tbl_def; }

  TABLE *get_table() { return table; }

  void set_tbl_def(Rdb_tbl_def* def) { m_tbl_def = def; }

  void set_only_calc_records(bool arg) { m_only_calc_records = arg; }

 private:
  void set_lock_type(int lock_type) override { m_lock_type = lock_type; }
  bool table_id_equals_pk_id(TABLE *const table_arg);
  std::string partition_tindex_ids_to_string();
  int update_def_from_share_table(bool *closed, const dd::Table *table_def);
  void delete_tbl_def_cache();
  int delete_tbl_def(Rdb_tbl_def *tbl);
  /* Create table def and write down to rdb local persistence. */
  int create_tbl_def(const std::string &str, TABLE *const table_arg,
                     HA_CREATE_INFO *const create_info, const dd::Table *table_def,
                     const bool &lock = true);
  int find_and_create_tbl_def(const std::string &str, const dd::Table *table_def);
  int open_init_hidden_pk(TABLE *const table_arg);
  int open_init_auto_incr(TABLE *const table_arg);
  int check_index_dup_key(const Rdb_key_def &kd);

  int check_unique_index_dup_key(uint keyno) override;

  // The position in Partition_base.m_file.
  // Default(UINT32_MAX) exceeds MAX_PARTITIONS.
  //
  // TODO(Partition): Will 'DROP PARTITION' change position?
  uint32_t m_part_pos{UINT32_MAX};

  // Only assigned in set_partition_pos.
  partition_element* m_part_elem{nullptr};

#ifndef NDEBUG
  void dbg_assert_part_elem(const TABLE* table) const;
#endif

#if defined(EXTRA_CODE_FOR_UNIT_TESTING)
 public:
  Rdb_tbl_def* get_tbl_def_test() { return m_tbl_def; }
  void set_tbl_def_test(Rdb_tbl_def* def) { m_tbl_def = def; }
  void set_table_test(TABLE* table_arg) { table = table_arg; }
  int update_def_from_share_table_test(bool *closed) { return update_def_from_share_table(closed,nullptr); }
  bool table_id_equals_pk_id_test(TABLE *const table_arg) { return table_id_equals_pk_id(table_arg); }
  bool is_partition_table_test() { return is_partition_table(); }
  int open_init_auto_incr_generator_test(uint32 index_id, enum ha_base_keytype type) {
    return m_autoinc_generator.init_on_open_table_test(index_id, type);
  }
  int open_init_hidden_pk_test(TABLE *const table_arg) { return open_init_hidden_pk(table_arg); }
  int open_init_auto_incr_test(TABLE *const table_arg) {
    assert(table_arg);
    assert(table_arg->found_next_number_field);
    assert(table_arg->s);
    assert(!is_partition_table());
    return m_autoinc_generator.init_on_open_table_test(table_arg->s->autoinc_tindex_id,
      table_arg->found_next_number_field->key_type());
  }
  int create_key_def_test(const TABLE *const, const uint &,
                          const Rdb_tbl_def *const,
                          std::shared_ptr<Rdb_key_def> *const,
                          bool, bool) {
    assert(0);
    return TDSQL_EXIT_FAILURE;
  }
  rdb_autoinc_generator& autoinc_generator_test() {
    return m_autoinc_generator;
  }
  rdb_hidden_pk_generator& hidden_pk_generator_test() {
    return m_hidden_pk_generator;
  }
  tdsql::AutoIncGeneratorPtr autoinc_generator_impl_test() {
    return m_autoinc_generator.generator_test();
  }
  tdsql::AutoIncGeneratorPtr hidden_pk_generator_impl_test() {
    return m_hidden_pk_generator.generator_test();
  }
#endif
  // online ddl need
  ha_rocksdb *handler_head;

 private:
  rdb_autoinc_generator m_autoinc_generator;
  rdb_hidden_pk_generator m_hidden_pk_generator;

 protected:
  int get_row_count(ha_rows* rows_count) override;

  int get_row_count_from_index(ha_rows* rows_count, uint index) override;

  int get_row_count_from_key(ha_rows* num_rows, uint index);

  bool use_stmt_optimize_load_data(tdsql::Transaction* tx);

 private:
  /** The multi range read session object */
  DsMrr_impl m_ds_mrr;

  /* Parallel ThomasRulePut pointer used during parallel data fillback stage. */
  std::unique_ptr<tdsql::ThomasRulePutCntl> m_parallel_thomas_put;

 private:
  /** @name Multi Range Read interface @{ */

  /** Initialize multi range read @see DsMrr_impl::dsmrr_init
  @param seq
  @param seq_init_param
  @param n_ranges
  @param mode
  @param buf */
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode, HANDLER_BUFFER *buf) override;

  /** Process next multi range read @see DsMrr_impl::dsmrr_next
  @param range_info */
  int multi_range_read_next(char **range_info) override;

  /** Initialize multi range read and get information.
  @see ha_myisam::multi_range_read_info_const
  @see DsMrr_impl::dsmrr_info_const
  @param keyno
  @param seq
  @param seq_init_param
  @param n_ranges
  @param bufsz
  @param flags
  @param cost */
  ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                      void *seq_init_param, uint n_ranges,
                                      uint *bufsz, uint *flags,
                                      Cost_estimate *cost) override;

  /** Initialize multi range read and get information.
  @see DsMrr_impl::dsmrr_info
  @param keyno
  @param n_ranges
  @param keys
  @param bufsz
  @param flags
  @param cost */
  ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                uint *bufsz, uint *flags, Cost_estimate *cost) override;
  /** @} */

  int ValidateSchemaStatusBeforePutRecord(tdsql::Transaction *tx,
                                          const rocksdb::Slice &key_slice,
                                          const rocksdb::Slice &value_slice);
 public:
  int init_parallel_scan(parallel_scan_handle_t *handle, ulong *nranges,
                         parallel_scan_desc_t *scan_desc) override;

  int attach_parallel_scan(parallel_scan_handle_t scan_handle) override;

  void detach_parallel_scan(
      parallel_scan_handle_t scan_handle) override;

  void end_parallel_scan(parallel_scan_handle_t scan_handle) override;

  int restart_parallel_scan(parallel_scan_handle_t scan_handle) override;

  bool parallel_scan_attached() const {
    return m_parallel_scan_handle != nullptr;
  }

  void AttachAParallelScanJob() {
    m_parallel_scan_job = m_parallel_scan_handle->AttachAJob();
  }

  void DetachAParallelScanJob() { m_parallel_scan_job = nullptr; }

  MyRocksParallelScan *parallel_scan_handle() { return m_parallel_scan_handle; }

  MyRocksParallelScanJob *parallel_scan_job() { return m_parallel_scan_job; }

 private:
  MyRocksParallelScan* m_parallel_scan_handle{nullptr};
  // The running parallel scan job.
  MyRocksParallelScanJob* m_parallel_scan_job{nullptr};
};

/*
  Helper class for in-place alter, for storing handler context between inplace
  alter calls
*/
struct Rdb_inplace_alter_ctx : public my_core::inplace_alter_handler_ctx {
  /* The new table definition */
  Rdb_tbl_def *const m_new_tdef;

  /* Stores the original key definitions */
  std::shared_ptr<Rdb_key_def> *const m_old_key_descr;

  /* Stores the new key definitions */
  std::shared_ptr<Rdb_key_def> *m_new_key_descr;

  /* Stores the old number of key definitions */
  const uint m_old_n_keys;

  /* Stores the new number of key definitions */
  const uint m_new_n_keys;

  /* Stores the added key glids */
  const std::unordered_set<std::shared_ptr<Rdb_key_def>> m_added_indexes;

  /* Stores the dropped key glids */
  const std::unordered_set<GL_INDEX_ID> m_dropped_index_ids;

  /* Stores number of keys to add */
  const uint m_n_added_keys;

  /* Stores number of keys to drop */
  const uint m_n_dropped_keys;

  /* Stores the largest current auto increment value in the index */
  const ulonglong m_max_auto_incr;

  Rdb_inplace_alter_ctx(
      Rdb_tbl_def *new_tdef, std::shared_ptr<Rdb_key_def> *old_key_descr,
      std::shared_ptr<Rdb_key_def> *new_key_descr, uint old_n_keys,
      uint new_n_keys,
      std::unordered_set<std::shared_ptr<Rdb_key_def>> added_indexes,
      std::unordered_set<GL_INDEX_ID> dropped_index_ids, uint n_added_keys,
      uint n_dropped_keys, ulonglong max_auto_incr)
      : my_core::inplace_alter_handler_ctx(), m_new_tdef(new_tdef),
        m_old_key_descr(old_key_descr), m_new_key_descr(new_key_descr),
        m_old_n_keys(old_n_keys), m_new_n_keys(new_n_keys),
        m_added_indexes(added_indexes), m_dropped_index_ids(dropped_index_ids),
        m_n_added_keys(n_added_keys), m_n_dropped_keys(n_dropped_keys),
        m_max_auto_incr(max_auto_incr) {}

  ~Rdb_inplace_alter_ctx() {}

 private:
  /* Disable Copying */
  Rdb_inplace_alter_ctx(const Rdb_inplace_alter_ctx &);
  Rdb_inplace_alter_ctx &operator=(const Rdb_inplace_alter_ctx &);
};

/*
  Helper class to control access/init to handlerton instance.
  Contains a flag that is set if the handlerton is in an initialized, usable
  state, plus a reader-writer lock to protect it without serializing reads.
  Since we don't have static initializers for the opaque mysql_rwlock type,
  use constructor and destructor functions to create and destroy
  the lock before and after main(), respectively.
*/
struct Rdb_hton_init_state {
  struct Scoped_lock {
    Scoped_lock(Rdb_hton_init_state &state, bool write) : m_state(state) {
      if (write)
        m_state.lock_write();
      else
        m_state.lock_read();
    }
    ~Scoped_lock() { m_state.unlock(); }

   private:
    Scoped_lock(const Scoped_lock &sl) : m_state(sl.m_state) {}
    void operator=(const Scoped_lock &) {}

    Rdb_hton_init_state &m_state;
  };

  Rdb_hton_init_state() : m_initialized(false) {
    /*
      m_rwlock can not be instrumented as it must be initialized before
      mysql_mutex_register() call to protect some globals from race condition.
    */
    mysql_rwlock_init(0, &m_rwlock);
  }

  ~Rdb_hton_init_state() { mysql_rwlock_destroy(&m_rwlock); }

  void lock_read() { mysql_rwlock_rdlock(&m_rwlock); }

  void lock_write() { mysql_rwlock_wrlock(&m_rwlock); }

  void unlock() { mysql_rwlock_unlock(&m_rwlock); }

  /*
    Must be called with either a read or write lock held, unable to enforce
    behavior as mysql_rwlock has no means of determining if a thread has a lock
  */
  bool initialized() const { return m_initialized; }

  /*
    Must be called with only a write lock held, unable to enforce behavior as
    mysql_rwlock has no means of determining if a thread has a lock
  */
  void set_initialized(bool init) { m_initialized = init; }

 private:
  mysql_rwlock_t m_rwlock;
  bool m_initialized;
};

struct Index_range_info {
  uint64_t data_rows = 0;
  uint64_t data_size = 0;

  void clear() {
    data_rows = 0;
    data_size = 0;
  }
};

int get_index_statistics(const Rdb_key_def& kd,
                         const rocksdb::Slice& start_key,
                         const rocksdb::Slice& end_key,
                         Index_range_info* index_range_info);

// file name indicating RocksDB data corruption
std::string rdb_corruption_marker_file_name();

}  // namespace myrocks
