/* Copyright (C) 2022 Tencent 

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
   USA */

#define MYSQL_SERVER 1
#include "ha_spiderpart.h"
#include "mysql_version.h" 
#include "spd_environ.h"
#include "spd_err.h"
#include "spd_db_include.h"
#include "sql/mysqld.h"
#include "sql/xa.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_auto_partition.h"
#include "sql/sql_thd_internal_api.h"

extern handlerton *spider_hton_ptr;
struct MYSQL;
extern MYSQL *SPIDER_CONN_get_mysql(SPIDER_CONN *spider_conn);
handler *ha_spiderpart::get_file_handler(TABLE_SHARE *share,
                                        MEM_ROOT *alloc) const {
  ha_spider *file = new (alloc) ha_spider(spider_hton_ptr, share);
  file->init();
  return file;
}
#if 0
void ha_spiderpart::set_pk_can_be_decoded_for_each_partition() {
  for (auto file = reinterpret_cast<ha_spider **>(m_file); *file; file++)
    (*file)->set_pk_can_be_decoded(m_pk_can_be_decoded);
}
#endif
int ha_spiderpart::open(const char *name, int mode, uint test_if_locked,
                       const dd::Table *table_def) {
  int result =
      native_part::Partition_base::open(name, mode, test_if_locked, table_def);
  // set_pk_can_be_decoded_for_each_partition();
  return result;
}

int ha_spiderpart::create(const char *name, TABLE *form,
                         HA_CREATE_INFO *create_info, dd::Table *table_def) {
  int result = 0;
  if (table->part_info == nullptr) {
    // auto partitioned spider table
    spider_auto_partition auto_part(ha_thd(), create_info, table_def, form);
    if ((result = auto_part.add_partitions())) return result;  
    form->part_info = form->s->m_part_info;
    form->part_info->table = form;
    Partition_handler *part_handler = get_partition_handler();
    assert(part_handler != nullptr);
    part_handler->set_part_info(form->part_info, true);
    if ((result = auto_part.update_dd_table())) return result;
  }
  result = native_part::Partition_base::create(name, form, create_info, table_def);
  // set_pk_can_be_decoded_for_each_partition();
  return result;
}

int ha_spiderpart::delete_table(const char *name, const dd::Table *table_def [[maybe_unused]]) {
  int result = 0;
  if (opt_spider_auto_ddls) {
    std::vector<std::string> srv_names;
    get_all_server_name(srv_names);
    THD *thd = ha_thd();
    char db_name[FN_REFLEN];
    size_t length = 0;
    dirname_part(db_name, name, &length);
    assert(length > 0); // must be valid path
    db_name[length - 1] = '\0'; // replace the last '/' to '\0'
    length = dirname_length(db_name);
    for (auto srv : srv_names) {
      result = ha_spider::execute_remote_ddl(srv, thd, (db_name + length), table);
      //when drop multi table: drop table t1,t2,t3;
      //spider drop table one by one, but remote shard ddl_sql drop all
      if (result && result!=ER_BAD_TABLE_ERROR) return result;
    }
  }
  result = native_part::Partition_base::delete_table(name, table_def);

  return result;
}
/**
  Clone the open and locked partitioning handler.

  @param  mem_root  MEM_ROOT to use.

  @return Pointer to the successfully created clone or nullptr

  @details
  This function creates a new native_part::Partition_base handler as a
  clone/copy. The clone will use the original m_part_info. It also allocates
  memory for ref + ref_dup. In native_part::Partition_base::open()
  a new partition handlers are created (on the passed mem_root)
  for each partition and are also opened.
*/

handler *ha_spiderpart::clone(const char *name, MEM_ROOT *mem_root) {
  ha_spiderpart *new_handler;

  DBUG_ENTER("ha_spiderpart::clone");

  /* If this->table == nullptr, then the current handler has been created but
  not opened. Prohibit cloning such handler. */
  if (!table) DBUG_RETURN(nullptr);

  new_handler =
      new (mem_root) ha_spiderpart(ht, table_share, m_part_info, this, mem_root);
  if (!new_handler) DBUG_RETURN(nullptr);

  /*
    Allocate new_handler->ref here because otherwise ha_open will allocate it
    on this->table->mem_root and we will not be able to reclaim that memory
    when the clone handler object is destroyed.
  */
  if (!(new_handler->ref =
            (uchar *)mem_root->Alloc(ALIGN_SIZE(ref_length) * 2)))
    goto err;

  /* We will not use clone() interface to clone individual partition
  handlers. This is because tokudb_create_handler() gives ha_tokupart handler
  instead of ha_tokudb handlers. This happens because of presence of parition
  info in TABLE_SHARE. New partition handlers are created for each partiton
  in native_part::Partition_base::open() */
  if (new_handler->ha_open(table, name, table->db_stat,
                           HA_OPEN_IGNORE_IF_LOCKED, nullptr))
    goto err;

  // new_handler->m_pk_can_be_decoded = m_pk_can_be_decoded;
  // new_handler->set_pk_can_be_decoded_for_each_partition();

  DBUG_RETURN((handler *)new_handler);

err:
  delete new_handler;
  DBUG_RETURN(nullptr);
}

ulong ha_spiderpart::index_flags(uint idx, uint part[[maybe_unused]], bool all_parts[[maybe_unused]]) const {
  return ((table_share->key_info[idx].algorithm == HA_KEY_ALG_FULLTEXT)
                  ? 0
                  : (table_share->key_info[idx].algorithm == HA_KEY_ALG_HASH)
                        ? HA_ONLY_WHOLE_INDEX | HA_KEY_SCAN_NOT_ROR
                        : HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER |
                              HA_READ_RANGE | HA_KEYREAD_ONLY);
}

bool ha_spiderpart::rpl_lookup_rows() { return true; }

bool ha_spiderpart::allow_unsafe_alter() const {
  // spider do not store db data, it is safe to do inplace-alter
  return true;
}
#if 0
int ha_spiderpart::update_next_auto_inc_val() {
  if (!m_part_share->auto_inc_initialized || need_info_for_auto_inc())
    return info(HA_STATUS_AUTO);
  return 0;
}
#endif

bool ha_spiderpart::need_info_for_auto_inc() {
  handler **file= m_file;
  DBUG_ENTER("ha_spiderpart::need_info_for_auto_inc");

  do {
    if ((*file)->need_info_for_auto_inc()) {
      DBUG_RETURN(true);
    }
  } while (*(++file));
  DBUG_RETURN(false);
}

int ha_spiderpart::fast_update(THD *thd, mem_root_deque<Item *> &update_fields,
                               mem_root_deque<Item *> &update_values, Item *cond,
                               ha_rows *update, ha_rows *found) {
  if (!check_fast_update_and_delete(thd)) return ENOTSUP;

  // call fast_update of used partitions
  int error = 0;
  ha_rows update_rows = 0;
  ha_rows found_rows = 0;
  for (uint part_id = 0; part_id < m_tot_parts; part_id++) {
    if (m_part_info->is_partition_used(part_id)) {
      if ((error = m_file[part_id]->ha_fast_update(thd, update_fields,
                                                   update_values, cond,
                                                   &update_rows, &found_rows))) {
        break; // error happened
      }
      *update += update_rows;
      *found += found_rows;
      update_rows = 0;
      found_rows = 0;
    }
  }

  return error;
}

int ha_spiderpart::ha_direct_delete_rows(THD *thd, Item *cond, ha_rows *delete_rows) {
  if (!check_fast_update_and_delete(thd)) return ENOTSUP;

  // call direct_delete of used partitions
  int error = 0;
  ha_rows deleted = 0; 
  Diagnostics_area tmp_da(true);
  thd->push_diagnostics_area(&tmp_da, false);
  for (uint part_id = 0; part_id < m_tot_parts; part_id++) {
    if (m_part_info->is_partition_used(part_id)) {
      m_file[part_id]->cond_push(cond, false);
      ha_spider *spider = (ha_spider *)m_file[part_id];
      if ((error = spider->direct_delete_rows_init())) break; 
      if ((error = spider->direct_delete_rows(&deleted))) {
        spider->reset_sql_sql(SPIDER_SQL_TYPE_DELETE_SQL);
        break;
      }
      *delete_rows += deleted;
      deleted = 0;
    }
  }
  thd->pop_diagnostics_area();
  
  return error;
}

/* like Partition_helper::init_record_priority_queue
 * must call rnd_next_quick to fetch data from pushdown sql
 * and use the tmp table of pushdown sql result to do
 * priority sorting so can't just overriding the Partition_helper interfaces 
 * FIXME: maybe optimize in the future
 */
int ha_spiderpart::init_record_priority_queue_spider(TABLE *tmp) {
  uint used_parts = m_part_info->num_partitions_used();
  DBUG_TRACE;
  // assert(!m_ordered_rec_buffer);                            
  // assert(!m_queue);                                         
  if (m_ordered_rec_buffer) {
    my_free(m_ordered_rec_buffer);
    m_ordered_rec_buffer = nullptr;
  }    
  if (m_queue != nullptr) {
    m_queue->clear();
    delete m_queue;
    m_queue = nullptr;
  }
   
  if (!m_queue) {
    m_curr_key_info[0] = tmp->key_info; // only one key from pushdown group by
    m_curr_key_info[1] = nullptr;
    m_queue = new (std::nothrow) Prio_queue(Key_rec_less(m_curr_key_info));
    if (!m_queue) {
      return HA_ERR_OUT_OF_MEM;
    }  
  }    
  /* Initialize the ordered record buffer.  */              
  if (!m_ordered_rec_buffer) {
    uint alloc_len;
    /* 
     * only use the partion id stored before recs
    */ 
    m_rec_offset = PARTITION_BYTES_IN_POS;
    m_ref_usage = REF_NOT_USED;
    m_rec_length = tmp->s->reclength; // use tmp table rec length
    alloc_len = used_parts * (m_rec_offset + m_rec_length);
    /* Allocate a key for temporary use when setting up the scan. */
    alloc_len += tmp->s->max_key_length;

    m_ordered_rec_buffer = static_cast<uchar *>(
        my_malloc(key_memory_partition_sort_buffer, alloc_len, MYF(MY_WME)));
    if (!m_ordered_rec_buffer) {
      return HA_ERR_OUT_OF_MEM;
    }

    /*
      We set-up one record per partition and each record has 2 bytes in
      front where the partition id is written. This is used by ordered
      index_read.
      If we need to also sort by rowid (handler::ref), then m_curr_key_info[1]
      is NULL and we add the rowid before the record.
      We also set-up a reference to the first record for temporary use in
      setting up the scan.
    */
    char *ptr = (char *)m_ordered_rec_buffer;
    uint i;
    for (i = m_part_info->get_first_used_partition(); i < MY_BIT_NONE;
         i = m_part_info->get_next_used_partition(i)) {
      DBUG_PRINT("info", ("init rec-buf for part %u", i));
      int2store(ptr, i);
      ptr += m_rec_offset + m_rec_length;
    }
    m_start_key.key = (const uchar *)ptr;
    /*
      Initialize priority queue, initialized to reading forward.
      Start by only sort by KEY, HA_EXTRA_SECONDARY_SORT_ROWID
      will be given if we should sort by handler::ref too.
    */
    m_queue->m_rec_offset = m_rec_offset;
    if (m_queue->reserve(used_parts)) {
      return HA_ERR_OUT_OF_MEM;
    }
  }
  return 0;
}

/* if tmp not null use the tmp table to create 
 * priority queue 
 * see Partition_helper::init_record_priority_queue
 */
int ha_spiderpart::rnd_init_spider(TABLE *tmp) {
  if (tmp == nullptr) {
    curr_part_id = m_part_info->get_first_used_partition();
    assert(curr_part_id != MY_BIT_NONE);
    return 0;
  } else {
    int error = 0;
    if ((error = init_record_priority_queue_spider(tmp))) {
      destroy_record_priority_queue_spider();
      return error; 
    }
    m_first = true;
    return 0;
  }
}

void ha_spiderpart::destroy_record_priority_queue_spider() {
  if (m_ordered_rec_buffer) {
    my_free(m_ordered_rec_buffer);
    m_ordered_rec_buffer = nullptr;
  }    
  if (m_queue != nullptr) {
    m_queue->clear();
    delete m_queue;
    m_queue = nullptr;
  }
}

// need clean the priority_queue
int ha_spiderpart::rnd_end_spider() {
 destroy_record_priority_queue_spider();
 return 0;
}

/**
  Return the top record in sort order.

  @param[out] buf  Row returned in MySQL Row Format.
*/
  
void ha_spiderpart::return_top_record(uchar *buf) {
  uint part_id;
  uchar *key_buffer = m_queue->top();
  uchar *rec_buffer = key_buffer + m_rec_offset;

  part_id = uint2korr(key_buffer);
  memcpy(buf, rec_buffer, m_rec_length);
  DBUG_PRINT("info", ("from part_id %u", part_id));
  DBUG_DUMP("returned_row", buf, m_table->s->reclength);
  m_last_part = part_id;
  m_top_entry = part_id;
}

/*
 * most like Partition_helper::handle_ordered_index_scan
 * simplify for just pushdown agg group by table scan result
 */
int ha_spiderpart::handle_ordered_scan_spider(uchar *buf, THD *thd,
                                              mem_root_deque<Item *> *fields) {
  uint i;
  std::vector<uchar *> parts;
  bool found = false;
  uchar *part_rec_buf_ptr = m_ordered_rec_buffer;
  int saved_error = -1;
  DBUG_TRACE;
  assert(part_rec_buf_ptr);

  m_top_entry = NO_CURRENT_PART_ID;
  m_queue->clear();
  parts.reserve(m_queue->capacity());                       
  i = m_part_info->get_first_used_partition();
  assert(m_part_info->is_partition_used(i));

  for (; i < MY_BIT_NONE; i = m_part_info->get_next_used_partition(i)) {
    assert(i == uint2korr(part_rec_buf_ptr));
    uchar *rec_buf_ptr = part_rec_buf_ptr + m_rec_offset;   
    uchar *read_buf;
    int error;
    read_buf = buf; // tmp->record[0]

    ha_spider *spider = (ha_spider *)m_file[i];
    error = spider->rnd_next_quick(buf, thd, fields, true);
    memcpy(rec_buf_ptr, read_buf, m_rec_length);
    if (!error) {
      found = true;
      parts.push_back(part_rec_buf_ptr);
    } else if (error != HA_ERR_END_OF_FILE) {
      return error;
    }
    part_rec_buf_ptr += m_rec_offset + m_rec_length;
  }
  if (found) {
    /*
      We found at least one partition with data, now sort all entries and
      after that read the first entry and copy it to the buffer to return it.
    */
    m_queue->m_max_at_top = false;
    m_queue->m_keys = m_curr_key_info;
    assert(m_queue->empty());
    /*
      If PK, we should not sort by rowid, since that is already done
      through the KEY setup.
    */
    assert(!m_curr_key_info[1]);
    m_queue->assign(parts);
    return_top_record(buf);
    DBUG_PRINT("info", ("Record returned from partition %d", m_top_entry));
    return 0;
  }
  return saved_error;
}

/* most like Partition_helper::handle_ordered_next
 * use ha_spider::rnd_next_quick to read data
 */
int ha_spiderpart::handle_ordered_next_spider(uchar *buf, THD *thd,
                                              mem_root_deque<Item *> *fields) {
  int error;
  uint part_id = m_top_entry;
  uchar *rec_buf = m_queue->empty() ? nullptr : m_queue->top() + m_rec_offset;
  uchar *read_buf;
  DBUG_TRACE;

  if (part_id >= m_tot_parts) return HA_ERR_END_OF_FILE;

  read_buf = buf;
  ha_spider *spider = (ha_spider *)m_file[part_id];
  error = spider->rnd_next_quick(buf, thd, fields, true);

  if (error) {
    if (error == HA_ERR_END_OF_FILE) {
      /* Return next buffered row */
      if (!m_queue->empty()) m_queue->pop();
      if (m_queue->empty()) {
        /*
          If priority queue is empty, we have finished fetching rows from all
          partitions. Reset the value of next partition to NONE. This would
          imply HA_ERR_END_OF_FILE for all future calls.
        */
        m_top_entry = NO_CURRENT_PART_ID;
      } else {
        return_top_record(buf);
        DBUG_PRINT("info",
                   ("Record returned from partition %u (2)", m_top_entry));
        error = 0;
      }
    }
    return error;
  } 

  memcpy(rec_buf, read_buf, m_rec_length);
  m_queue->update_top();
  return_top_record(buf);
  DBUG_PRINT("info", ("Record returned from partition %u", m_top_entry));
  return 0;
}

int ha_spiderpart::rnd_next_spider(THD *thd, uchar *buf,
                                   mem_root_deque<Item *> *fields,
                                   bool need_rewrite,
                                   bool ordered) {
  int error = 0;
  if (!ordered) {
    // support scan multi parts
    assert(curr_part_id != MY_BIT_NONE);
    for (; curr_part_id < m_tot_parts; ) { 
      ha_spider *spider = (ha_spider *)m_file[curr_part_id];
      error = spider->rnd_next_quick(buf, thd, fields, need_rewrite);
      if (error == HA_ERR_END_OF_FILE) {
        curr_part_id = m_part_info->get_next_used_partition(curr_part_id);
        // change to next part
        if (curr_part_id < m_tot_parts) continue;
        // finish
        error = -1; // not error
      }
      return error;
    }
  } else {
    // use priority queue to sort data from multi-partitions
    if (m_first) {
      m_first = false;
      error = handle_ordered_scan_spider(buf, thd, fields);
    } else {
      error = handle_ordered_next_spider(buf, thd, fields);
    }

    if (error) destroy_record_priority_queue_spider();
    if (error == HA_ERR_END_OF_FILE) error = -1; // not error

    return error;
  }
  // make compiler happy 
  return -1;
}

int ha_spiderpart::direct_pushdown_update(THD *thd, ha_rows *update_rows, ha_rows *found_rows) {
  uint part_id = m_part_info->get_first_used_partition();
  ha_spider *spider = (ha_spider *)m_file[part_id];
  return spider->direct_pushdown_update(thd, update_rows, found_rows);
}

int ha_spiderpart::direct_pushdown_insert(THD *thd, ha_rows *insert_rows, ha_rows *found_rows) {
  uint part_id = m_part_info->get_first_used_partition();
  ha_spider *spider = (ha_spider *)m_file[part_id];
  return spider->direct_pushdown_insert(thd, insert_rows, found_rows);
}

int ha_spiderpart::direct_pushdown_delete(THD *thd, ha_rows *delete_rows, ha_rows *found_rows) {
  uint part_id = m_part_info->get_first_used_partition();
  ha_spider *spider = (ha_spider *)m_file[part_id];
  return spider->direct_pushdown_delete(thd, delete_rows, found_rows);
}

const Item *ha_spiderpart::cond_push(const Item *cond, bool other_tbls_ok [[maybe_unused]]) {
  DBUG_ENTER("ha_spider::cond_push");
  // push condition to all parts
  for (uint part_id = 0; part_id < m_tot_parts; part_id++) {
    if (m_part_info->is_partition_used(part_id)) {
      m_file[part_id]->cond_push(cond, false);
    }
  }

  // Only for explain plan.
  pushed_cond = cond;
  DBUG_RETURN(NULL);
}

void ha_spiderpart::set_file_part_no(handler *file, int part_no) {
  ((ha_spider*)file)->part_no = part_no;
}

std::string ha_spiderpart::explain_extra() const {
  std::string str("");

  if (pushed_cond != nullptr) {
    str += ", with pushed condition: " + ItemToString(pushed_cond);
  }
  return str;
}

SPIDER_CONN *ha_spiderpart::get_spider_conn_for_pq(uint partid) {
  auto *spider = down_cast<ha_spider *>(m_file[partid]);
  return spider->get_spider_conn_for_pq(0);
}

std::string ha_spiderpart::get_spider_conn_hashkey(uint partid) const {
  auto *spider = down_cast<ha_spider *>(m_file[partid]);
  return spider->get_spider_conn_hashkey(0);
}

bool ha_spiderpart::check_fast_update_and_delete(THD *thd) {
  bool has_multi_parts = (m_part_info->num_partitions_used() > 1);

  /**
   * We can not use fast update or delete if any of the following
   * conditions are false:
   * 1. Update partition key. Might cross partitions.
   * 2. There are triggers
   * 3. Ignore is set
   * 4. There is a virtual not stored column in the WHERE clause.
   * 5. ORDER BY or LIMIT with multiple partitions.
   */
  if (m_table->triggers || thd->lex->is_ignore() ||
      partition_key_modified(m_table, m_table->write_set) ||
      m_table->check_virtual_columns_marked_for_read() ||
      m_table->check_virtual_columns_marked_for_write()) {
    return false;
  }

  for (uint part_id = 0; part_id < m_tot_parts; part_id++) {
    if (m_part_info->is_partition_used(part_id)) {
      ha_spider *spider = (ha_spider *)m_file[part_id];
      if (!spider->check_fast_update_and_delete(has_multi_parts)) {
        return false;
      }
    }
  }

  return true;
}
