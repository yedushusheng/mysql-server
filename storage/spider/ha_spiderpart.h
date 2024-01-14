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
#pragma once

#include "sql/partitioning/partition_base.h"

class st_spider_conn;
typedef st_spider_conn SPIDER_CONN;

// #include "./ha_spider.h"
extern PSI_memory_key key_memory_partition_sort_buffer;

/* This class must contain engine-specific functions for partitioning */
class ha_spiderpart : public native_part::Partition_base {
 typedef Priority_queue<uchar *, std::vector<uchar *>, Key_rec_less>
      Prio_queue;
  static const uint NO_CURRENT_PART_ID = UINT_MAX32;
 public:
  ha_spiderpart(handlerton *hton, TABLE_SHARE *table_arg)
      : Partition_base(hton, table_arg){}

  ha_spiderpart(handlerton *hton, TABLE_SHARE *table_arg,
               partition_info *part_info,
               native_part::Partition_base *clone_base,
               MEM_ROOT *clone_mem_root)
      : native_part::Partition_base(hton, table_arg, clone_base,
                                    clone_mem_root) {
    this->get_partition_handler()->set_part_info(part_info, true);
  }

  ~ha_spiderpart() override { destroy_record_priority_queue_spider(); }

  std::string explain_extra() const override;
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override;
  int delete_table(const char *name, const dd::Table *table_def [[maybe_unused]]) override;
  enum row_type get_partition_row_type(const dd::Table *, uint) override {
    return ROW_TYPE_NOT_USED;
  }
  bool allow_unsafe_alter() const override;
  bool get_no_parts(const char *name[[maybe_unused]], uint *num_parts) override {
    DBUG_ENTER("ha_partition::get_no_parts");
    *num_parts= m_tot_parts;
    DBUG_RETURN(0);
  }

  // int update_next_auto_inc_val() override;
  bool need_info_for_auto_inc() override;

  bool check_fast_update_and_delete(THD *);
  int fast_update(THD *, mem_root_deque<Item *> &,
                  mem_root_deque<Item *> &, Item *, ha_rows *, ha_rows *) override;

  int ha_direct_delete_rows(THD *thd, Item *cond, ha_rows *delete_rows) override;
  // quick interface for pushdown the sql to remote node
  int rnd_init_spider(TABLE *tmp) override;

  int rnd_next_spider(THD *thd, uchar *buf, mem_root_deque<Item *> *fields,
                      bool need_rewrite, bool ordered) override;

  int direct_pushdown_update(THD *thd, ha_rows *update_rows, ha_rows *found_rows) override;

  int direct_pushdown_insert(THD *thd, ha_rows *insert_rows, ha_rows *found_rows) override;

  int direct_pushdown_delete(THD *thd, ha_rows *delete_rows, ha_rows *found_rows) override;

  const Item *cond_push(const Item *cond, bool other_tbls_ok [[maybe_unused]]) override;
  
  int rnd_end_spider() override;

  int init_parallel_scan(parallel_scan_handle_t *handle MY_ATTRIBUTE((unused)),
                         ulong *nranges MY_ATTRIBUTE((unused)),
                         parallel_scan_desc_t *scan_desc
                             MY_ATTRIBUTE((unused))) override {
    return 0;
  }

  int attach_parallel_scan(
      parallel_scan_handle_t scan_handle MY_ATTRIBUTE((unused))) override {
    return 0;
  }

  int estimate_parallel_scan_ranges(
      uint keynr MY_ATTRIBUTE((unused)),
      key_range *min_key MY_ATTRIBUTE((unused)),
      key_range *max_key MY_ATTRIBUTE((unused)),
      bool type_ref_or_null MY_ATTRIBUTE((unused)),
      ulong *nranges MY_ATTRIBUTE((unused)),
      ha_rows *nrows MY_ATTRIBUTE((unused))) override {
    *nranges = m_part_info->num_partitions_used();
    return 0;
  }
  // Get the spider_conn for the parallel query.
  SPIDER_CONN *get_spider_conn_for_pq(uint partid);
  std::string get_spider_conn_hashkey(uint partid) const;

 private:
  handler *get_file_handler(TABLE_SHARE *share, MEM_ROOT *alloc) const override;
  handler *clone(const char *name, MEM_ROOT *mem_root) override;
  ulong index_flags(uint inx, uint part, bool all_parts) const override;
  bool rpl_lookup_rows() override;

  // void set_pk_can_be_decoded_for_each_partition();
  // mutable bool m_pk_can_be_decoded = true;
  void set_file_part_no(handler *file, int part_no) override;
  // support iterate multi parts 
  uint curr_part_id = 0;
  // for pushdown agg use priority queue to return
  // pushdown agg recs order by group clause
  int init_record_priority_queue_spider(TABLE *tmp);
  bool m_first{false};
  int handle_ordered_scan_spider(uchar *buf, THD *thd, mem_root_deque<Item *> *fields);
  int handle_ordered_next_spider(uchar *buf, THD *thd, mem_root_deque<Item *> *fields);
  void return_top_record(uchar *buf);
  void destroy_record_priority_queue_spider();
};
