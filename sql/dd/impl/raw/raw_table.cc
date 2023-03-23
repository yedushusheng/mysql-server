/* Copyright (c) 2014, 2019, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/dd/impl/raw/raw_table.h"

#include <stddef.h>
#include <algorithm>
#include <new>

#include "m_string.h"
#include "my_base.h"
#include "my_bitmap.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql/udf_registration_types.h"
#include "sql/dd/impl/object_key.h"          // dd::Object_key
#include "sql/dd/impl/raw/raw_key.h"         // dd::Raw_key
#include "sql/dd/impl/raw/raw_record.h"      // dd::Raw_record
#include "sql/dd/impl/raw/raw_record_set.h"  // dd::Raw_record_set
#include "sql/handler.h"

namespace dd {

///////////////////////////////////////////////////////////////////////////
/** Note:默认初始化mysql库
 * 
 * 调用：
 * #0  dd::Raw_table::Raw_table (this=0x7f4f2c0a7430, lock_type=TL_READ, name=...) at /sql/dd/impl/raw/raw_table.cc:47
 * #1  0x00000000067ab584 in dd::Open_dictionary_tables_ctx::add_table (this=0x7f4fb04f2c30, name=...) at /sql/dd/impl/transaction_impl.cc:52
 * #2  0x0000000006b69bfc in dd::Open_dictionary_tables_ctx::add_table<dd::tables::Foreign_keys> (this=0x7f4fb04f2c30)
    at /sql/dd/impl/transaction_impl.h:96
 * #3  0x0000000006b68210 in dd::Foreign_key_impl::register_tables (otx=0x7f4fb04f2c30) at /sql/dd/impl/types/foreign_key_impl.cc:326
 * #4  0x000000000683c444 in dd::Open_dictionary_tables_ctx::register_tables<dd::Foreign_key> (this=0x7f4fb04f2c30)
    at /sql/dd/impl/transaction_impl.h:91
 * #5  0x00000000067fb3f4 in dd::cache::Dictionary_client::fetch_fk_children_uncached (this=0x7f4f3c01d9f0, parent_schema=..., parent_name=..., parent_engine=..., 
    uncommitted=true, children_schemas=0x7f4fb04f2ef0, children_names=0x7f4fb04f2ed0) at /sql/dd/impl/cache/dictionary_client.cc:2337
 * #6  0x00000000042e8056 in fetch_fk_children_uncached_uncommitted_normalized (thd=0x7f4f3c019f10, parent_schema=0x7f4f2c027a28 "test", parent_name=0x7f4f2c026da8 "tx1", 
    parent_engine=0x9d12a13 <innobase_hton_name> "InnoDB", fk_children=0x7f4fb04f2fd0) at /sql/sql_table.cc:9206
 * #7  0x00000000042e86c7 in collect_fk_children (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", arg_table_name=0x7f4f2c026da8 "tx1", hton=0x124030e0, 
    lock_type=MDL_EXCLUSIVE, mdl_requests=0x7f4fb04f3090) at /sql/sql_table.cc:9243
 * #8  0x00000000042eeaad in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10006
 * #9  0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #10 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #11 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #12 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #13 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
 * #14 0x00000000044d5fd5 in bthread_handle_connection (arg=0x1233d590) at /sql/tdsql/connection_handler.cc:118
 * #15 0x00000000085cf29f in bthread::TaskGroup::task_runner(long) ()
    at /extra/incubator-brpc/incubator-brpc-1.0.0/src/bthread/task_group.cpp:295
 * #16 0x00000000086053b1 in bthread_make_fcontext () at /extra/incubator-brpc/incubator-brpc-1.0.0/src/bvar/detail/series.h:127
*/
Raw_table::Raw_table(thr_lock_type lock_type, const String_type &name)
    : m_table_list(STRING_WITH_LEN("mysql"), name.c_str(), name.length(),
                   name.c_str(), lock_type) {
  m_table_list.is_dd_ctx_table = true;
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
  Find record and populate raw_record.

  @param key - Pointer to Raw_record after moving to next row.
  @param r - row on which data is updated.

  @return false - On success. 1) We found a row.
                              2) OR Either we don't have any matching rows
  @return true - On failure and error is reported.
*/
/** Note:外部接口
 * 该接口被共享缓存get方法调用,用于检查是否存在不存在则到innodb查询
 * 直接调用handler接口根据传入的key(比如表名)查找记录
 * 即根据schema_id和table_name读取对应mysql.tables表的文件记录Raw_table::find_record
 * 
 * 调用:
 * #0  dd::Raw_table::find_record (this=0x7f4f2c0a7430, key=..., r=...) at /sql/dd/impl/raw/raw_table.cc:66
 * #1  0x0000000006a0deee in dd::cache::Storage_adapter::get<dd::Item_name_key, dd::Abstract_table> (thd=0x7f4f3c019f10, key=..., isolation=ISO_READ_COMMITTED, 
    bypass_core_registry=false, object=0x7f4fb04f2560) at /sql/dd/impl/cache/storage_adapter.cc:181
 * #2  0x0000000006a00a70 in dd::cache::Shared_dictionary_cache::get_uncached<dd::Item_name_key, dd::Abstract_table> (
    this=0xf14ce20 <dd::cache::Shared_dictionary_cache::instance()::s_cache>, thd=0x7f4f3c019f10, key=..., isolation=ISO_READ_COMMITTED, object=0x7f4fb04f2560)
    at /sql/dd/impl/cache/shared_dictionary_cache.cc:128
 * #3  0x0000000006a004a4 in dd::cache::Shared_dictionary_cache::get<dd::Item_name_key, dd::Abstract_table> (
    this=0xf14ce20 <dd::cache::Shared_dictionary_cache::instance()::s_cache>, thd=0x7f4f3c019f10, key=..., element=0x7f4fb04f25d8)
    at /sql/dd/impl/cache/shared_dictionary_cache.cc:113
 * #4  0x0000000006845d3c in dd::cache::Dictionary_client::acquire<dd::Item_name_key, dd::Abstract_table> (this=0x7f4f3c01d9f0, key=..., object=0x7f4fb04f2648, 
    local_committed=0x7f4fb04f2647, local_uncommitted=0x7f4fb04f2646) at /sql/dd/impl/cache/dictionary_client.cc:910
 * #5  0x0000000006809b66 in dd::cache::Dictionary_client::acquire<dd::Abstract_table> (this=0x7f4f3c01d9f0, schema_name=..., object_name=..., object=0x7f4fb04f2778)
    at /sql/dd/impl/cache/dictionary_client.cc:1379
 * #6  0x00000000042e3950 in check_if_table_exists (thd=0x7f4f3c019f10, schema_name=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", alias=0x7f4f2c026da8 "tx1", 
    ha_lex_create_tmp_table=false, ha_create_if_not_exists=false, internal_tmp_table=false) at /sql/sql_table.cc:8533
 * #7  0x00000000042e5399 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8823
 * #8  0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #9  0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #10 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #11 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #12 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #13 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #14 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
 * #15 0x00000000044d5fd5 in bthread_handle_connection (arg=0x1233d590) at /sql/tdsql/connection_handler.cc:118
 * #16 0x00000000085cf29f in bthread::TaskGroup::task_runner(long) ()
    at /extra/incubator-brpc/incubator-brpc-1.0.0/src/bthread/task_group.cpp:295
 * #17 0x00000000086053b1 in bthread_make_fcontext () at /extra/incubator-brpc/incubator-brpc-1.0.0/src/bvar/detail/series.h:127
*/
bool Raw_table::find_record(const Object_key &key,
                            std::unique_ptr<Raw_record> &r) {
  DBUG_TRACE;

  TABLE *table = get_table();
  std::unique_ptr<Raw_key> k(key.create_access_key(this));

  int rc;
  if (!table->file->inited &&
      (rc = table->file->ha_index_init(k->index_no, true))) {
    table->file->print_error(rc, MYF(0));
    return true;
  }

  rc = table->file->ha_index_read_idx_map(
      table->record[0], k->index_no, k->key, k->keypart_map,
      (k->keypart_map == HA_WHOLE_KEY) ? HA_READ_KEY_EXACT : HA_READ_PREFIX);

  if (table->file->inited)
    table->file->ha_index_end();  // Close the scan over the index

  // Row not found.
  if (rc == HA_ERR_KEY_NOT_FOUND || rc == HA_ERR_END_OF_FILE) {
    r.reset(nullptr);
    return false;
  }

  // Got unexpected error.
  if (rc) {
    table->file->print_error(rc, MYF(0));
    r.reset(nullptr);
    return true;
  }

  r.reset(new Raw_record(table));
  return false;
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
  Write modified data into row buffer.

  @param key - Pointer to Raw_record after moving to next row.
  @param r - row on which data is updated.

  @return false - On success.
  @return true - On failure and error is reported.
*/
bool Raw_table::prepare_record_for_update(const Object_key &key,
                                          std::unique_ptr<Raw_record> &r) {
  DBUG_TRACE;

  TABLE *table = get_table();

  // Setup row buffer for update
  table->use_all_columns();
  bitmap_set_all(table->write_set);
  bitmap_set_all(table->read_set);

  if (find_record(key, r)) return true;

  store_record(table, record[1]);

  return false;
}

///////////////////////////////////////////////////////////////////////////

Raw_new_record *Raw_table::prepare_record_for_insert() {
  return new (std::nothrow) Raw_new_record(get_table());
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
   Initiate table scan operation for the given key.

  @return false - on success.
  @return true - on failure and error is reported.
*/
bool Raw_table::open_record_set(const Object_key *key,
                                std::unique_ptr<Raw_record_set> &rs) {
  DBUG_TRACE;

  Raw_key *access_key = nullptr;

  // Create specific access key if submitted.
  if (key) {
    restore_record(get_table(), s->default_values);
    access_key = key->create_access_key(this);
  }

  std::unique_ptr<Raw_record_set> rs1(
      new (std::nothrow) Raw_record_set(get_table(), access_key));

  if (rs1->open()) return true;

  rs = std::move(rs1);

  return false;
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
  Find last record in table and populate raw_record.

  @param key - Pointer to Raw_record after moving to next row.
  @param r - row on which data is updated.

  @return false - On success. 1) We found a row.
                              2) OR Either we don't have any matching rows
  @return true - On failure and error is reported.
*/
/* purecov: begin deadcode */
bool Raw_table::find_last_record(const Object_key &key,
                                 std::unique_ptr<Raw_record> &r) {
  DBUG_TRACE;

  TABLE *table = get_table();
  std::unique_ptr<Raw_key> k(key.create_access_key(this));

  int rc;
  if (!table->file->inited &&
      (rc = table->file->ha_index_init(k->index_no, true))) {
    table->file->print_error(rc, MYF(0));
    return true;
  }

  rc = table->file->ha_index_read_idx_map(table->record[0], k->index_no, k->key,
                                          k->keypart_map,
                                          HA_READ_PREFIX_LAST_OR_PREV);

  if (table->file->inited)
    table->file->ha_index_end();  // Close the scan over the index

  // Row not found.
  if (rc == HA_ERR_KEY_NOT_FOUND || rc == HA_ERR_END_OF_FILE) {
    r.reset(nullptr);
    return false;
  }

  // Got unexpected error.
  if (rc) {
    table->file->print_error(rc, MYF(0));
    r.reset(nullptr);
    return true;
  }

  r.reset(new Raw_record(table));

  return false;
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
