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

#include "sql/dd/impl/raw/raw_record.h"

#include <stddef.h>

#include "m_ctype.h"
#include "my_base.h"
#include "my_bitmap.h"
#include "my_dbug.h"
#include "my_time.h"
#include "mysql/udf_registration_types.h"
#include "sql/dd/properties.h"  // dd::Properties
#include "sql/field.h"          // Field
#include "sql/handler.h"
#include "sql/my_decimal.h"
#include "sql/sql_const.h"
#include "sql/table.h"   // TABLE
#include "sql/tztime.h"  // Time_zone_offset
#include "sql_string.h"
#include "template_utils.h"

namespace dd {

///////////////////////////////////////////////////////////////////////////
/** Note:外部接口
 * 
 * 调用:
 * #0  dd::Raw_record::Raw_record (this=0x7f4f2c037ce0, table=0x7f4f2c07ae90) at /sql/dd/impl/raw/raw_record.cc:47
 * #1  0x0000000006a43fcf in dd::Raw_new_record::Raw_new_record (this=0x7f4f2c037ce0, table=0x7f4f2c07ae90)
    at /sql/dd/impl/raw/raw_record.cc:289
 * #2  0x0000000006a45a0f in dd::Raw_table::prepare_record_for_insert (this=0x7f4f2c031c50) at /sql/dd/impl/raw/raw_table.cc:135
 * #3  0x0000000006bedb10 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:118
 * #4  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #5  0x000000000681de25 in dd::cache::Dictionary_client::store<dd::Table> (this=0x7f4f3c01d9f0, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #6  0x00000000042bca3c in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1096
 * #7  0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
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
*/
Raw_record::Raw_record(TABLE *table) : m_table(table) {
  // Note:使用bitmap优化
  bitmap_set_all(m_table->read_set);
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
    Update table record into SE

  @return true - on failure and error is reported.
  @return false - on success.
*/
/** Note:外部接口
 * 更新存储引擎的record
 * Storage_adapter::store负责调用
 * 
 * 调用:
 * #0  dd::Raw_record::update (this=0x7f506801aa70) at /sql/dd/impl/raw/raw_record.cc:61
 * #1  0x0000000006bed9e9 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c0a1d70, otx=0x7f4fb04f06b0)
    at /sql/dd/impl/types/weak_object_impl.cc:111
 * #2  0x00000000066969a5 in dd::Collection<dd::Index_element*>::store_items (this=0x7f4f2c0352b8, otx=0x7f4fb04f06b0)
    at /sql/dd/collection.cc:218
 * #3  0x0000000006b740a6 in dd::Index_impl::store_children (this=0x7f4f2c035180, otx=0x7f4fb04f06b0) at /sql/dd/impl/types/index_impl.cc:146
 * #4  0x0000000006beda61 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c035180, otx=0x7f4fb04f06b0)
    at /sql/dd/impl/types/weak_object_impl.cc:113
 * #5  0x0000000006696511 in dd::Collection<dd::Index*>::store_items (this=0x7f4f2c033f10, otx=0x7f4fb04f06b0) at /sql/dd/collection.cc:218
 * #6  0x0000000006bb4b8c in dd::Table_impl::store_children (this=0x7f4f2c033d60, otx=0x7f4fb04f06b0) at /sql/dd/impl/types/table_impl.cc:368
 * #7  0x0000000006beda61 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c033d60, otx=0x7f4fb04f06b0)
    at /sql/dd/impl/types/weak_object_impl.cc:113
 * #8  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c034040)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #9  0x000000000681e68c in dd::cache::Dictionary_client::update<dd::Table> (this=0x7f4f3c01d9f0, new_object=0x7f4f2c034040)
    at /sql/dd/impl/cache/dictionary_client.cc:2655
 * #10 0x00000000038ef5e6 in ha_create_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    create_info=0x7f4fb04f34e0, create_fields=0x7f4fb04f3420, update_create_info=false, is_temp_table=false, table_def=0x7f4f2c034040)
    at /sql/handler.cc:5551
 * #11 0x00000000042bd162 in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1161
 * #12 0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
 * #13 0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #14 0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #15 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #16 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #17 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #18 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #19 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
bool Raw_record::update() {
  DBUG_TRACE;

  int rc = m_table->file->ha_update_row(m_table->record[1], m_table->record[0]);

  /**
    We ignore HA_ERR_RECORD_IS_THE_SAME here for following reason.
    If in case we are updating childrens of some DD object,
    and only one of the children has really changed and other have
    not. Then we get HA_ERR_RECORD_IS_THE_SAME for children (rows)
    which has not really been modified.

    Currently DD framework creates/updates *all* childrens at once
    and we don't have machinism to update only required child.
    May be this is part of task which will implement inplace
    alter in better way, updating only the changed child (or row)
    and ignore others. Then we can remove the below check which
    ignores HA_ERR_RECORD_IS_THE_SAME.
  */

  if (rc && rc != HA_ERR_RECORD_IS_THE_SAME) {
    m_table->file->print_error(rc, MYF(0));
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
    Drop the record from SE

  @return true - on failure and error is reported.
  @return false - on success.
*/
bool Raw_record::drop() {
  DBUG_TRACE;
  
  // Note:从存储引擎层StorageEngine删除记录,调用具体存储引擎接口
  int rc = m_table->file->ha_delete_row(m_table->record[1]);

  if (rc) {
    m_table->file->print_error(rc, MYF(0));
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////

Field *Raw_record::field(int field_no) const {
  return m_table->field[field_no];
}

///////////////////////////////////////////////////////////////////////////

/** Note:外部接口
 * 加载主键id(在Engine层完成数据字典操作后,在Server层加载)
 * 
 * 调用:
 * #0  dd::Raw_record::store_pk_id (this=0x7f4f2c0c71d0, field_no=0, id=18446744073709551615) at /sql/dd/impl/raw/raw_record.cc:119
 * #1  0x0000000006b5c3e2 in dd::Entity_object_impl::store_id (this=0x7f4f2c0c1a10, r=0x7f4f2c0c71d0, field_idx=0)
    at /sql/dd/impl/types/entity_object_impl.cc:80
 * #2  0x0000000006bcbf7f in dd::Tablespace_impl::store_attributes (this=0x7f4f2c0c1a10, r=0x7f4f2c0c71d0)
    at /sql/dd/impl/types/tablespace_impl.cc:201
 * #3  0x0000000006bedb87 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c0c1a10, otx=0x7f4fb04efd60)
    at /sql/dd/impl/types/weak_object_impl.cc:122
 * #4  0x0000000006a2c5dc in dd::cache::Storage_adapter::store<dd::Tablespace> (thd=0x7f4f3c019f10, object=0x7f4f2c0c1a30)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #5  0x0000000006821371 in dd::cache::Dictionary_client::store<dd::Tablespace> (this=0x7f4f3c01d9f0, object=0x7f4f2c0c1a30)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #6  0x0000000007877cdd in dd_create_tablespace (dd_client=0x7f4f3c01d9f0, thd=0x7f4f3c019f10, dd_space_name=0x7f4f2c0c1bd8 "test/tx1", space_id=3, flags=16417, 
    filename=0x7f4f2c0c1c40 "./test/tx1.ibd", discarded=false, dd_space_id=@0x7f4fb04f0158: 18446744073709551615)
    at /storage/innobase/dict/dict0dd.cc:3177
 * #7  0x0000000007878357 in dd_create_implicit_tablespace (dd_client=0x7f4f3c01d9f0, thd=0x7f4f3c019f10, space_id=3, space_name=0x7f4f2c0376f0 "test/tx1", 
    filename=0x7f4f2c0c1c40 "./test/tx1.ibd", discarded=false, dd_space_id=@0x7f4fb04f0158: 18446744073709551615)
    at /storage/innobase/dict/dict0dd.cc:3207
 * #8  0x0000000006f6a52e in create_table_info_t::create_table_update_global_dd<dd::Table> (this=0x7f4fb04f0280, dd_table=0x7f4f2c034040)
    at /storage/innobase/handler/ha_innodb.cc:14848
 * #9  0x0000000006f6bf57 in innobase_basic_ddl::create_impl<dd::Table> (thd=0x7f4f3c019f10, name=0x7f4fb04f2d50 "./test/tx1", form=0x7f4fb04f0ef0, 
    create_info=0x7f4fb04f34e0, dd_tab=0x7f4f2c034040, file_per_table=true, evictable=true, skip_strict=false, old_flags=0, old_flags2=0)
    at /storage/innobase/handler/ha_innodb.cc:14951
 * #10 0x0000000006f211ed in ha_innobase::create (this=0x7f4f2c0bf158, name=0x7f4fb04f2d50 "./test/tx1", form=0x7f4fb04f0ef0, create_info=0x7f4fb04f34e0, 
    table_def=0x7f4f2c034040) at /storage/innobase/handler/ha_innodb.cc:15905
 * #11 0x00000000038eda43 in handler::ha_create (this=0x7f4f2c0bf158, name=0x7f4fb04f2d50 "./test/tx1", form=0x7f4fb04f0ef0, info=0x7f4fb04f34e0, table_def=0x7f4f2c034040)
    at /sql/handler.cc:5283
 * #12 0x00000000038ef252 in ha_create_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    create_info=0x7f4fb04f34e0, create_fields=0x7f4fb04f3420, update_create_info=false, is_temp_table=false, table_def=0x7f4f2c034040)
    at /sql/handler.cc:5530
 * #13 0x00000000042bd162 in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1161
 * #14 0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
 * #15 0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #16 0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
*/
bool Raw_record::store_pk_id(int field_no, Object_id id) {
  field(field_no)->set_notnull();

  return (id == INVALID_OBJECT_ID) ? false : store(field_no, id);
}

///////////////////////////////////////////////////////////////////////////

/** Note:外部接口
 * 
 * 调用:
 * #0  dd::Raw_record::store_ref_id (this=0x7f4f2c037ce0, field_no=1, id=6) at /sql/dd/impl/raw/raw_record.cc:127
 * #1  0x0000000006b3a794 in dd::Abstract_table_impl::store_attributes (this=0x7f4f2c020930, r=0x7f4f2c037ce0)
    at /sql/dd/impl/types/abstract_table_impl.cc:179
 * #2  0x0000000006bb602e in dd::Table_impl::store_attributes (this=0x7f4f2c020930, r=0x7f4f2c037ce0) at /sql/dd/impl/types/table_impl.cc:509
 * #3  0x0000000006bedb87 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:122
 * #4  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #5  0x000000000681de25 in dd::cache::Dictionary_client::store<dd::Table> (this=0x7f4f3c01d9f0, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #6  0x00000000042bca3c in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1096
 * #7  0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
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
*/
bool Raw_record::store_ref_id(int field_no, Object_id id) {
  if (id == INVALID_OBJECT_ID) {
    set_null(field_no, true);
    return false;
  }

  set_null(field_no, false);
  type_conversion_status rc = field(field_no)->store(id, true);

  DBUG_ASSERT(rc == TYPE_OK);
  return rc != TYPE_OK;
}

///////////////////////////////////////////////////////////////////////////

void Raw_record::set_null(int field_no, bool is_null) {
  if (is_null)
    field(field_no)->set_null();
  else
    field(field_no)->set_notnull();
}

///////////////////////////////////////////////////////////////////////////
/** Note:外部接口
 * 
 * 调用:
 * #0  dd::Raw_record::store (this=0x7f4f2c037ce0, field_no=2, s=..., is_null=false) at /sql/dd/impl/raw/raw_record.cc:151
 * #1  0x0000000006b5c444 in dd::Entity_object_impl::store_name (this=0x7f4f2c020930, r=0x7f4f2c037ce0, field_idx=2, is_null=false)
    at /sql/dd/impl/types/entity_object_impl.cc:87
 * #2  0x0000000006b5c499 in dd::Entity_object_impl::store_name (this=0x7f4f2c020930, r=0x7f4f2c037ce0, field_idx=2)
    at /sql/dd/impl/types/entity_object_impl.cc:93
 * #3  0x0000000006b3a74c in dd::Abstract_table_impl::store_attributes (this=0x7f4f2c020930, r=0x7f4f2c037ce0)
    at /sql/dd/impl/types/abstract_table_impl.cc:178
 * #4  0x0000000006bb602e in dd::Table_impl::store_attributes (this=0x7f4f2c020930, r=0x7f4f2c037ce0) at /sql/dd/impl/types/table_impl.cc:509
 * #5  0x0000000006bedb87 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:122
 * #6  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #7  0x000000000681de25 in dd::cache::Dictionary_client::store<dd::Table> (this=0x7f4f3c01d9f0, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #8  0x00000000042bca3c in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1096
 * #9  0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
 * #10 0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #11 0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #12 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #13 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #14 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #15 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #16 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
bool Raw_record::store(int field_no, const String_type &s, bool is_null) {
  set_null(field_no, is_null);

  if (is_null) return false;

  type_conversion_status rc =
      field(field_no)->store(s.c_str(), s.length(), system_charset_info);

  DBUG_ASSERT(rc == TYPE_OK);
  return rc != TYPE_OK;
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store(int field_no, ulonglong ull, bool is_null) {
  set_null(field_no, is_null);

  if (is_null) return false;

  type_conversion_status rc = field(field_no)->store(ull, true);

  DBUG_ASSERT(rc == TYPE_OK);
  return rc != TYPE_OK;
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store(int field_no, longlong ll, bool is_null) {
  set_null(field_no, is_null);

  if (is_null) return false;

  type_conversion_status rc = field(field_no)->store(ll, false);

  DBUG_ASSERT(rc == TYPE_OK);
  return rc != TYPE_OK;
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store(int field_no, const Properties &p) {
  return store(field_no, p.raw_string(), p.empty());
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store_time(int field_no, my_time_t val, bool is_null) {
  set_null(field_no, is_null);

  if (is_null) return false;

  MYSQL_TIME time;

  my_tz_OFFSET0->gmt_sec_to_TIME(&time, val);
  return field(field_no)->store_time(&time);
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store_timestamp(int field_no, const timeval &tv) {
  field(field_no)->store_timestamp(&tv);
  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::store_json(int field_no, const Json_wrapper &json) {
  Field_json *json_field = down_cast<Field_json *>(field(field_no));
  return json_field->store_json(&json) != TYPE_OK;
}

///////////////////////////////////////////////////////////////////////////

bool Raw_record::is_null(int field_no) const {
  return field(field_no)->is_null();
}

///////////////////////////////////////////////////////////////////////////

longlong Raw_record::read_int(int field_no) const {
  return field(field_no)->val_int();
}

///////////////////////////////////////////////////////////////////////////

ulonglong Raw_record::read_uint(int field_no) const {
  return static_cast<ulonglong>(field(field_no)->val_int());
}

///////////////////////////////////////////////////////////////////////////

String_type Raw_record::read_str(int field_no) const {
  char buff[MAX_FIELD_WIDTH];
  String val(buff, sizeof(buff), &my_charset_bin);

  field(field_no)->val_str(&val);

  return String_type(val.ptr(), val.length());
}

///////////////////////////////////////////////////////////////////////////

Object_id Raw_record::read_ref_id(int field_no) const {
  return field(field_no)->is_null() ? dd::INVALID_OBJECT_ID
                                    : read_int(field_no);
}

///////////////////////////////////////////////////////////////////////////

my_time_t Raw_record::read_time(int field_no) const {
  MYSQL_TIME time;
  bool not_used;

  field(field_no)->get_date(&time, TIME_DATETIME_ONLY);
  return my_tz_OFFSET0->TIME_to_gmt_sec(&time, &not_used);
}

///////////////////////////////////////////////////////////////////////////

timeval Raw_record::read_timestamp(int field_no) const {
  int warnings = 0;
  timeval tv;
  if (field(field_no)->get_timestamp(&tv, &warnings)) {
    DBUG_ASSERT(false);
    return {0, 0};
  }
  return tv;
}

////////////////////////////////////////////////////////////////////////////

bool Raw_record::read_json(int field_no, Json_wrapper *json_wrapper) const {
  return down_cast<Field_json *>(field(field_no))->val_json(json_wrapper);
}

///////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////

Raw_new_record::Raw_new_record(TABLE *table) : Raw_record(table) {
  bitmap_set_all(m_table->write_set);
  bitmap_set_all(m_table->read_set);

  m_table->next_number_field = m_table->found_next_number_field;
  m_table->autoinc_field_has_explicit_non_null_value = true;

  restore_record(m_table, s->default_values);
}

///////////////////////////////////////////////////////////////////////////

/**
  @brief
    Create new record in SE

  @return true - on failure and error is reported.
  @return false - on success.
*/
/** Note:外部接口
 * 在存储引擎层创建一条记录
 * 
 * 调用:
 * #0  dd::Raw_new_record::insert (this=0x7f4f2c037ce0) at /sql/dd/impl/raw/raw_record.cc:309
 * #1  0x0000000006bedd6b in dd::Weak_object_impl_<true>::store (this=0x7f4f2c03d060, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:129
 * #2  0x00000000066952c1 in dd::Collection<dd::Column*>::store_items (this=0x7f4f2c0209d0, otx=0x7f4fb04f1710) at /sql/dd/collection.cc:218
 * #3  0x0000000006b3a32e in dd::Abstract_table_impl::store_children (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/abstract_table_impl.cc:128
 * #4  0x0000000006bb4b46 in dd::Table_impl::store_children (this=0x7f4f2c020930, otx=0x7f4fb04f1710) at /sql/dd/impl/types/table_impl.cc:365
 * #5  0x0000000006bedf1d in dd::Weak_object_impl_<true>::store (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:152
 * #6  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #7  0x000000000681de25 in dd::cache::Dictionary_client::store<dd::Table> (this=0x7f4f3c01d9f0, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #8  0x00000000042bca3c in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1096
 * #9  0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
 * #10 0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #11 0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #12 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #13 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #14 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #15 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #16 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
bool Raw_new_record::insert() {
  DBUG_TRACE;

  // Note:使用存储引擎接口实现插入
  int rc = m_table->file->ha_write_row(m_table->record[0]);

  if (rc) {
    m_table->file->print_error(rc, MYF(0));
    return true;
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////

Object_id Raw_new_record::get_insert_id() const {
  Object_id id = m_table->file->insert_id_for_cur_row;

  // Objects without primary key should have still get INVALID_OBJECT_ID.
  return id ? id : dd::INVALID_OBJECT_ID;
}

///////////////////////////////////////////////////////////////////////////

void Raw_new_record::finalize() {
  if (!m_table) return;

  m_table->autoinc_field_has_explicit_non_null_value = false;
  m_table->file->ha_release_auto_increment();
  m_table->next_number_field = nullptr;

  m_table = nullptr;
}

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
