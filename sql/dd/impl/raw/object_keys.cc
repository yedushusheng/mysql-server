/* Copyright (c) 2014, 2020, Oracle and/or its affiliates.

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

#include "sql/dd/impl/raw/object_keys.h"

#include <new>
#include <sstream>

#include "m_ctype.h"
#include "my_base.h"  // HA_WHOLE_KEY
#include "my_dbug.h"
#include "mysql/udf_registration_types.h"
#include "sql/dd/impl/raw/raw_key.h"              // dd::Raw_key
#include "sql/dd/impl/raw/raw_table.h"            // dd::Raw_table
#include "sql/dd/impl/types/object_table_impl.h"  // dd::Object_table_impl
#include "sql/dd/string_type.h"                   // dd::String_type
#include "sql/field.h"                            // Field
#include "sql/key.h"                              // KEY
#include "sql/table.h"                            // TABLE

namespace dd {

///////////////////////////////////////////////////////////////////////////
// Primary_id_key
///////////////////////////////////////////////////////////////////////////
// Note:主键
Raw_key *Primary_id_key::create_access_key(Raw_table *db_table) const {
  // Positional index of PK-Index on object-id field.
  // It is 0 for any DD-table (PK-Index is the 1st index on a DD-table).
  const int ID_INDEX_NO =
      static_cast<int>(Object_table_impl::Common_index::PK_ID);
  DBUG_ASSERT(ID_INDEX_NO == 0);

  // Positional index of PK-object-id-column.
  // It is 0 for any DD-table (object-id is the 1st column on a DD-table).
  const int ID_COLUMN_NO =
      static_cast<int>(Object_table_impl::Common_field::ID);
  DBUG_ASSERT(ID_COLUMN_NO == 0);

  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[ID_COLUMN_NO]->store(m_object_id, true);

  KEY *key_info = t->key_info + ID_INDEX_NO;

  Raw_key *k = new (std::nothrow)
      Raw_key(ID_INDEX_NO, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

/* purecov: begin inspected */
String_type Primary_id_key::str() const {
  dd::Stringstream_type ss;
  ss << m_object_id;
  return ss.str();
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////
// Parent_id_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Parent_id_range_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_id_column_no]->store(m_object_id, true);
  t->field[m_id_column_no]->set_notnull();

  KEY *key_info = t->key_info + m_id_index_no;

  Raw_key *k = new (std::nothrow)
      Raw_key(m_id_index_no, key_info->key_length, 1 /* Use 1st column */);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

/* purecov: begin inspected */
String_type Parent_id_range_key::str() const {
  // XXX: not needed
  dd::Stringstream_type ss;
  ss << m_id_column_no << ":" << m_object_id;
  return ss.str();
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////
// Global_name_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Global_name_key::create_access_key(Raw_table *db_table) const {
  /*
    Positional index of name index on the name field.
    It is 1 for any DD-table (the name index is the 2nd index
    on a DD-table, i.e., the ordinal position is 2). This is a
    convention both for entities with global names, and entities
    that are items contained in another entity.
  */
  const int NAME_INDEX_NO =
      static_cast<int>(Object_table_impl::Common_index::UK_NAME);

  DBUG_ASSERT(NAME_INDEX_NO == 1);

  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_name_column_no]->store(m_object_name.c_str(),
                                    m_object_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + NAME_INDEX_NO;

  Raw_key *k = new (std::nothrow)
      Raw_key(NAME_INDEX_NO, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////
// Item_name_key
///////////////////////////////////////////////////////////////////////////
/** Note:外部接口
 * 在raw_record相关操作(查找)中调用,这里是设置访问Object的key信息
 * 
 * 调用:
 * #0  dd::Item_name_key::create_access_key (this=0x7f4fb04f2660, db_table=0x7f4f2c0a7430) at /sql/dd/impl/raw/object_keys.cc:168
 * #1  0x0000000006a45243 in dd::Raw_table::find_record (this=0x7f4f2c0a7430, key=..., r=...) at /sql/dd/impl/raw/raw_table.cc:69
 * #2  0x0000000006a0deee in dd::cache::Storage_adapter::get<dd::Item_name_key, dd::Abstract_table> (thd=0x7f4f3c019f10, key=..., isolation=ISO_READ_COMMITTED, 
    bypass_core_registry=false, object=0x7f4fb04f2560) at /sql/dd/impl/cache/storage_adapter.cc:181
 * #3  0x0000000006a00a70 in dd::cache::Shared_dictionary_cache::get_uncached<dd::Item_name_key, dd::Abstract_table> (
    this=0xf14ce20 <dd::cache::Shared_dictionary_cache::instance()::s_cache>, thd=0x7f4f3c019f10, key=..., isolation=ISO_READ_COMMITTED, object=0x7f4fb04f2560)
    at /sql/dd/impl/cache/shared_dictionary_cache.cc:128
 * #4  0x0000000006a004a4 in dd::cache::Shared_dictionary_cache::get<dd::Item_name_key, dd::Abstract_table> (
    this=0xf14ce20 <dd::cache::Shared_dictionary_cache::instance()::s_cache>, thd=0x7f4f3c019f10, key=..., element=0x7f4fb04f25d8)
    at /sql/dd/impl/cache/shared_dictionary_cache.cc:113
 * #5  0x0000000006845d3c in dd::cache::Dictionary_client::acquire<dd::Item_name_key, dd::Abstract_table> (this=0x7f4f3c01d9f0, key=..., object=0x7f4fb04f2648, 
    local_committed=0x7f4fb04f2647, local_uncommitted=0x7f4fb04f2646) at /sql/dd/impl/cache/dictionary_client.cc:910
 * #6  0x0000000006809b66 in dd::cache::Dictionary_client::acquire<dd::Abstract_table> (this=0x7f4f3c01d9f0, schema_name=..., object_name=..., object=0x7f4fb04f2778)
    at /sql/dd/impl/cache/dictionary_client.cc:1379
 * #7  0x00000000042e3950 in check_if_table_exists (thd=0x7f4f3c019f10, schema_name=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", alias=0x7f4f2c026da8 "tx1", 
    ha_lex_create_tmp_table=false, ha_create_if_not_exists=false, internal_tmp_table=false) at /sql/sql_table.cc:8533
 * #8  0x00000000042e5399 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8823
 * #9  0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #10 0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #11 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #12 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #13 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #14 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #15 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
Raw_key *Item_name_key::create_access_key(Raw_table *db_table) const {
  /*
    Positional index of name index on the name field.
    It is 1 for any DD-table (the name index is the 2nd index
    on a DD-table, i.e., the ordinal position is 2). This is a
    convention both for entities with global names, and entities
    that are items contained in another entity.
  */
  const int NAME_INDEX_NO =
      static_cast<int>(Object_table_impl::Common_index::UK_NAME);

  DBUG_ASSERT(NAME_INDEX_NO == 1);

  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_container_id_column_no]->store(m_container_id, true);

  t->field[m_name_column_no]->store(m_object_name.c_str(),
                                    m_object_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + NAME_INDEX_NO;

  Raw_key *k = new (std::nothrow)
      Raw_key(NAME_INDEX_NO, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Item_name_key::str() const {
  dd::Stringstream_type ss;
  ss << m_container_id << ":" << m_object_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Se_private_id_key
///////////////////////////////////////////////////////////////////////////

/* purecov: begin deadcode */
Raw_key *Se_private_id_key::create_access_key(Raw_table *db_table) const {
  key_part_map keypart_map = 1;
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_engine_column_no]->store(m_engine.c_str(), m_engine.length(),
                                      &my_charset_bin);
  t->field[m_engine_column_no]->set_notnull();

  t->field[m_private_id_column_no]->store(m_private_id, true);
  t->field[m_private_id_column_no]->set_notnull();

  if (m_private_id != INVALID_OBJECT_ID) keypart_map = HA_WHOLE_KEY;

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k =
      new (std::nothrow) Raw_key(m_index_no, key_info->key_length, keypart_map);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////

String_type Se_private_id_key::str() const {
  dd::Stringstream_type ss;
  ss << m_engine << ":" << m_private_id;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Composite_pk
///////////////////////////////////////////////////////////////////////////

Raw_key *Composite_pk::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_first_column_no]->store(m_first_id, true);
  t->field[m_second_column_no]->store(m_second_id, true);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new (std::nothrow)
      Raw_key(m_index_no, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

/* purecov: begin inspected */
String_type Composite_pk::str() const {
  dd::Stringstream_type ss;
  ss << m_first_id << ":" << m_second_id;
  return ss.str();
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////
// Routine_name_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Routine_name_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_container_id_column_no]->store(m_container_id, true);

  t->field[m_type_column_no]->store(m_type, true);

  t->field[m_name_column_no]->store(m_object_name.c_str(),
                                    m_object_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new (std::nothrow)
      Raw_key(m_index_no, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

/* purecov: begin inspected */
String_type Routine_name_key::str() const {
  dd::Stringstream_type ss;
  ss << m_container_id << ":" << m_type << ":" << m_object_name;
  return ss.str();
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////

bool Routine_name_key::operator<(const Routine_name_key &rhs) const {
  if (m_container_id != rhs.m_container_id)
    return m_container_id < rhs.m_container_id;
  if (m_type != rhs.m_type) return m_type < rhs.m_type;

  return (my_strnncoll(m_cs, pointer_cast<const uchar *>(m_object_name.c_str()),
                       m_object_name.length(),
                       pointer_cast<const uchar *>(rhs.m_object_name.c_str()),
                       rhs.m_object_name.length()) < 0);
}

///////////////////////////////////////////////////////////////////////////
// Composite_char_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Composite_char_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();  // TODO can mark only required fields ?

  t->field[m_first_column_no]->store(m_first_name.c_str(),
                                     m_first_name.length(), &my_charset_bin);

  t->field[m_second_column_no]->store(m_second_name.c_str(),
                                      m_second_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new Raw_key(m_index_no, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Composite_char_key::str() const {
  dd::Stringstream_type ss;
  ss << m_first_name << ":" << m_second_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Composite_4char_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Composite_4char_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();  // TODO can mark only required fields ?

  t->field[m_first_column_no]->store(m_first_name.c_str(),
                                     m_first_name.length(), &my_charset_bin);

  t->field[m_second_column_no]->store(m_second_name.c_str(),
                                      m_second_name.length(), &my_charset_bin);

  t->field[m_third_column_no]->store(m_third_name.c_str(),
                                     m_third_name.length(), &my_charset_bin);

  t->field[m_fourth_column_no]->store(m_fourth_name.c_str(),
                                      m_fourth_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new Raw_key(m_index_no, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Composite_4char_key::str() const {
  dd::Stringstream_type ss;
  ss << m_first_name << ":" << m_second_name << ":" << m_third_name << ":"
     << m_fourth_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Composite_obj_id_3char_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Composite_obj_id_3char_key::create_access_key(
    Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();  // TODO can mark only required fields ?

  t->field[m_id_column_no]->store(m_id, true);

  t->field[m_first_column_no]->store(m_first_name.c_str(),
                                     m_first_name.length(), &my_charset_bin);

  t->field[m_second_column_no]->store(m_second_name.c_str(),
                                      m_second_name.length(), &my_charset_bin);

  t->field[m_third_column_no]->store(m_third_name.c_str(),
                                     m_third_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new Raw_key(m_index_no, key_info->key_length, HA_WHOLE_KEY);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Composite_obj_id_3char_key::str() const {
  dd::Stringstream_type ss;
  ss << m_id << m_first_name << ":" << m_second_name << ":" << m_third_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Index_stat_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Index_stat_range_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_schema_name_column_no]->store(
      m_schema_name.c_str(), m_schema_name.length(), &my_charset_bin);
  t->field[m_table_name_column_no]->store(
      m_table_name.c_str(), m_table_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new (std::nothrow)
      Raw_key(m_index_no, key_info->key_length, 3 /* Use first two column */);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Index_stat_range_key::str() const {
  dd::Stringstream_type ss;
  ss << m_schema_name_column_no << ":" << m_schema_name << ":"
     << m_table_name_column_no << ":" << m_table_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Table_reference_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Table_reference_range_key::create_access_key(
    Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_catalog_name_column_no]->store(
      m_catalog_name.c_str(), m_catalog_name.length(), &my_charset_bin);
  t->field[m_schema_name_column_no]->store(
      m_schema_name.c_str(), m_schema_name.length(), &my_charset_bin);
  t->field[m_table_name_column_no]->store(
      m_table_name.c_str(), m_table_name.length(), &my_charset_bin);

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new (std::nothrow) Raw_key(m_index_no, key_info->key_length, 7);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Table_reference_range_key::str() const {
  dd::Stringstream_type ss;
  ss << m_catalog_name_column_no << ":" << m_catalog_name
     << m_schema_name_column_no << ":" << m_schema_name << ":"
     << m_table_name_column_no << ":" << m_table_name;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Sub_partition_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Sub_partition_range_key::create_access_key(Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_table_id_column_no]->store(m_table_id, true);
  t->field[m_table_id_column_no]->set_notnull();

  if (m_parent_partition_id == dd::INVALID_OBJECT_ID)
    t->field[m_parent_partition_id_column_no]->set_null();
  else {
    t->field[m_parent_partition_id_column_no]->store(m_parent_partition_id,
                                                     true);
    t->field[m_parent_partition_id_column_no]->set_notnull();
  }

  KEY *key_info = t->key_info + m_index_no;

  Raw_key *k = new (std::nothrow)
      Raw_key(m_index_no, key_info->key_length, 3 /* Use first two column */);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Sub_partition_range_key::str() const {
  dd::Stringstream_type ss;
  ss << m_parent_partition_id_column_no << ":" << m_parent_partition_id << ":"
     << m_table_id_column_no << ":" << m_table_id;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
// Definer_reference_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *Definer_reference_range_key::create_access_key(
    Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_definer_column_no]->store(m_definer.c_str(), m_definer.length(),
                                       &my_charset_bin);
  t->field[m_definer_column_no]->set_notnull();

  KEY *key_info = t->key_info + m_index_no;

  // Use one column of the key.
  // TODO: Investigate why HA_WHOLE_KEY does not give the expected result.
  Raw_key *k = new (std::nothrow) Raw_key(m_index_no, key_info->key_length, 1);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type Definer_reference_range_key::str() const { return m_definer; }

///////////////////////////////////////////////////////////////////////////
// View_definer_reference_range_key
///////////////////////////////////////////////////////////////////////////

Raw_key *View_definer_reference_range_key::create_access_key(
    Raw_table *db_table) const {
  TABLE *t = db_table->get_table();

  t->use_all_columns();

  t->field[m_table_type_column_no]->store(m_table_type, true);
  t->field[m_definer_column_no]->store(m_definer.c_str(), m_definer.length(),
                                       &my_charset_bin);
  t->field[m_definer_column_no]->set_notnull();

  KEY *key_info = t->key_info + m_index_no;

  // Use two columns of the key.
  // TODO: Investigate why HA_WHOLE_KEY does not give the expected result.
  Raw_key *k = new (std::nothrow) Raw_key(m_index_no, key_info->key_length, 3);

  key_copy(k->key, t->record[0], key_info, k->key_len);

  return k;
}

///////////////////////////////////////////////////////////////////////////

String_type View_definer_reference_range_key::str() const {
  dd::Stringstream_type ss;
  ss << m_table_type << ":" << m_definer;
  return ss.str();
}

///////////////////////////////////////////////////////////////////////////
}  // namespace dd
