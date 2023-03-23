/* Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.

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

#include "sql/dd/impl/cache/shared_dictionary_cache.h"

#include <atomic>

#include "my_dbug.h"
#include "sql/dd/impl/cache/shared_multi_map.h"
#include "sql/dd/impl/cache/storage_adapter.h"  // Storage_adapter
#include "sql/mysqld.h"
#include "sql/sql_class.h"  // THD::is_error()

namespace dd {
namespace cache {

template <typename T>
class Cache_element;

/** Note:构造全局缓存实例
 * 单实例
*/
Shared_dictionary_cache *Shared_dictionary_cache::instance() {
  static Shared_dictionary_cache s_cache;
  return &s_cache;
}

/** Note:全局共享缓存初始化
 * 设置每一个缓存的对象Object的大小
*/
void Shared_dictionary_cache::init() {
  instance()->m_map<Collation>()->set_capacity(collation_capacity);
  instance()->m_map<Charset>()->set_capacity(charset_capacity);

  // Set capacity to have room for all connections to leave an element
  // unused in the cache to avoid frequent cache misses while e.g.
  // opening a table.
  instance()->m_map<Abstract_table>()->set_capacity(max_connections);
  instance()->m_map<Event>()->set_capacity(event_capacity);
  instance()->m_map<Routine>()->set_capacity(stored_program_def_size);
  instance()->m_map<Schema>()->set_capacity(schema_def_size);
  instance()->m_map<Column_statistics>()->set_capacity(
      column_statistics_capacity);
  instance()->m_map<Spatial_reference_system>()->set_capacity(
      spatial_reference_system_capacity);
  instance()->m_map<Tablespace>()->set_capacity(tablespace_def_size);
  instance()->m_map<Resource_group>()->set_capacity(resource_group_capacity);
}

/** Note:关闭全局共享缓存
 * 清理每个对象的缓存
*/
void Shared_dictionary_cache::shutdown() {
  instance()->m_map<Abstract_table>()->shutdown();
  instance()->m_map<Collation>()->shutdown();
  instance()->m_map<Column_statistics>()->shutdown();
  instance()->m_map<Charset>()->shutdown();
  instance()->m_map<Event>()->shutdown();
  instance()->m_map<Routine>()->shutdown();
  instance()->m_map<Schema>()->shutdown();
  instance()->m_map<Spatial_reference_system>()->shutdown();
  instance()->m_map<Tablespace>()->shutdown();
  instance()->m_map<Resource_group>()->shutdown();
}

// Don't call this function anywhere except upgrade scenario.
void Shared_dictionary_cache::reset(bool keep_dd_entities) {
  shutdown();
  if (!keep_dd_entities) Storage_adapter::instance()->erase_all();
  init();
}

// Workaround to be used during recovery at server restart.
bool Shared_dictionary_cache::reset_tables_and_tablespaces(THD *thd) {
  return (instance()->m_map<Abstract_table>()->reset(thd) ||
          instance()->m_map<Tablespace>()->reset(thd));
}

// Get an element from the cache, given the key.
/** Note:外部接口
 * 通过key查找(Shared_multi_map->get()共享内存中获取),如果找到则返回
 * 如果未找到则调用get_uncached从持久化存储(innodb表)读取,然后将找到的结果写回缓存(Shared_multi_map->put())
 * 
 * 调用:
 * #1  0x0000000006a004a4 in dd::cache::Shared_dictionary_cache::get<dd::Item_name_key, dd::Abstract_table> (
    this=0xf14ce20 <dd::cache::Shared_dictionary_cache::instance()::s_cache>, thd=0x7f4f3c019f10, key=..., element=0x7f4fb04f27c8)
    at /sql/dd/impl/cache/shared_dictionary_cache.cc:113
 * #2  0x0000000006845d3c in dd::cache::Dictionary_client::acquire<dd::Item_name_key, dd::Abstract_table> (this=0x7f4f3c01d9f0, key=..., object=0x7f4fb04f2838, 
    local_committed=0x7f4fb04f2837, local_uncommitted=0x7f4fb04f2836) at /sql/dd/impl/cache/dictionary_client.cc:910
 * #3  0x0000000006809b66 in dd::cache::Dictionary_client::acquire<dd::Abstract_table> (this=0x7f4f3c01d9f0, schema_name=..., object_name=..., object=0x7f4fb04f2958)
    at /sql/dd/impl/cache/dictionary_client.cc:1379
 * #4  0x000000000673314e in dd::table_exists (client=0x7f4f3c01d9f0, schema_name=0x7f4f2c027a28 "test", name=0x7f4f2c026da8 "tx1", exists=0x7f4fb04f2caf)
    at /sql/dd/dd_table.cc:2468
 * #5  0x0000000003f79e53 in check_if_table_exists (thd=0x7f4f3c019f10, table=0x7f4f2c0273e8, exists=0x7f4fb04f2caf) at /sql/sql_base.cc:2506
 * #6  0x0000000003f7c19a in open_table (thd=0x7f4f3c019f10, table_list=0x7f4f2c0273e8, ot_ctx=0x7f4fb04f2f30) at /sql/sql_base.cc:3108
 * #7  0x0000000003f85ce8 in open_and_process_table (thd=0x7f4f3c019f10, lex=0x7f4f3c01d130, tables=0x7f4f2c0273e8, counter=0x7f4fb04f3168, 
    prelocking_strategy=0x7f4fb04f3018, has_prelocking_list=false, ot_ctx=0x7f4fb04f2f30) at /sql/sql_base.cc:5085
 * #8 0x0000000003f892cd in open_tables (thd=0x7f4f3c019f10, start=0x7f4f3c01d140, counter=0x7f4fb04f3168, flags=0, prelocking_strategy=0x7f4fb04f3018)
    at /sql/sql_base.cc:5895
 * #9 0x0000000003fa4f17 in open_tables (thd=0x7f4f3c019f10, tables=0x7f4f3c01d140, counter=0x7f4fb04f3168, flags=0) at /sql/sql_base.h:462
 * #10 0x00000000042ee5c4 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:9970
 * #11 0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #12 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #13 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #14 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #15 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
template <typename K, typename T>
bool Shared_dictionary_cache::get(THD *thd, const K &key,
                                  Cache_element<T> **element) {
  bool error = false;
  DBUG_ASSERT(element);
  // Note:共享内存获取
  if (m_map<T>()->get(key, element)) {
    // Handle cache miss.
    const T *new_object = nullptr;
    // Note:cache miss,则到磁盘获取
    error = get_uncached(thd, key, ISO_READ_COMMITTED, &new_object);

    // Add the new object, and assign the output element, even in the case of
    // a miss error (needed to remove the missed key).
    // Note:缓存没有命中,到InnoDB查找,同时将这个对象加入到缓存中
    m_map<T>()->put(&key, new_object, element);
  }
  return error;
}

// Read an object directly from disk, given the key.
/** Note:外部接口
 * 对于在全局缓存中不存在的对象直接调用Storage_adapter::get从InnoDB表加载数据
 * 直接从innodb表读取object对象，并且设置一个key
*/
template <typename K, typename T>
bool Shared_dictionary_cache::get_uncached(THD *thd, const K &key,
                                           enum_tx_isolation isolation,
                                           const T **object) const {
  DBUG_ASSERT(object);
  // Note:调用Storage_adapter接口从磁盘的innodb表获取数据
  bool error = Storage_adapter::get(thd, key, isolation, false, object);
  DBUG_ASSERT(!error || thd->is_system_thread() || thd->killed ||
              thd->is_error());

  return error;
}

// Add an object to the shared cache.
/** Note:外部接口
 * 将element_cache放入相应的map,如果map中已经存在该element_cache,
 * 返回这个element_cache的引用,element_cache的引用计数加1
*/
template <typename T>
void Shared_dictionary_cache::put(const T *object, Cache_element<T> **element) {
  DBUG_ASSERT(object);
  // Cast needed to help the compiler choose the correct template instance..
  m_map<T>()->put(static_cast<const typename T::Id_key *>(nullptr), object,
                  element);
}

// Explicitly instantiate the types for the various usages.
template bool
Shared_dictionary_cache::get<Abstract_table::Id_key, Abstract_table>(
    THD *thd, const Abstract_table::Id_key &, Cache_element<Abstract_table> **);
template bool
Shared_dictionary_cache::get<Abstract_table::Name_key, Abstract_table>(
    THD *thd, const Abstract_table::Name_key &,
    Cache_element<Abstract_table> **);
template bool
Shared_dictionary_cache::get<Abstract_table::Aux_key, Abstract_table>(
    THD *thd, const Abstract_table::Aux_key &,
    Cache_element<Abstract_table> **);
template bool
Shared_dictionary_cache::get_uncached<Abstract_table::Id_key, Abstract_table>(
    THD *thd, const Abstract_table::Id_key &, enum_tx_isolation,
    const Abstract_table **) const;
template bool
Shared_dictionary_cache::get_uncached<Abstract_table::Name_key, Abstract_table>(
    THD *thd, const Abstract_table::Name_key &, enum_tx_isolation,
    const Abstract_table **) const;
template bool
Shared_dictionary_cache::get_uncached<Abstract_table::Aux_key, Abstract_table>(
    THD *thd, const Abstract_table::Aux_key &, enum_tx_isolation,
    const Abstract_table **) const;
template void Shared_dictionary_cache::put<Abstract_table>(
    const Abstract_table *, Cache_element<Abstract_table> **);

template bool Shared_dictionary_cache::get<Charset::Id_key, Charset>(
    THD *thd, const Charset::Id_key &, Cache_element<Charset> **);
template bool Shared_dictionary_cache::get<Charset::Name_key, Charset>(
    THD *thd, const Charset::Name_key &, Cache_element<Charset> **);
template bool Shared_dictionary_cache::get<Charset::Aux_key, Charset>(
    THD *thd, const Charset::Aux_key &, Cache_element<Charset> **);
template bool Shared_dictionary_cache::get_uncached<Charset::Id_key, Charset>(
    THD *thd, const Charset::Id_key &, enum_tx_isolation,
    const Charset **) const;
template bool Shared_dictionary_cache::get_uncached<Charset::Name_key, Charset>(
    THD *thd, const Charset::Name_key &, enum_tx_isolation,
    const Charset **) const;
template bool Shared_dictionary_cache::get_uncached<Charset::Aux_key, Charset>(
    THD *thd, const Charset::Aux_key &, enum_tx_isolation,
    const Charset **) const;
template void Shared_dictionary_cache::put<Charset>(const Charset *,
                                                    Cache_element<Charset> **);

template bool Shared_dictionary_cache::get<Collation::Id_key, Collation>(
    THD *thd, const Collation::Id_key &, Cache_element<Collation> **);
template bool Shared_dictionary_cache::get<Collation::Name_key, Collation>(
    THD *thd, const Collation::Name_key &, Cache_element<Collation> **);
template bool Shared_dictionary_cache::get<Collation::Aux_key, Collation>(
    THD *thd, const Collation::Aux_key &, Cache_element<Collation> **);
template bool Shared_dictionary_cache::get_uncached<
    Collation::Id_key, Collation>(THD *thd, const Collation::Id_key &,
                                  enum_tx_isolation, const Collation **) const;
template bool
Shared_dictionary_cache::get_uncached<Collation::Name_key, Collation>(
    THD *thd, const Collation::Name_key &, enum_tx_isolation,
    const Collation **) const;
template bool Shared_dictionary_cache::get_uncached<
    Collation::Aux_key, Collation>(THD *thd, const Collation::Aux_key &,
                                   enum_tx_isolation, const Collation **) const;
template void Shared_dictionary_cache::put<Collation>(
    const Collation *, Cache_element<Collation> **);

template bool Shared_dictionary_cache::get<Event::Id_key, Event>(
    THD *thd, const Event::Id_key &, Cache_element<Event> **);
template bool Shared_dictionary_cache::get<Event::Name_key, Event>(
    THD *thd, const Event::Name_key &, Cache_element<Event> **);
template bool Shared_dictionary_cache::get<Event::Aux_key, Event>(
    THD *thd, const Event::Aux_key &, Cache_element<Event> **);
template bool Shared_dictionary_cache::get_uncached<Event::Id_key, Event>(
    THD *thd, const Event::Id_key &, enum_tx_isolation, const Event **) const;
template bool Shared_dictionary_cache::get_uncached<Event::Name_key, Event>(
    THD *thd, const Event::Name_key &, enum_tx_isolation, const Event **) const;
template bool Shared_dictionary_cache::get_uncached<Event::Aux_key, Event>(
    THD *thd, const Event::Aux_key &, enum_tx_isolation, const Event **) const;
template void Shared_dictionary_cache::put<Event>(const Event *,
                                                  Cache_element<Event> **);

template bool Shared_dictionary_cache::get<Routine::Id_key, Routine>(
    THD *thd, const Routine::Id_key &, Cache_element<Routine> **);
template bool Shared_dictionary_cache::get<Routine::Name_key, Routine>(
    THD *thd, const Routine::Name_key &, Cache_element<Routine> **);
template bool Shared_dictionary_cache::get<Routine::Aux_key, Routine>(
    THD *thd, const Routine::Aux_key &, Cache_element<Routine> **);
template bool Shared_dictionary_cache::get_uncached<Routine::Id_key, Routine>(
    THD *thd, const Routine::Id_key &, enum_tx_isolation,
    const Routine **) const;
template bool Shared_dictionary_cache::get_uncached<Routine::Name_key, Routine>(
    THD *thd, const Routine::Name_key &, enum_tx_isolation,
    const Routine **) const;
template bool Shared_dictionary_cache::get_uncached<Routine::Aux_key, Routine>(
    THD *thd, const Routine::Aux_key &, enum_tx_isolation,
    const Routine **) const;
template void Shared_dictionary_cache::put<Routine>(const Routine *,
                                                    Cache_element<Routine> **);

template bool Shared_dictionary_cache::get<Schema::Id_key, Schema>(
    THD *thd, const Schema::Id_key &, Cache_element<Schema> **);
template bool Shared_dictionary_cache::get<Schema::Name_key, Schema>(
    THD *thd, const Schema::Name_key &, Cache_element<Schema> **);
template bool Shared_dictionary_cache::get<Schema::Aux_key, Schema>(
    THD *thd, const Schema::Aux_key &, Cache_element<Schema> **);
template bool Shared_dictionary_cache::get_uncached<Schema::Id_key, Schema>(
    THD *thd, const Schema::Id_key &, enum_tx_isolation, const Schema **) const;
template bool Shared_dictionary_cache::get_uncached<Schema::Name_key, Schema>(
    THD *thd, const Schema::Name_key &, enum_tx_isolation,
    const Schema **) const;
template bool Shared_dictionary_cache::get_uncached<Schema::Aux_key, Schema>(
    THD *thd, const Schema::Aux_key &, enum_tx_isolation,
    const Schema **) const;
template void Shared_dictionary_cache::put<Schema>(const Schema *,
                                                   Cache_element<Schema> **);

template bool Shared_dictionary_cache::get<Spatial_reference_system::Id_key,
                                           Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Id_key &,
    Cache_element<Spatial_reference_system> **);
template bool Shared_dictionary_cache::get<Spatial_reference_system::Name_key,
                                           Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Name_key &,
    Cache_element<Spatial_reference_system> **);
template bool Shared_dictionary_cache::get<Spatial_reference_system::Aux_key,
                                           Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Aux_key &,
    Cache_element<Spatial_reference_system> **);
template bool Shared_dictionary_cache::get_uncached<
    Spatial_reference_system::Id_key, Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Id_key &, enum_tx_isolation,
    const Spatial_reference_system **) const;
template bool Shared_dictionary_cache::get_uncached<
    Spatial_reference_system::Name_key, Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Name_key &, enum_tx_isolation,
    const Spatial_reference_system **) const;
template bool Shared_dictionary_cache::get_uncached<
    Spatial_reference_system::Aux_key, Spatial_reference_system>(
    THD *thd, const Spatial_reference_system::Aux_key &, enum_tx_isolation,
    const Spatial_reference_system **) const;
template void Shared_dictionary_cache::put<Spatial_reference_system>(
    const Spatial_reference_system *,
    Cache_element<Spatial_reference_system> **);

template bool
Shared_dictionary_cache::get<Column_statistics::Id_key, Column_statistics>(
    THD *thd, const Column_statistics::Id_key &,
    Cache_element<Column_statistics> **);
template bool
Shared_dictionary_cache::get<Column_statistics::Name_key, Column_statistics>(
    THD *thd, const Column_statistics::Name_key &,
    Cache_element<Column_statistics> **);
template bool
Shared_dictionary_cache::get<Column_statistics::Aux_key, Column_statistics>(
    THD *thd, const Column_statistics::Aux_key &,
    Cache_element<Column_statistics> **);
template bool Shared_dictionary_cache::get_uncached<Column_statistics::Id_key,
                                                    Column_statistics>(
    THD *thd, const Column_statistics::Id_key &, enum_tx_isolation,
    const Column_statistics **) const;
template bool Shared_dictionary_cache::get_uncached<Column_statistics::Name_key,
                                                    Column_statistics>(
    THD *thd, const Column_statistics::Name_key &, enum_tx_isolation,
    const Column_statistics **) const;
template bool Shared_dictionary_cache::get_uncached<Column_statistics::Aux_key,
                                                    Column_statistics>(
    THD *thd, const Column_statistics::Aux_key &, enum_tx_isolation,
    const Column_statistics **) const;
template void Shared_dictionary_cache::put<Column_statistics>(
    const Column_statistics *, Cache_element<Column_statistics> **);

template bool Shared_dictionary_cache::get<Tablespace::Id_key, Tablespace>(
    THD *thd, const Tablespace::Id_key &, Cache_element<Tablespace> **);
template bool Shared_dictionary_cache::get<Tablespace::Name_key, Tablespace>(
    THD *thd, const Tablespace::Name_key &, Cache_element<Tablespace> **);
template bool Shared_dictionary_cache::get<Tablespace::Aux_key, Tablespace>(
    THD *thd, const Tablespace::Aux_key &, Cache_element<Tablespace> **);
template bool
Shared_dictionary_cache::get_uncached<Tablespace::Id_key, Tablespace>(
    THD *thd, const Tablespace::Id_key &, enum_tx_isolation,
    const Tablespace **) const;
template bool
Shared_dictionary_cache::get_uncached<Tablespace::Name_key, Tablespace>(
    THD *thd, const Tablespace::Name_key &, enum_tx_isolation,
    const Tablespace **) const;
template bool
Shared_dictionary_cache::get_uncached<Tablespace::Aux_key, Tablespace>(
    THD *thd, const Tablespace::Aux_key &, enum_tx_isolation,
    const Tablespace **) const;
template void Shared_dictionary_cache::put<Tablespace>(
    const Tablespace *, Cache_element<Tablespace> **);

template bool
Shared_dictionary_cache::get<Resource_group::Id_key, Resource_group>(
    THD *thd, const Resource_group::Id_key &, Cache_element<Resource_group> **);
template bool
Shared_dictionary_cache::get<Resource_group::Name_key, Resource_group>(
    THD *thd, const Resource_group::Name_key &,
    Cache_element<Resource_group> **);
template bool
Shared_dictionary_cache::get<Resource_group::Aux_key, Resource_group>(
    THD *thd, const Resource_group::Aux_key &,
    Cache_element<Resource_group> **);
template bool
Shared_dictionary_cache::get_uncached<Resource_group::Id_key, Resource_group>(
    THD *thd, const Resource_group::Id_key &, enum_tx_isolation,
    const Resource_group **) const;
template bool
Shared_dictionary_cache::get_uncached<Resource_group::Name_key, Resource_group>(
    THD *thd, const Resource_group::Name_key &, enum_tx_isolation,
    const Resource_group **) const;
template bool
Shared_dictionary_cache::get_uncached<Resource_group::Aux_key, Resource_group>(
    THD *thd, const Resource_group::Aux_key &, enum_tx_isolation,
    const Resource_group **) const;
template void Shared_dictionary_cache::put<Resource_group>(
    const Resource_group *, Cache_element<Resource_group> **);
}  // namespace cache
}  // namespace dd
