/* Copyright (c) 2015, 2020, Oracle and/or its affiliates.

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

#include "sql/dd/impl/cache/storage_adapter.h"

#include <memory>
#include <string>

#include "mutex_lock.h"  // Mutex_lock
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/dd/cache/dictionary_client.h"       // Dictionary_client
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"  // bootstrap::DD_bootstrap_ctx
#include "sql/dd/impl/cache/cache_element.h"
#include "sql/dd/impl/raw/object_keys.h"           // Primary_id_key
#include "sql/dd/impl/raw/raw_record.h"            // Raw_record
#include "sql/dd/impl/raw/raw_table.h"             // Raw_table
#include "sql/dd/impl/sdi.h"                       // sdi::store() sdi::drop()
#include "sql/dd/impl/tables/basic_column_statistics.h"   // dd::tables::Basic_column_statistics
#include "sql/dd/impl/tables/character_sets.h"     // dd::tables::Character_sets
#include "sql/dd/impl/tables/collations.h"         // dd::tables::Collations
#include "sql/dd/impl/tables/column_statistics.h"  // dd::tables::Column_stat...
#include "sql/dd/impl/tables/events.h"             // dd::tables::Events
#include "sql/dd/impl/tables/index_stats.h"        // dd::tables::Index_stats
#include "sql/dd/impl/tables/resource_groups.h"  // dd::tables::Resource_groups
#include "sql/dd/impl/tables/routines.h"         // dd::tables::Routines
#include "sql/dd/impl/tables/schemata.h"         // dd::tables::Schemata
#include "sql/dd/impl/tables/spatial_reference_systems.h"  // dd::tables::Spatial...
#include "sql/dd/impl/tables/table_stats.h"  // dd::tables::Table_stats
#include "sql/dd/impl/tables/tables.h"       // dd::tables::Tables
#include "sql/dd/impl/tables/tablespaces.h"  // dd::tables::Tablespaces
#include "sql/dd/impl/transaction_impl.h"    // Transaction_ro
#include "sql/dd/impl/types/entity_object_impl.h"
#include "sql/dd/impl/tables/statistics_collector_jobs.h"  // dd::Statistics_collector_jobs
#include "sql/dd/types/abstract_table.h"            // Abstract_table
#include "sql/dd/types/charset.h"                   // Charset
#include "sql/dd/types/collation.h"                 // Collation
#include "sql/dd/types/column_statistics.h"         // Column_statistics
#include "sql/dd/types/entity_object_table.h"       // Entity_object_table
#include "sql/dd/types/event.h"                     // Event
#include "sql/dd/types/function.h"                  // Routine, Function
#include "sql/dd/types/index_stat.h"                // Index_stat
#include "sql/dd/types/procedure.h"                 // Procedure
#include "sql/dd/types/schema.h"                    // Schema
#include "sql/dd/types/spatial_reference_system.h"  // Spatial_reference_system
#include "sql/dd/types/table.h"                     // Table
#include "sql/dd/types/table_stat.h"                // Table_stat
#include "sql/dd/types/tablespace.h"                // Tablespace
#include "sql/dd/types/view.h"                      // View
#include "sql/dd/types/statistics_collector_job.h"  // Statistics_collector_job
#include "sql/dd/upgrade_57/upgrade.h"              // allow_sdi_creation
#include "sql/debug_sync.h"                         // DEBUG_SYNC
#include "sql/error_handler.h"                      // Internal_error_handler
#include "sql/log.h"
#include "sql/sql_class.h"  // THD

namespace dd {
namespace cache {

Storage_adapter *Storage_adapter::instance() {
  static Storage_adapter s_instance;
  return &s_instance;
}

bool Storage_adapter::s_use_fake_storage = false;

// Generate a new object id for a registry partition.
template <typename T>
Object_id Storage_adapter::next_oid() {
  static Object_id next_oid = FIRST_OID;
  return next_oid++;
}

// Get the number of core objects in a registry partition.
template <typename T>
size_t Storage_adapter::core_size() {
  MUTEX_LOCK(lock, &m_lock);
  return m_core_registry.size<typename T::Cache_partition>();
}

// Get a dictionary object id from core storage.
template <typename T>
Object_id Storage_adapter::core_get_id(const typename T::Name_key &key) {
  Cache_element<typename T::Cache_partition> *element = nullptr;
  MUTEX_LOCK(lock, &m_lock);
  // Note:从注册对象m_core_registry中按照key查找对应的缓存数据
  m_core_registry.get(key, &element);
  if (element) {
    DBUG_ASSERT(element->object());
    return element->object()->id();
  }
  return INVALID_OBJECT_ID;
}

// Get a dictionary object from core storage.
template <typename K, typename T>
void Storage_adapter::core_get(const K &key, const T **object) {
  DBUG_ASSERT(object);
  *object = nullptr;
  Cache_element<typename T::Cache_partition> *element = nullptr;
  MUTEX_LOCK(lock, &m_lock);
  m_core_registry.get(key, &element);
  if (element) {
    // Must clone the object here, otherwise evicting the object from
    // the shared cache will also make it vanish from the core storage.
    *object = dynamic_cast<const T *>(element->object())->clone();
  }
}

// Update the dictionary object for a dd entity in the core registry.
/** 更新数据字典对象
 * 调用:
 * #0  dd::cache::Storage_adapter::core_update (this=0xf15f340 <dd::cache::Storage_adapter::instance()::s_instance>, new_tsp=0x7f4f2c0c1d60)
 *     at /sql/dd/impl/cache/storage_adapter.cc:131
 * #1  0x0000000006820744 in dd::cache::Dictionary_client::remove_uncommitted_objects<dd::Tablespace> (this=0x7f4f3c01d9f0, commit_to_shared_cache=true)
 *     at /sql/dd/impl/cache/dictionary_client.cc:2847
 * #2  0x00000000067fcd6f in dd::cache::Dictionary_client::commit_modified_objects (this=0x7f4f3c01d9f0)
 *     at /sql/dd/impl/cache/dictionary_client.cc:2914
 * #3  0x00000000044921b6 in trans_commit_implicit (thd=0x7f4f3c019f10, ignore_global_read_lock=false) at /sql/transaction.cc:378
 * #4  0x00000000042ef9e0 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
 *     at /sql/sql_table.cc:10143
 * #5  0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #6  0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #7  0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
 *     at /sql/sql_parse.cc:5346
 * #8  0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /data1/casonjiang/TDProxyEngine/sql/sql_parse.cc:1958
 * #9  0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
void Storage_adapter::core_update(const dd::Tablespace *new_tsp) {
  if (new_tsp->id() != MYSQL_TABLESPACE_DD_ID) {
    return;
  }

  Cache_element<typename dd::Tablespace::Cache_partition> *element = nullptr;
  typename dd::Tablespace::Id_key key(new_tsp->id());
  MUTEX_LOCK(lock, &m_lock);
  m_core_registry.get(key, &element);
  DBUG_ASSERT(element != nullptr);
  m_core_registry.remove(element);
  std::unique_ptr<const dd::Tablespace> old{element->object()};

  element->set_object(new_tsp->clone());
  element->recreate_keys();
  m_core_registry.put(element);
}

/** Note:外部接口
 * 从持久存储层获取一个数据字典对应
 * Storage_adapter是访问持久存储引擎的处理类,包括get()/drop()/store()等接口。
 * 当初次获取一个表的元信息时,会调用Storage_adapter::get()接口。
 * 
 * Storage_adapter::get()
 *   // 根据访问对象类型,将依赖的DD tables加入到open table list中
 *   |--Open_dictionary_tables_ctx::register_tables<T>()
 *     |--Table_impl::register_tables()
 *   |--Open_dictionary_tables_ctx::open_tables() // 调用Server层接口打开所有表
 *   |--Raw_table::find_record() // 直接调用handler接口根据传入的key（比如表名）查找记录
 *     |--handler::ha_index_read_idx_map() // index read
 *   // 从读取到的record中解析出对应属性,调用field[field_no]->val_xx()函数
 *   |--Table_impl::restore_attributes()
 *     // 通过调用restore_children()函数从与该对象关联的其他DD表中根据主外键读取完整的元数据定义
 *     |--Table_impl::restore_children()
 *   |--返回完整的DD cache对象
*/
// Get a dictionary object from persistent storage.
template <typename K, typename T>
bool Storage_adapter::get(THD *thd, const K &key, enum_tx_isolation isolation,
                          bool bypass_core_registry, const T **object) {
  DBUG_ASSERT(object);
  *object = nullptr;

  if (!bypass_core_registry) {
    instance()->core_get(key, object);
    if (*object || s_use_fake_storage) return false;
  }

  // We may have a cache miss while checking for existing tables during
  // server start. At this stage, the object will be considered not existing.
  // Note:在bootstrap::Stage::CREATED_TABLES阶段之前的所有查询都认为数据字典对象不存在
  if (bootstrap::DD_bootstrap_ctx::instance().get_stage() <
      bootstrap::Stage::CREATED_TABLES)
    return false;

  // Start a DD transaction to get the object.
  Transaction_ro trx(thd, isolation);
  trx.otx.register_tables<T>();  /** Note:根据访问对象类型,将依赖的DD tables加入到open table list中 */ 

  /** Note:调用Server层接口打开所有表 */
  if (trx.otx.open_tables()) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  const Entity_object_table &table = T::DD_table::instance();
  // Get main object table.
  Raw_table *t = trx.otx.get_table(table.name());

  // Find record by the object-id.
  std::unique_ptr<Raw_record> r;
  /** Note:直接调用handler接口根据传入的key（比如表名）查找记录 */
  if (t->find_record(key, r)) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  // Restore the object from the record.
  Entity_object *new_object = nullptr;
  if (r.get() &&
      table.restore_object_from_record(&trx.otx, *r.get(), &new_object)) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  // Delete the new object if dynamic cast fails.
  if (new_object) {
    // Here, a failing dynamic cast is not a legitimate situation.
    // In production, we report an error.
    *object = dynamic_cast<T *>(new_object);
    if (!*object) {
      /* purecov: begin inspected */
      my_error(ER_INVALID_DD_OBJECT, MYF(0), new_object->name().c_str());
      delete new_object;
      DBUG_ASSERT(false);
      return true;
      /* purecov: end */
    }
  }

  return false;
}

// Drop a dictionary object from core storage.
template <typename T>
void Storage_adapter::core_drop(THD *thd MY_ATTRIBUTE((unused)),
                                const T *object) {
  DBUG_ASSERT(s_use_fake_storage || thd->is_dd_system_thread());
  DBUG_ASSERT(bootstrap::DD_bootstrap_ctx::instance().get_stage() <=
              bootstrap::Stage::CREATED_TABLES);
  Cache_element<typename T::Cache_partition> *element = nullptr;
  MUTEX_LOCK(lock, &m_lock);

  // For unit tests, drop based on id to simulate behavior of persistent tables.
  // For storing core objects during bootstrap, drop based on names since id may
  // differ between scaffolding objects and persisted objects.
  if (s_use_fake_storage) {
    typename T::Id_key key;
    object->update_id_key(&key);
    m_core_registry.get(key, &element);
  } else {
    typename T::Name_key key;
    object->update_name_key(&key);
    m_core_registry.get(key, &element);
  }
  if (element) {
    m_core_registry.remove(element);
    delete element->object();
    delete element;
  }
}

// Drop a dictionary object from persistent storage.
// Note:从持久存储层(磁盘)删除一个数据子带你对象
template <typename T>
bool Storage_adapter::drop(THD *thd, const T *object) {
  if (s_use_fake_storage ||
      bootstrap::DD_bootstrap_ctx::instance().get_stage() <
          bootstrap::Stage::CREATED_TABLES) {
    instance()->core_drop(thd, object);
    return false;
  }

  if (object->impl()->validate()) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  if (sdi::drop(thd, object)) {
    return true;
  }

  // Drop the object from the dd tables. We need to switch transaction ctx to do
  // this.
  Update_dictionary_tables_ctx ctx(thd);
  ctx.otx.register_tables<T>();

  if (ctx.otx.open_tables() || object->impl()->drop(&ctx.otx)) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  return false;
}

// Store a dictionary object to core storage.
/** Note:加载一个数据字典对象到core storage
 * 
*/
template <typename T>
void Storage_adapter::core_store(THD *thd, T *object) {
  DBUG_ASSERT(s_use_fake_storage || thd->is_dd_system_thread());
  DBUG_ASSERT(bootstrap::DD_bootstrap_ctx::instance().get_stage() <=
              bootstrap::Stage::CREATED_TABLES);
  Cache_element<typename T::Cache_partition> *element =
      new Cache_element<typename T::Cache_partition>();

  if (object->id() != INVALID_OBJECT_ID) {
    // For unit tests, drop old object (based on id) to simulate update.
    if (s_use_fake_storage) core_drop(thd, object);
  } else {
    dynamic_cast<dd::Entity_object_impl *>(object)->set_id(next_oid<T>());
  }

  // Need to clone since core registry takes ownership
  element->set_object(object->clone());
  element->recreate_keys();
  MUTEX_LOCK(lock, &m_lock);
  m_core_registry.put(element);
}

// Re-map error messages emitted during DDL with innodb-read-only == 1.
class Open_dictionary_tables_error_handler : public Internal_error_handler {
 public:
  bool handle_condition(THD *, uint sql_errno, const char *,
                        Sql_condition::enum_severity_level *,
                        const char *) override {
    if (sql_errno == ER_OPEN_AS_READONLY) {
      my_error(ER_READ_ONLY_MODE, MYF(0));
      return true;
    }
    return false;
  }
};

// Store a dictionary object to persistent storage.
/** Note:数据字典对象存储到持久层
 * 1. 由数据字典的客户端接口Dictionary_client::store调用
 * 2. 最终使用具体的Object的方法实现加载,比如这里的Table_impl::store_attributes
 * 3. 最后Raw_record::store负责记录的更新
 * 
 * 调用:
 * #0  dd::Raw_record::store (this=0x7f4f2c037ce0, field_no=34, v=0, is_null=false) at /sql/dd/impl/raw/raw_record.h:73
 * #1  0x0000000006bb5e56 in dd::Table_impl::store_attributes (this=0x7f4f2c020930, r=0x7f4f2c037ce0) at /sql/dd/impl/types/table_impl.cc:491
 * #2  0x0000000006bedb87 in dd::Weak_object_impl_<true>::store (this=0x7f4f2c020930, otx=0x7f4fb04f1710)
    at /sql/dd/impl/types/weak_object_impl.cc:122
 * #3  0x0000000006a1086c in dd::cache::Storage_adapter::store<dd::Table> (thd=0x7f4f3c019f10, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/storage_adapter.cc:335
 * #4  0x000000000681de25 in dd::cache::Dictionary_client::store<dd::Table> (this=0x7f4f3c01d9f0, object=0x7f4f2c020c10)
    at /sql/dd/impl/cache/dictionary_client.cc:2578
 * #5  0x00000000042bca3c in rea_create_base_table (thd=0x7f4f3c019f10, path=0x7f4fb04f2d50 "./test/tx1", sch_obj=..., db=0x7f4f2c027a28 "test", 
    table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, create_fields=..., keys=1, key_info=0x7f4f2c029898, keys_onoff=Alter_info::ENABLE, fk_keys=0, 
    fk_key_info=0x7f4f2c029978, check_cons_spec=0x7f4fb04f3400, file=0x7f4f2c027fe8, no_ha_table=false, do_not_store_in_dd=false, part_info=0x0, 
    binlog_to_trx_cache=0x7f4fb04f316f, table_def_ptr=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:1096
 * #6  0x00000000042e6075 in create_table_impl (thd=0x7f4f3c019f10, schema=..., db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", 
    error_table_name=0x7f4f2c026da8 "tx1", path=0x7f4fb04f2d50 "./test/tx1", create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340, internal_tmp_table=false, 
    select_field_count=0, find_parent_keys=true, no_ha_table=false, do_not_store_in_dd=false, is_trans=0x7f4fb04f316f, key_info=0x7f4fb04f2f80, key_count=0x7f4fb04f2f7c, 
    keys_onoff=Alter_info::ENABLE, fk_key_info=0x7f4fb04f2f70, fk_key_count=0x7f4fb04f2f6c, existing_fk_info=0x0, existing_fk_count=0, existing_fk_table=0x0, 
    fk_max_generated_name_number=0, table_def=0x7f4fb04f2f60, post_ddl_ht=0x7f4fb04f3160) at /sql/sql_table.cc:8916
 * #7  0x00000000042e7c1d in mysql_create_table_no_lock (thd=0x7f4f3c019f10, db=0x7f4f2c027a28 "test", table_name=0x7f4f2c026da8 "tx1", create_info=0x7f4fb04f34e0, 
    alter_info=0x7f4fb04f3340, select_field_count=0, find_parent_keys=true, is_trans=0x7f4fb04f316f, post_ddl_ht=0x7f4fb04f3160)
    at /sql/sql_table.cc:9175
 * #8  0x00000000042eee79 in mysql_create_table (thd=0x7f4f3c019f10, create_table=0x7f4f2c0273e8, create_info=0x7f4fb04f34e0, alter_info=0x7f4fb04f3340)
    at /sql/sql_table.cc:10036
 * #9  0x000000000400a7e8 in Sql_cmd_create_table::execute (this=0x7f4f2c027ce0, thd=0x7f4f3c019f10) at /sql/sql_cmd_ddl_table.cc:428
 * #10 0x0000000004148e9a in mysql_execute_command (thd=0x7f4f3c019f10, first_level=true) at /sql/sql_parse.cc:3645
 * #11 0x0000000004154c8f in dispatch_sql_command (thd=0x7f4f3c019f10, parser_state=0x7f4fb04f4cf0, update_userstat=false)
    at /sql/sql_parse.cc:5346
 * #12 0x000000000413dc9b in dispatch_command (thd=0x7f4f3c019f10, com_data=0x7f4fb04f5e90, command=COM_QUERY) at /sql/sql_parse.cc:1958
 * #13 0x0000000004139c4f in do_command (thd=0x7f4f3c019f10) at /sql/sql_parse.cc:1404
*/
template <typename T>
bool Storage_adapter::store(THD *thd, T *object) {
  // Note:如果是测试或者未到真正需要建表的阶段,只存入缓存,不进行持久化存储
  if (s_use_fake_storage ||
      bootstrap::DD_bootstrap_ctx::instance().get_stage() <
          bootstrap::Stage::CREATED_TABLES) {
    instance()->core_store(thd, object);
    return false;
  }
  // Note:这里会验证DD对象的有效性
  if (object->impl()->validate()) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    return true;
  }

  // Store the object into the dd tables. We need to switch transaction
  // ctx to do this.
  /** Note:切换上下文,包括更新系统表的时候关闭binlog、修改auto_increament_increament增量、设置一些相关变量等与修改DD相关的上下文。
  */
  Update_dictionary_tables_ctx ctx(thd);
  ctx.otx.register_tables<T>();  
  /** Note:注册入参类型
   * 比如这里如果是需要持久化元数据到mysql.tables系统表
   * 则这里的T就是tables,然后前面只需要初始化这个object即可
  */
  DEBUG_SYNC(thd, "before_storing_dd_object");

  Open_dictionary_tables_error_handler error_handler;
  thd->push_internal_handler(&error_handler);
  /** Note:object->impl()->store 这里会将DD对象存入相关的系统表
   * 具体比如表,列, 表空间是如何持久化到系统表中的
   * 具体调用Table_impl::store_attributes,加载系统表的信息
  */
  if (ctx.otx.open_tables() || object->impl()->store(&ctx.otx)) {
    DBUG_ASSERT(thd->is_system_thread() || thd->killed || thd->is_error());
    thd->pop_internal_handler();
    return true;
  }
  thd->pop_internal_handler();

  // Do not create SDIs for tablespaces and tables while creating
  // dictionary entry during upgrade.
  // Note:调用SDI接口实现持久层操作
  if (bootstrap::DD_bootstrap_ctx::instance().get_stage() >
          bootstrap::Stage::CREATED_TABLES &&
      dd::upgrade_57::allow_sdi_creation() && sdi::store(thd, object))
    return true;

  return false;
}

// Sync a dictionary object from persistent to core storage.
// Note:从持久存储层加载数据字典对象(比如table)到缓存层的core storage
template <typename T>
bool Storage_adapter::core_sync(THD *thd, const typename T::Name_key &key,
                                const T *object) {
  DBUG_ASSERT(thd->is_dd_system_thread());
  DBUG_ASSERT(bootstrap::DD_bootstrap_ctx::instance().get_stage() <=
              bootstrap::Stage::CREATED_TABLES);

  // Copy the name, needed for error output. The object has to be
  // dropped before get().
  String_type name(object->name());
  core_drop(thd, object);
  const typename T::Cache_partition *new_obj = nullptr;

  /*
    Fetch the object from persistent tables. The object was dropped
    from the core registry above, so we know get() will fetch it
    from the tables.

    There is a theoretical possibility of get() failing or sending
    back a nullptr if there has been a corruption or wrong usage
    (e.g. dropping a DD table), leaving one or more DD tables
    inaccessible. Assume, e.g., that the 'mysql.tables' table has
    been dropped. Then, the following will happen during restart:

    1. After creating the scaffolding, the meta data representing
       the DD tables is kept in the shared cache, secured by a
       scoped auto releaser in 'sync_meta_data()' in the bootstrapper
       (this is to make sure the meta data is not evicted during
       synchronization).
    2. We sync the DD tables, starting with 'mysql.character_sets'
       (because it is the first entry in the System_table_registry).
    3. Here in core_sync(), the entry in the core registry is
       removed. Then, we call get(), which will read the meta data
       from the persistent DD tables.
    4. While trying to fetch the meta data for the first table to
       be synced (i.e., 'mysql.character_sets'), we first open
       the tables that are needed to read the meta data for a table
       (i.e., we open the core tables). One of these tables is the
       'mysql.tables' table.
    5. While opening these tables, the server will fetch the meta
       data for them. The meta data for 'mysql.tables' is indeed
       found (because it was created as part of the scaffolding
       with the meta data now being in the shared cache), however,
       when opening the table in the storage engine, we get a
       failure because the SE knows nothing about this table, and
       is unable to open it.
  */
  if (get(thd, key, ISO_READ_COMMITTED, false, &new_obj) ||
      new_obj == nullptr) {
    LogErr(ERROR_LEVEL, ER_DD_METADATA_NOT_FOUND, name.c_str());
    return true;
  }

  Cache_element<typename T::Cache_partition> *element =
      new Cache_element<typename T::Cache_partition>();
  element->set_object(new_obj);
  element->recreate_keys();  //Note:重新生成key
  MUTEX_LOCK(lock, &m_lock);
  m_core_registry.put(element);  // Note:注册新的对象
  return false;
}

// Remove and delete all elements and objects from core storage.
void Storage_adapter::erase_all() {
  MUTEX_LOCK(lock, &m_lock);
  instance()->m_core_registry.erase_all();
}

// Dump the contents of the core storage.
void Storage_adapter::dump() {
#ifndef DBUG_OFF
  MUTEX_LOCK(lock, &m_lock);
  fprintf(stderr, "================================\n");
  fprintf(stderr, "Storage adapter\n");
  m_core_registry.dump<dd::Tablespace>();
  m_core_registry.dump<dd::Schema>();
  m_core_registry.dump<dd::Abstract_table>();
  fprintf(stderr, "================================\n");
#endif
}

// Explicitly instantiate the type for the various usages.
template bool Storage_adapter::core_sync(THD *, const Table::Name_key &,
                                         const Table *);
template bool Storage_adapter::core_sync(THD *, const Tablespace::Name_key &,
                                         const Tablespace *);
template bool Storage_adapter::core_sync(THD *, const Schema::Name_key &,
                                         const Schema *);

template Object_id Storage_adapter::core_get_id<Table>(const Table::Name_key &);
template Object_id Storage_adapter::core_get_id<Schema>(
    const Schema::Name_key &);
template Object_id Storage_adapter::core_get_id<Tablespace>(
    const Tablespace::Name_key &);

template void Storage_adapter::core_get(dd::Item_name_key const &,
                                        const dd::Schema **);
template void Storage_adapter::core_get<dd::Item_name_key, dd::Abstract_table>(
    dd::Item_name_key const &, const dd::Abstract_table **);
template void Storage_adapter::core_get<dd::Global_name_key, dd::Tablespace>(
    dd::Global_name_key const &, const dd::Tablespace **);

template Object_id Storage_adapter::next_oid<Abstract_table>();
template Object_id Storage_adapter::next_oid<Table>();
template Object_id Storage_adapter::next_oid<View>();
template Object_id Storage_adapter::next_oid<Charset>();
template Object_id Storage_adapter::next_oid<Collation>();
template Object_id Storage_adapter::next_oid<Column_statistics>();
template Object_id Storage_adapter::next_oid<Event>();
template Object_id Storage_adapter::next_oid<Routine>();
template Object_id Storage_adapter::next_oid<Function>();
template Object_id Storage_adapter::next_oid<Procedure>();
template Object_id Storage_adapter::next_oid<Schema>();
template Object_id Storage_adapter::next_oid<Spatial_reference_system>();
template Object_id Storage_adapter::next_oid<Tablespace>();

template size_t Storage_adapter::core_size<Abstract_table>();
template size_t Storage_adapter::core_size<Table>();
template size_t Storage_adapter::core_size<Schema>();
template size_t Storage_adapter::core_size<Tablespace>();

template bool Storage_adapter::get<Abstract_table::Id_key, Abstract_table>(
    THD *, const Abstract_table::Id_key &, enum_tx_isolation, bool,
    const Abstract_table **);
template bool Storage_adapter::get<Abstract_table::Name_key, Abstract_table>(
    THD *, const Abstract_table::Name_key &, enum_tx_isolation, bool,
    const Abstract_table **);
template bool Storage_adapter::get<Abstract_table::Aux_key, Abstract_table>(
    THD *, const Abstract_table::Aux_key &, enum_tx_isolation, bool,
    const Abstract_table **);
template bool Storage_adapter::drop(THD *, const Abstract_table *);
template bool Storage_adapter::store(THD *, Abstract_table *);
template bool Storage_adapter::drop(THD *, const Table *);
template bool Storage_adapter::store(THD *, Table *);
template bool Storage_adapter::drop(THD *, const View *);
template bool Storage_adapter::store(THD *, View *);

template bool Storage_adapter::get<Charset::Id_key, Charset>(
    THD *, const Charset::Id_key &, enum_tx_isolation, bool, const Charset **);
template bool Storage_adapter::get<Charset::Name_key, Charset>(
    THD *, const Charset::Name_key &, enum_tx_isolation, bool,
    const Charset **);
template bool Storage_adapter::get<Charset::Aux_key, Charset>(
    THD *, const Charset::Aux_key &, enum_tx_isolation, bool, const Charset **);
template bool Storage_adapter::drop(THD *, const Charset *);
template bool Storage_adapter::store(THD *, Charset *);

template bool Storage_adapter::get<Collation::Id_key, Collation>(
    THD *, const Collation::Id_key &, enum_tx_isolation, bool,
    const Collation **);
template bool Storage_adapter::get<Collation::Name_key, Collation>(
    THD *, const Collation::Name_key &, enum_tx_isolation, bool,
    const Collation **);
template bool Storage_adapter::get<Collation::Aux_key, Collation>(
    THD *, const Collation::Aux_key &, enum_tx_isolation, bool,
    const Collation **);
template bool Storage_adapter::drop(THD *, const Collation *);
template bool Storage_adapter::store(THD *, Collation *);

template bool
Storage_adapter::get<Column_statistics::Id_key, Column_statistics>(
    THD *, const Column_statistics::Id_key &, enum_tx_isolation, bool,
    const Column_statistics **);
template bool
Storage_adapter::get<Column_statistics::Name_key, Column_statistics>(
    THD *, const Column_statistics::Name_key &, enum_tx_isolation, bool,
    const Column_statistics **);
template bool
Storage_adapter::get<Column_statistics::Aux_key, Column_statistics>(
    THD *, const Column_statistics::Aux_key &, enum_tx_isolation, bool,
    const Column_statistics **);
template bool Storage_adapter::drop(THD *, const Column_statistics *);
template bool Storage_adapter::store(THD *, Column_statistics *);

template bool Storage_adapter::get<Event::Id_key, Event>(THD *,
                                                         const Event::Id_key &,
                                                         enum_tx_isolation,
                                                         bool, const Event **);
template bool Storage_adapter::get<Event::Name_key, Event>(
    THD *, const Event::Name_key &, enum_tx_isolation, bool, const Event **);
template bool Storage_adapter::get<Event::Aux_key, Event>(
    THD *, const Event::Aux_key &, enum_tx_isolation, bool, const Event **);
template bool Storage_adapter::drop(THD *, const Event *);
template bool Storage_adapter::store(THD *, Event *);

template bool Storage_adapter::get<Resource_group::Id_key, Resource_group>(
    THD *, const Resource_group::Id_key &, enum_tx_isolation, bool,
    const Resource_group **);
template bool Storage_adapter::get<Resource_group::Name_key, Resource_group>(
    THD *, const Resource_group::Name_key &, enum_tx_isolation, bool,
    const Resource_group **);
template bool Storage_adapter::get<Resource_group::Aux_key, Resource_group>(
    THD *, const Resource_group::Aux_key &, enum_tx_isolation, bool,
    const Resource_group **);
template bool Storage_adapter::drop(THD *, const Resource_group *);
template bool Storage_adapter::store(THD *, Resource_group *);

template bool Storage_adapter::get<Routine::Id_key, Routine>(
    THD *, const Routine::Id_key &, enum_tx_isolation, bool, const Routine **);
template bool Storage_adapter::get<Routine::Name_key, Routine>(
    THD *, const Routine::Name_key &, enum_tx_isolation, bool,
    const Routine **);
template bool Storage_adapter::get<Routine::Aux_key, Routine>(
    THD *, const Routine::Aux_key &, enum_tx_isolation, bool, const Routine **);
template bool Storage_adapter::drop(THD *, const Routine *);
template bool Storage_adapter::store(THD *, Routine *);
template bool Storage_adapter::drop(THD *, const Function *);
template bool Storage_adapter::store(THD *, Function *);
template bool Storage_adapter::drop(THD *, const Procedure *);
template bool Storage_adapter::store(THD *, Procedure *);

template bool Storage_adapter::get<Schema::Id_key, Schema>(
    THD *, const Schema::Id_key &, enum_tx_isolation, bool, const Schema **);
template bool Storage_adapter::get<Schema::Name_key, Schema>(
    THD *, const Schema::Name_key &, enum_tx_isolation, bool, const Schema **);
template bool Storage_adapter::get<Schema::Aux_key, Schema>(
    THD *, const Schema::Aux_key &, enum_tx_isolation, bool, const Schema **);
template bool Storage_adapter::drop(THD *, const Schema *);
template bool Storage_adapter::store(THD *, Schema *);

template bool Storage_adapter::get<Spatial_reference_system::Id_key,
                                   Spatial_reference_system>(
    THD *, const Spatial_reference_system::Id_key &, enum_tx_isolation, bool,
    const Spatial_reference_system **);
template bool Storage_adapter::get<Spatial_reference_system::Name_key,
                                   Spatial_reference_system>(
    THD *, const Spatial_reference_system::Name_key &, enum_tx_isolation, bool,
    const Spatial_reference_system **);
template bool Storage_adapter::get<Spatial_reference_system::Aux_key,
                                   Spatial_reference_system>(
    THD *, const Spatial_reference_system::Aux_key &, enum_tx_isolation, bool,
    const Spatial_reference_system **);
template bool Storage_adapter::drop(THD *, const Spatial_reference_system *);
template bool Storage_adapter::store(THD *, Spatial_reference_system *);

template bool Storage_adapter::get<Tablespace::Id_key, Tablespace>(
    THD *, const Tablespace::Id_key &, enum_tx_isolation, bool,
    const Tablespace **);
template bool Storage_adapter::get<Tablespace::Name_key, Tablespace>(
    THD *, const Tablespace::Name_key &, enum_tx_isolation, bool,
    const Tablespace **);
template bool Storage_adapter::get<Tablespace::Aux_key, Tablespace>(
    THD *, const Tablespace::Aux_key &, enum_tx_isolation, bool,
    const Tablespace **);
template bool Storage_adapter::drop(THD *, const Tablespace *);
template bool Storage_adapter::store(THD *, Tablespace *);

// Statistics_collector_job table
template <>
void Storage_adapter::core_get(const Statistics_collector_job::Name_key &,
                               const Statistics_collector_job **) {}
template <>
void Storage_adapter::core_get(const Statistics_collector_job::Id_key &,
                               const Statistics_collector_job **) {}

template <>
void Storage_adapter::core_drop(THD *, const Statistics_collector_job *) {}

template <>
void Storage_adapter::core_store(THD *, Statistics_collector_job *) {}

template bool Storage_adapter::get<Statistics_collector_job::Name_key, Statistics_collector_job>(
    THD *, const Statistics_collector_job::Name_key &, enum_tx_isolation, bool,
    const Statistics_collector_job **);
template bool Storage_adapter::get<Statistics_collector_job::Id_key, Statistics_collector_job>(
    THD *, const Statistics_collector_job::Id_key &, enum_tx_isolation, bool,
    const Statistics_collector_job **);
template bool Storage_adapter::store(THD *, Statistics_collector_job *);

template bool Storage_adapter::drop(THD *, const Statistics_collector_job *);
/*
  DD objects dd::Table_stat and dd::Index_stat are not cached,
  because these objects are only updated and never read by DD
  API's. Information schema system views use these DD tables
  to project table/index statistics. As these objects are
  not in DD cache, it cannot make it to core storage.
*/
template <>
void Storage_adapter::core_get(const Basic_column_statistic::Name_key &,
                               const Basic_column_statistic **) {}

template <>
void Storage_adapter::core_drop(THD *, const Basic_column_statistic *) {}

template <>
void Storage_adapter::core_store(THD *, Basic_column_statistic *) {}

template bool Storage_adapter::get<Basic_column_statistic::Name_key, Basic_column_statistic>(
    THD *, const Basic_column_statistic::Name_key &, enum_tx_isolation, bool,
    const Basic_column_statistic **);
template bool Storage_adapter::store(THD *, Basic_column_statistic *);
template bool Storage_adapter::drop(THD *, const Basic_column_statistic *);

template <>
void Storage_adapter::core_get(const Table_stat::Name_key &,
                               const Table_stat **) {}

template <>
void Storage_adapter::core_get(const Index_stat::Name_key &,
                               const Index_stat **) {}

template <>
void Storage_adapter::core_drop(THD *, const Table_stat *) {}

template <>
void Storage_adapter::core_drop(THD *, const Index_stat *) {}

template <>
void Storage_adapter::core_store(THD *, Table_stat *) {}

template <>
void Storage_adapter::core_store(THD *, Index_stat *) {}

template bool Storage_adapter::get<Table_stat::Name_key, Table_stat>(
    THD *, const Table_stat::Name_key &, enum_tx_isolation, bool,
    const Table_stat **);
template bool Storage_adapter::store(THD *, Table_stat *);
template bool Storage_adapter::drop(THD *, const Table_stat *);
template bool Storage_adapter::get<Index_stat::Name_key, Index_stat>(
    THD *, const Index_stat::Name_key &, enum_tx_isolation, bool,
    const Index_stat **);
template bool Storage_adapter::store(THD *, Index_stat *);
template bool Storage_adapter::drop(THD *, const Index_stat *);

// Doxygen doesn't understand this explicit template instantiation.
#ifndef IN_DOXYGEN
template void Storage_adapter::core_drop<Schema>(THD *, const Schema *);
template void Storage_adapter::core_drop<Table>(THD *, const Table *);
template void Storage_adapter::core_drop<Tablespace>(THD *, const Tablespace *);

template void Storage_adapter::core_store<Schema>(THD *, Schema *);
template void Storage_adapter::core_store<Table>(THD *, Table *);
template void Storage_adapter::core_store<Tablespace>(THD *, Tablespace *);
#endif

}  // namespace cache
}  // namespace dd
