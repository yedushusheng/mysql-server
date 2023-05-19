/* Copyright (c) 2015, 2020, Oracle and/or its affiliates. All rights reserved.

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

#ifndef DD_CACHE__STORAGE_ADAPTER_INCLUDED
#define DD_CACHE__STORAGE_ADAPTER_INCLUDED

#include <stddef.h>

#include "mysql/components/services/bits/psi_bits.h"
#include "mysql/components/services/mysql_mutex_bits.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql/dd/cache/object_registry.h"  // Object_registry
#include "sql/dd/object_id.h"
#include "sql/handler.h"  // enum_tx_isolation
#include "thr_mutex.h"

class THD;

namespace dd_cache_unittest {
class CacheStorageTest;
}

/**
 * Note:数据字典存储适配器
 * Storage_adapter是MySQL内部实现的一种存储适配器,用于在MySQL中管理存储引擎之间的数据转换和交互.
 * 在MySQL中,每个存储引擎都有自己的存储格式和数据访问方式,Storage_adapter的作用就是将不同存储引擎之间的数据转换为统一的格式,以实现数据的共享和交互.
 * Storage_adapter在MySQL中是由ha_storage_adapter类实现的.它通过提供一组接口来访问和管理存储引擎中的数据,包括读取和写入数据、锁定和释放数据、事务管理、索引管理等等.
 * Storage_adapter还提供了一组标准的API,以便存储引擎可以实现与MySQL内部的交互.当一个存储引擎被加载到MySQL中时,它会注册自己的API,以便MySQL可以使用Storage_adapter来访问和管理它的数据.
 * 总的来说,Storage_adapter是MySQL内部实现的一种存储适配器,它通过提供一组接口和标准的API,将不同存储引擎之间的数据转换为统一的格式,以实现数据的共享和交互.它是MySQL数据字典的核心组成部分之一.
 * 
 * 数据字典表本身的元数据也会保存到数据字典表里,但是某个数据字典表创建的时候,有一些数据字典表还没有创建,这就有问题了.
 * 我们以columns、indexes这2个数据字典表为例来说明：
 * columns表先于indexes表创建,columns表创建成功之后,需要把索引元数据保存到indexes表中,而此时 indexes表还没有创建,columns表的索引元数据自然也就没办法保存到indexes表中了.
 * MySQL解决这个问题的方案是引入一个中间层,用于临时存放所有数据字典表的各种元数据,等到所有数据字典表都创建完成之后,再把临时存放在中间层的所有数据字典表的元数据保存到相应的数据字典表中.
 * 这里所谓的中间层实际上是一个存储适配器,源码中对应的类名为 Storage_adapter,这是一个实现了单例模式的类.
 * MySQL在初始化数据目录的过程中,Storage_adapter类的实例属性m_core_registry就是所有数据字典表元数据的临时存放场所.
 */
namespace dd {

namespace cache {

/**
  Handling of access to persistent storage.

  This class provides static template functions that manipulates an object on
  persistent storage based on the submitted key and object type. There is also
  an object registry instance to keep the core DD objects that are needed to
  handle cache misses for table meta data. The storage adapter owns the objects
  in the core registry. When adding objects to the registry using core_store(),
  the storage adapter will clone the object and take ownership of the clone.
  When retrieving objects from the registry using core_get(), a clone of the
  object will be returned, and this is therefore owned by the caller.
*/
/** Note:存储core属性的数据字典对象
 * 这个类抽象了对DD对象的metadata进行存储的方法.
 * 它是一个静态类.对于新创建的对象（表,索引,表空间等）都会通过该类进行一个clone, clone之后该类会将该对象的metadata存储到对应的系统表中.
 * 另外,它也提供接口用来从系统表中获取metadata并生成调用需要的DD对象.
 * 该类同时也提供了一个缓存,每次调用存储新对象的时候,它会自动将一个对象clone缓存起来.
 * 
 * 说明:
 * 这里是负责操作存储引擎层的,因此会调用raw/raw_record.h相关接口,进而调用存储引擎handler接口
 * 
 * 该类成员函数中core_xxx都是负责操作缓存,比如core_dop.
 * 不带core前缀的都是操作磁盘的,比如drop.
*/
class Storage_adapter {
  friend class dd_cache_unittest::CacheStorageTest;

 private:
  /**
    Use an id not starting at 1 to make it easy to recognize ids generated
    before objects are stored persistently.
  */

  static const Object_id FIRST_OID = 10001;

  /**
    Generate a new object id for a registry partition.

    Simulate an auto increment column. Used when the server is starting,
    while the scaffolding is being built.

    @tparam T      Object registry partition.

    @return Next object id to be used.
  */
  /** Note:为新的对象产生一个ID标识.
  */
  template <typename T>
  Object_id next_oid();

  /**
    Get a dictionary object from core storage.

    A clone of the registry object will be returned, owned by the caller.

    @tparam      K         Key type.
    @tparam      T         Dictionary object type.
    @param       key       Key for which to get the object.
    @param [out] object    Object retrieved, possibly nullptr if not present.
  */
  // Note:根据对象名称从缓存中返回一个对象的clone
  template <typename K, typename T>
  void core_get(const K &key, const T **object);

  Object_registry m_core_registry;  // Object registry storing core DD objects.
  /** Note:所有数据字典表元数据的临时存放场所 */

  mysql_mutex_t m_lock;             // Single mutex to protect the registry.
  static bool s_use_fake_storage;   // Whether to use the core registry to
                                    // simulate the storage engine.

  Storage_adapter() {
    mysql_mutex_init(PSI_NOT_INSTRUMENTED, &m_lock, MY_MUTEX_INIT_FAST);
  }

  ~Storage_adapter() {
    mysql_mutex_lock(&m_lock);
    m_core_registry.erase_all();
    mysql_mutex_unlock(&m_lock);
    mysql_mutex_destroy(&m_lock);
  }

 public:
  // Note:这里可以获取到单例
  static Storage_adapter *instance();

  /**
    Get the number of core objects in a registry partition.

    @tparam      T         Dictionary object type.
    @return      Number of elements.
  */
  // Note:根据对象类型返回缓存区中所有对象的数量
  template <typename T>
  size_t core_size();

  /**
    Get a dictionary object id from core storage.

    @tparam      T         Dictionary object type.
    @param       key       Name key for which to get the object id.
    @return      Object id, INVALID_OBJECT_ID if the object is not present.
  */
  // Note:获取对象ID标识
  template <typename T>
  Object_id core_get_id(const typename T::Name_key &key);

  /**
     Update the dd object in the core registry.  This is a noop unless
     this member function is overloaded for a given type. See below.
   */
  template <typename T>
  void core_update(const T *) {}

  /**
     Overload of core_update for dd::Tablespace. Currently the core
     registry can only be updated for the DD tablespace when
     encrypting it. A clone of the dd::Tablespace object passed in is
     stored in the registry.

     @param new_tsp the new dd::Tablespace object to keep in the core registry.
  */
  void core_update(const dd::Tablespace *new_tsp);

  /**
    Get a dictionary object from persistent storage.

    Create an access key based on the submitted key, and find the record
    from the appropriate table. Restore the record into a new dictionary
    object.

    @tparam      K         Key type.
    @tparam      T         Dictionary object type.
    @param       thd       Thread context.
    @param       key       Key for which to get the object.
    @param       isolation Isolation level.
    @param       bypass_core_registry If set to true, get the object from the
                                      DD tables. Needed during DD bootstrap.
    @param [out] object    Object retrieved, possibly NULL if not present.

    @retval      false   No error.
    @retval      true    Error.
  */
  /** Note:该函数可以根据对象类型及名称获取对象.
   * 如果该对象已经被缓存,那么调用core_get获取clone对象.
   * 否则会根据对象类型到对应的metadata数据表中查找并构造一个对象.
  */
  template <typename K, typename T>
  static bool get(THD *thd, const K &key, enum_tx_isolation isolation,
                  bool bypass_core_registry, const T **object);

  /**
    Drop a dictionary object from core storage.

    @tparam  T       Dictionary object type.
    @param   thd     Thread context.
    @param   object  Object to be dropped.
  */
  // Note:缓存中清除一个对象
  template <typename T>
  void core_drop(THD *thd, const T *object);

  /**
    Drop a dictionary object from persistent storage.

    @tparam  T       Dictionary object type.
    @param   thd     Thread context.
    @param   object  Object to be dropped.

    @retval  false   No error.
    @retval  true    Error.
  */
  // Note:从对象所对应的各个metadata数据表中清除相关数据
  template <typename T>
  static bool drop(THD *thd, const T *object);

  /**
    Store a dictionary object to core storage.

    A clone of the submitted object will be added to the core
    storage. The caller is still the owner of the submitted
    objecct.

    @tparam  T       Dictionary object type.
    @param   thd     Thread context.
    @param   object  Object to be stored.
  */
  // Note:缓冲区中添加一个DD对象
  template <typename T>
  void core_store(THD *thd, T *object);

  /**
    Store a dictionary object to persistent storage.

    @tparam  T       Dictionary object type.
    @param   thd     Thread context.
    @param   object  Object to be stored.

    @retval  false   No error.
    @retval  true    Error.
  */
  /** Note:该函数会根据DD对象类型,将metadata存入相关的系统表中.
  */
  template <typename T>
  static bool store(THD *thd, T *object);

  /**
    Sync a dictionary object from persistent to core storage.

    @tparam      T         Dictionary object type.
    @param       thd       Thread context.
    @param       key       Key for object to get from persistent storage.
    @param       object    Object to drop from the core registry.
  */
  // Note:同步缓存中的DD对象
  template <typename T>
  bool core_sync(THD *thd, const typename T::Name_key &key, const T *object);

  /**
    Remove and delete all elements and objects from core storage.
  */

  void erase_all();

  /**
    Dump the contents of the core storage.
  */
  // Note:备份缓存中对象
  void dump();
};

}  // namespace cache
}  // namespace dd

#endif  // DD_CACHE__STORAGE_ADAPTER_INCLUDED
