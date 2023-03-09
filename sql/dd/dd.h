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

#ifndef DD__DD_INCLUDED
#define DD__DD_INCLUDED
/** Note:数据字典 
 * 在MySQL8.0前，Server层和存储引擎各自保留一份元数据，这样存在一些问题：
 * 1. 信息存储上存在冗余，而且可能存在Server和存储引擎之间存储的元数据不同步的现象
 * 2. 不同存储引擎之间有着不同的元数据存储形式和位置，造成元数据无法统一管理
 * 3. 将元数据存放在不支持事务的表和文件中，使得DDL变更不会是原子的，crash recovery也会成为一个问题
 * 为了解决该问题，MySQL8.0引入Data Dictionary来进行Server和不同存储引擎间统一的元数据管理
 * 这些数据存储在InnoDB引擎的表中，自然的支持原子性，且Server层和引擎层共享一份元数据，不再存在不同步的问题
*/
namespace dd {

///////////////////////////////////////////////////////////////////////////

// enum type to pass to init() function.
enum class enum_dd_init_type {
  DD_INITIALIZE = 1,
  DD_INITIALIZE_SYSTEM_VIEWS,
  DD_RESTART_OR_UPGRADE,
  DD_POPULATE_UPGRADE,
  DD_DELETE,
  DD_UPDATE_I_S_METADATA,
  DD_INITIALIZE_NON_DD_BASED_SYSTEM_VIEWS
};

/**
  Initialize data dictionary upon server startup, server startup on old
  data directory or install data dictionary for the first time.

  @param dd_init - Option for initialization, population or deletion
                   of data dictionary.

  @return false - On success
  @return true - On error
*/
bool init(enum_dd_init_type dd_init);

/**
  Shuts down the data dictionary instance by deleting
  the instance of dd::Dictionary_impl* upon server shutdown.

  @return false - On success
  @return true - If invoked when data dictionary instance
                 is not yet initialized.
*/
bool shutdown();

/**
  Get the data dictionary instance.

  @returns 'Dictionary*' pointer to data dictionary instance.
           Else returns NULL if data dictionary is not
           initialized.
*/
class Dictionary *get_dictionary();

/**
  Create a instance of data dictionary object of type X.
  E.g., X could be 'dd::Table', 'dd::View' and etc.

  @returns Pointer to the newly allocated dictionary object.
*/
template <typename X>
X *create_object();

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__DD_INCLUDED
