/* Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.

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

#ifndef DD__ENTITY_OBJECT_INCLUDED
#define DD__ENTITY_OBJECT_INCLUDED

#include "sql/dd/object_id.h"          // dd::Object_id
#include "sql/dd/string_type.h"        // dd::String_type
#include "sql/dd/types/weak_object.h"  // dd::Weak_object

namespace dd {
namespace cache {
class Storage_adapter;
}

///////////////////////////////////////////////////////////////////////////

/**
  Base class for dictionary objects which has single column
  integer primary key.

  @note This class may be inherited along different paths
        for some subclasses due to the diamond shaped
        inheritance hierarchy; thus, direct subclasses
        must inherit this class virtually.
*/
/** Note:对于含有单列int类型主键数据字典的基类
 * 调用关系:
 * Weak_object
 * 	Column_type_element
 * 	Entity_object
 * 		Entity_object_impl
 * 			Charset_impl
 * 		Check_constraint_impl
 * 			Collation_impl
 * 			Column_statistics_impl
 * 			Column_impl
 * 			Event_impl
 * 			Foreign_key_impl
 * 			Index_stat_impl
 * 			Index_impl
 * 			Parameter_impl
 * 			Partition_impl
 * 			Resource_group_impl
 * 			Routine_impl
 * 			Schema_impl
 * 			Spatial_reference_system_impl
 * 			Trigger_impl
 * 			......
 * 		Abstract_table
 * 			Table
 * 			View
 * 		Charset
 * 			Charset_impl
 * 		Check_constraint
 * 			Check_constraint_impl
 * 		Collation
 * 			Collation_impl
 * 		Column_statistics
 * 			Column_statistics_impl
 * 		Column
 * 			Column_impl
 * 		Event
 * 			Event_impl
 * 		Foreign_key
 * 			Foreign_key_impl
 * 		Index_stat
 * 			Index_stat_impl
 * 		Index
 * 			Index_impl
 * 		Parameter
 * 			Parameter_impl
 * 		Partition
 * 			Partition_impl
 * 		Resource_group
 * 			Resource_group_impl
 * 		Routine
 * 			Routine_impl
 * 				Function_impl
 * 				Procedure_impl
 * 			Function
 * 				Function_impl
 * 			Procedure
 * 				Procedure_impl
 * 		Schema
 * 			Schema_impl
 * 		Spatial_reference_system
 * 			Spatial_reference_system_impl
 * 				Geographic_srs
 * 				Projected_srs
 * 		Table_stat
 * 			Table_stat_impl
 * 		Tablespace
 * 			SysTablespace
 * 		Trigger
 * 			Trigger_impl
 * 	Foreign_key_element
 * 		Foreign_key_element_impl
 * 	Index_element
 * 		Index_element_impl
 * 	Parameter_type_element
 * 		Parameter_type_element_impl
 * 	Partition_index
 * 		Partition_index_impl
 * 	Partition_value
 * 		Partition_value_impl
 * 	Tablespace_file
 * 		Tablespace_file_impl
 * 	View_routine
 * 		View_routine_impl
 * 	View_table
 * 		View_table_impl
*/
class Entity_object : virtual public Weak_object {
 public:
  /// The unique dictionary object id.
  virtual Object_id id() const = 0;

  /// Is dictionary object persistent in dictionary tables ?
  virtual bool is_persistent() const = 0;

  virtual const String_type &name() const = 0;
  virtual void set_name(const String_type &name) = 0;

 private:
  virtual class Entity_object_impl *impl() = 0;
  virtual const class Entity_object_impl *impl() const = 0;
  friend class cache::Storage_adapter;
  friend class Entity_object_table_impl;
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__ENTITY_OBJECT_INCLUDED
