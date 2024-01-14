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
#include <string>
#include <vector>

//#include "sql/tdsql/td_servers.h"
/* 
   this class use the information from mysql.servsers route tables to part
   single spider table atomaticly
*/

namespace tdsql::meta_data {
class TdClusterInfo;
}

class THD;
class HA_CREATE_INFO;
namespace dd {
class Table;
}
struct TABLE;
class FOREIGN_SERVER;
#define MAX_SHARD_NUM 16

class spider_auto_partition {
 public:
  spider_auto_partition(THD *thd, HA_CREATE_INFO *create_info, dd::Table *dd_def,
                        TABLE *table) : spd_table(table), spd_create_info(create_info),
                        spd_table_def(dd_def), ha_thd(thd) {}  
  ~spider_auto_partition() {}

  bool add_partitions();
  bool update_dd_table();

 private:
  std::string generate_part_comment_str(
      const std::shared_ptr<tdsql::meta_data::TdSetInfo> &set_info);
  std::string generate_list_items_str(int start, int end);
  std::string generate_list_parts_str();
  bool get_shard_key_names();
  bool generate_patition_str();
  bool create_part_info_from_str();
  
  TABLE *spd_table;
  HA_CREATE_INFO *spd_create_info;
  dd::Table *spd_table_def;  // need update the dd after add partitions
  THD *ha_thd;

  std::string partition_str;
  std::vector<std::string> shard_key_names;
  std::shared_ptr<tdsql::meta_data::TdClusterInfo> cluster_info;
};
