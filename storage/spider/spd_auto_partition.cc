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

#define MYSQL_SERVER 1
#include "sql/tdsql/td_servers.h"

#include "spd_auto_partition.h"
#include <my_config.h>
#include "sql/mysqld.h"
#include "sql/table.h"
#include "sql/key.h"
#include "sql/field.h"
#include "sql/sql_servers.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/dd/dd_table.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dictionary.h"

/* 
 * get the shard key names from the table_share
 * if the table has primary key, then return the first field of the primary key
 * else retrurn the first field of the table
 */
bool spider_auto_partition::get_shard_key_names() {
  std::string field_name;
  if (spd_table->s->primary_key < MAX_KEY) {
    KEY *keyinfo = spd_table->s->key_info + spd_table->s->primary_key;
    KEY_PART_INFO *keypart = keyinfo->key_part; 
    field_name = keypart->field->field_name;
    shard_key_names.emplace_back(field_name);
  } else {
    field_name = spd_table->s->field[0]->field_name;
    shard_key_names.emplace_back(field_name);
  }
  return false;
}

/* the route comment of the partitions */
std::string spider_auto_partition::generate_part_comment_str(
    const std::shared_ptr<tdsql::meta_data::TdSetInfo> &set_info) {
  std::string comment_str;
  assert(set_info);
//  assert(server != nullptr);

  comment_str = "comment='";
  comment_str += "srv \"" + std::string(set_info->Name().c_str()) + "\",";
  // comment_str += "database \"" + std::string(server->db) + "\",";
  comment_str += "database \"" + std::string(spd_table->s->db.str) + "\",";
  comment_str += "table \"" + std::string(spd_table->s->table_name.str) + "\"'"; 

  return comment_str;
}

/* the (1,2,3,4,5 ...) items in partition list */
std::string spider_auto_partition::generate_list_items_str(int start, int end) {
  std::string list_items;
  int i = start;
  for (; i < (end - 1); i++) {
    list_items += std::to_string(i) + ",";
  }
  list_items += std::to_string(i);
  return list_items;
}

std::string spider_auto_partition::generate_list_parts_str() {
  std::string parts_str;
  assert(cluster_info);
  auto route_set_info = cluster_info->SetInfos();
  int sets_num = route_set_info.size();
  assert(sets_num > 0);
  int items = (MAX_SHARD_NUM + sets_num - 1) / sets_num;
  int i = 0;
  for (const auto &item : route_set_info) {
    parts_str += "partition `p" + std::to_string(i) + "` values in (";
    parts_str += generate_list_items_str(items * i,
                                         std::min(items * (i + 1), MAX_SHARD_NUM));  
    parts_str += ") "; 
    parts_str += generate_part_comment_str(item.second);
    parts_str += " engine = innodb";
    if (i < sets_num - 1) parts_str += ",";
    i++;
  }
  return parts_str;
}

/*
 * create partittion by list sql string from mysql.servers route info
 */
bool spider_auto_partition::generate_patition_str() {
  // get the shard key
  if (get_shard_key_names()) return true;

  // use one field
  std::string first_name = shard_key_names[0];
  assert(first_name.size() > 0);

  partition_str = "partition by list(murmurHashCodeAndMod(`" + first_name + "`, ";
  partition_str += std::to_string(MAX_SHARD_NUM) + "))";
  partition_str += "( " + generate_list_parts_str() + " )";
  return false;
}

/*
 * use sql parser to create partition_info from fake sql string add
 * from route info and use the part info to create ha_spiderpart handler
 * see mysql_unpack_partition
 */
bool spider_auto_partition::create_part_info_from_str() {
  // Backup query arena
  Query_arena *backup_stmt_arena_ptr = ha_thd->stmt_arena;
  Query_arena backup_arena;
  Query_arena part_func_arena(ha_thd->mem_root, Query_arena::STMT_INITIALIZED);
  ha_thd->swap_query_arena(part_func_arena, &backup_arena);
  ha_thd->stmt_arena = &part_func_arena;

  //
  // Parsing the partition expression.
  //
  // Save old state and prepare new LEX
  const CHARSET_INFO *old_character_set_client =
      ha_thd->variables.character_set_client;
  ha_thd->variables.character_set_client = system_charset_info;
  LEX *old_lex = ha_thd->lex;
  LEX lex;
  Query_expression unit(CTX_NONE);
  Query_block select(ha_thd->mem_root, nullptr, nullptr);
  lex.new_static_query(&unit, &select);
  ha_thd->lex = &lex;

  sql_digest_state *parent_digest = ha_thd->m_digest;
  PSI_statement_locker *parent_locker = ha_thd->m_statement_psi;

  Partition_expr_parser_state parser_state;
  bool error = true;
  if ((error = parser_state.init(ha_thd, partition_str.c_str(), partition_str.size())))
    goto end;

  // Parse the string and filling the partition_info.
  ha_thd->m_digest = nullptr;
  ha_thd->m_statement_psi = nullptr;

  error = parse_sql(ha_thd, &parser_state, nullptr) ||
          parser_state.result->fix_parser_data(ha_thd);

  if (error == false) {
    spd_table->s->partition_info_str = strdup_root(ha_thd->mem_root, partition_str.c_str());
    spd_table->s->partition_info_str_len = partition_str.size();
    spd_table->s->m_part_info = parser_state.result;
  }

  ha_thd->m_digest = parent_digest;
  ha_thd->m_statement_psi = parent_locker;

end:
  // Free items from current arena.
  // ha_thd->free_items();

  // Retore the old lex.
  lex_end(ha_thd->lex);
  ha_thd->lex = old_lex;

  // Restore old arena.
  ha_thd->stmt_arena = backup_stmt_arena_ptr;
  ha_thd->swap_query_arena(backup_arena, &part_func_arena);
  ha_thd->variables.character_set_client = old_character_set_client;

  return (error); 
}

/* add partitions to create info and table_share and dd_table */
bool spider_auto_partition::add_partitions() {
  // get all sets info
  cluster_info = get_all_servers();
  if (!cluster_info) return true;
  auto route_set_info = cluster_info->RouteInfos();
  if (route_set_info.size() <= 1) return false; // no need partition

  if (generate_patition_str()) return true;

  if (create_part_info_from_str()) return true;
  return false;
}

/* update the parition info to dd storage */
bool spider_auto_partition::update_dd_table() {
  // add the partition info to dd::Table
  if (dd::fill_dd_partition_from_create_info(ha_thd, spd_table_def,
                                             spd_create_info,
                                             spd_create_info->spd_create_list,
                                             spd_table->part_info))
    return true;
  // restore the dd::Table
  dd::cache::Dictionary_client *client = dd::get_dd_client(ha_thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(client);

  if (client->update(spd_table_def)) return true;
  return false;
}
