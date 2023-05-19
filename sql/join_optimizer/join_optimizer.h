/* Copyright (c) 2020, Oracle and/or its affiliates.

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

#ifndef SQL_JOIN_OPTIMIZER_JOIN_OPTIMIZER_H
#define SQL_JOIN_OPTIMIZER_JOIN_OPTIMIZER_H

/**
  @file

  The hypergraph join optimizer takes a query block and decides how to
  execute it as fast as possible (within a given cost model), based on
  the idea of expressing the join relations as edges in a hypergraph.
  (See subgraph_enumeration.h for more details on the core algorithm,
  or FindBestQueryPlan() for more information on overall execution.)

  It is intended to eventually take over completely from the older join
  optimizer based on prefix search (sql_planner.cc and related code),
  but is currently in early alpha stage with a very simplistic cost model
  and a large number of limitations: The most notable ones are that
  we do not support:

    - Many SQL features: DISTINCT, recursive CTE, windowing functions,
      LATERAL, JSON_TABLE.
    - Secondary engine.
    - Hints.
    - TRADITIONAL and JSON formats for EXPLAIN (use FORMAT=tree).

  For unsupported queries, we will return an error; every valid SQL
  query should either give such an error a correct result set.

  There are also have many optimization features it does not yet support;
  among them:

    - Reordering of non-inner joins; outer joins work as an optimization
      barrier, pretty much like the existing join optimizer.
    - Indexes of any kind (and thus, no understanding of interesting
      orders); table scans only.
    - Nested loop joins; hash joins only.
    - Multiple equalities; they are simplified to simple equalities
      before optimization (so some legal join orderings will be missed).
    - Aggregation through a temporary table.
    - Queries with a very large amount of possible orderings, e.g. 30-way
      star joins. (Less extreme queries, such as 30-way chain joins,
      will be fine.) They will receive a similar error message as with
      unsupported SQL features, instead of timing out.
    - Differentiating between initial (startup) cost and full cost,
      which is important in queries with LIMIT.
 */

#include <string>

struct AccessPath;
class THD;
class SELECT_LEX;

/** Note:MySQL8.0优化器代价模型
 * Hyper Graph优化器
 * 在经典的volcano模型中,每个query会转成一颗树去执行,而join部分也会转为其中的子树.
 * 不同的join tree对应不同的执行顺序,经典的join tree可以分为三类:left-deep,zig-zag和bushy
 * 
 * 火山模型与AccessPath:
 * MySQL火山模型和AccessPath都是MySQL的查询优化器,在优化查询过程中发挥重要作用.
 * 它们的对比如下:
 * 1.原理不同:
 * 火山模型是一种基于代价的优化器,采用代价估算和代价比较的方式选择最优的查询计划,
 * 而AccessPath则是一种基于规则的优化器,采用一系列规则来选择最优的查询计划.
 * 2.查询优化效果不同:
 * 火山模型相对于AccessPath来说,能够更好地利用统计信息和硬件特性,选择和执行最优的查询计划,因此在复杂查询和大规模数据场景下,火山模型的优化效果更好.
 * 而在简单查询和小数据集场景下,AccessPath通常能够得到更好的查询性能.
 * 3.实现复杂度不同:
 * 火山模型的实现比AccessPath要复杂一些,需要进行代价估算、代价比较、多种查询计划的生成和选择等多个步骤,同时需要对硬件资源进行更充分的利用,因此开发和维护成本较高.
 * 可扩展性不同:由于火山模型采用了基于流水线的查询执行架构,可以更好地支持并行查询和分布式查询,同时具有更好的容错性,能够更好地应对故障和负载波动等情况.
 * 而AccessPath则相对较为简单,可扩展性较差.
 * 总的来说,火山模型和AccessPath各有优劣,用户需要根据自己的实际业务场景来选择合适的优化器,以达到最优的查询性能和效果.
 * 
 * 在MySQL中,AccessPath优化器主要是用于优化查询语句的执行计划,提高查询性能.
 * AccessPath优化器会在执行查询语句时,根据表的索引情况、统计信息以及查询条件等因素,选择最优的索引或者索引组合,来加速查询过程.
 * AccessPath优化器会对查询语句进行解析、重写、优化和执行四个步骤.
 * 其中,解析是将查询语句转换成一个内部数据结构;
 * 重写是对查询语句进行语义等价的改写;
 * 优化是选择最优的执行计划;
 * 而执行则是按照执行计划执行查询语句.
 * 在优化阶段,AccessPath优化器主要有以下几个任务:
 * 1.确定查询的执行方式:
 * 例如,是使用全表扫描还是使用索引扫描,是使用单表查询还是使用多表关联查询等.
 * 2.选择最优的索引:
 * 如果有多个索引可供选择,优化器会选择最优的索引或索引组合,以便尽可能地减少查询所需的I/O操作和CPU计算.
 * 3.生成执行计划:
 * AccessPath优化器会根据选择的执行方式和索引,生成一个最优的执行计划,以便在执行阶段快速地获取查询结果.
 * AccessPath优化器的优化过程是一个复杂的决策过程,需要考虑到很多因素,例如数据分布、数据量、索引情况、查询语句等.
 * MySQL提供了一系列的工具和参数,来帮助用户理解和优化查询执行计划,例如EXPLAIN语句、慢查询日志、索引提示等.
*/
/**
  The main entry point for the hypergraph join optimizer; takes in a query
  block and returns an access path to execute it (or nullptr, for error).
  It works as follows:

    1. Convert the query block from MySQL's TABLE_LIST structures into
       a hypergraph (see make_join_hypergraph.h).
    2. Find all legal subplans in the hypergraph, calculate costs for
       them and create access paths -- if there are multiple ways to make a
       given subplan (e.g. multiple join types, or joining {t1,t2,t3} can be
       made through either {t1}-{t2,t3} or {t1,t2}-{t3}), keep only the cheapest
       one. Filter predicates (from WHERE and pushed-down join conditions)
       are added as soon down as it is legal, which is usually (but not
       universally) optimal. The algorithm works so that we always see smaller
       subplans first and then end at the complete join plan containing all the
       tables in the query block.
    3. Add an access path for non-pushable filter predicates.
    4. Add extra access paths for operations done after the joining,
       such as ORDER BY, GROUP BY, LIMIT, etc..
    5. Make access paths for the filters in nodes made by #2
       (see ExpandFilterAccessPaths()).

  @param thd Thread handle.
  @param select_lex The query block to find a plan for.
  @param trace If not nullptr, will be filled with human-readable optimizer
    trace showing some of the inner workings of the code.
 */
AccessPath *FindBestQueryPlan(THD *thd, SELECT_LEX *select_lex,
                              std::string *trace);

#endif  // SQL_JOIN_OPTIMIZER_JOIN_OPTIMIZER_H
