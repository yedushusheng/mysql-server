/* Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

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

/* Defines to make different thread packages compatible */

#ifndef THREAD_TYPE_INCLUDED
#define THREAD_TYPE_INCLUDED

/**
  @file include/mysql/thread_type.h
*/

/* Note:在MySQL中，enum_thread_type是一个用于表示线程类型的枚举类型.
具体来说，`enum_thread_type` 定义了以下几种线程类型：
1. **后台任务线程（Background Task Threads）**
   - 这些线程负责执行一些后台任务，例如刷新查询缓存、清理临时表等。
2. **二进制日志线程（Binary Log Dump Threads）**
   - 这些线程负责处理二进制日志文件的传输，通常用于主从复制。
3. **连接处理线程（Connect Handling Threads）**
   - 这些线程负责处理客户端连接请求，接受新的连接，分配新的线程来处理连接上的查询。
4. **事件调度线程（Event Scheduler Threads）**
   - 用于执行 MySQL 事件调度器中定义的周期性任务。
5. **查询执行线程（Query Execution Threads）**
   - 这些线程负责执行客户端发送的查询语句。
6. **主线程（Main Thread）**
   - MySQL 服务器的主线程，负责初始化、启动其他线程，并处理一些全局性的任务。
通过查看线程的 `enum_thread_type` 类型，可以了解到每个线程在 MySQL 服务器中执行的特定任务。这对于诊断和监视 MySQL 服务器的运行状态非常有用。
请注意，具体的线程类型可能因 MySQL 版本和配置而异，因此在查看相关文档时请参考您使用的 MySQL 版本的文档。
*/
/* Flags for the THD::system_thread variable */
enum enum_thread_type {
  NON_SYSTEM_THREAD = 0,
  SYSTEM_THREAD_SLAVE_IO = 1,
  SYSTEM_THREAD_SLAVE_SQL = 2,
  SYSTEM_THREAD_NDBCLUSTER_BINLOG = 4,
  SYSTEM_THREAD_EVENT_SCHEDULER = 8,
  SYSTEM_THREAD_EVENT_WORKER = 16,
  SYSTEM_THREAD_INFO_REPOSITORY = 32,
  SYSTEM_THREAD_SLAVE_WORKER = 64,
  SYSTEM_THREAD_COMPRESS_GTID_TABLE = 128,
  SYSTEM_THREAD_BACKGROUND = 256,
  SYSTEM_THREAD_DD_INITIALIZE = 512,
  SYSTEM_THREAD_DD_RESTART = 1024,
  SYSTEM_THREAD_SERVER_INITIALIZE = 2048,
  SYSTEM_THREAD_INIT_FILE = 4096,
  SYSTEM_THREAD_SERVER_UPGRADE = 8192
};

#endif /* THREAD_TYPE_INCLUDED */
