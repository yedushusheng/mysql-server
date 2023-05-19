#ifndef CONNECTION_HANDLER_INCLUDED
#define CONNECTION_HANDLER_INCLUDED

/* Copyright (c) 2007, 2017, Oracle and/or its affiliates. All rights reserved.

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

#include "my_inttypes.h"

class THD;
class Channel_info;
class Connection_handler_manager;
/** Note:连接与线程管理
 * https://zhuanlan.zhihu.com/p/114343815
 * 
 * Connection_handler_manager:单例类,用来管理连接处理器;
 * Connection_handlerv连接处理的抽象类,具体实现由其子类实现;
 * Per_thread_connection_handler:继承了Connection_handler,每一个连接用单独的线程处理,默认为该实现,通过thread_handling参数可以设置;
 * One_thread_connection_handler:继承了Connection_handler,所有连接用同一个线程处理;
 * Plugin_connection_handler:继承了Connection_handler,支持由Plugin具体实现的handler,例如线程池;
 * Connection_acceptor:一个模版类,以模版方式支持不同的监听实现(三个listener).并且提供一个死循环,用以监听连接;
 * Mysqld_socket_listener:实现以Socket的方式监听客户端的连接事件,并且支持TCP socket和Unix socket,即对应的TCP_socket和Unix_socket;
 * Shared_mem_listener:通过共享内存的监听方式;
 * Named_pipe_listener:通过命名管道来监听和接收客户请求;
 * Channel_info:连接信道的抽象类,具体实现有Channel_info_local_socket和Channel_info_tcpip_socket,Channel_info_local_socket:与本地方式与服务器进行交互;
 * Channel_info_tcpip_socket:以TCP/IP方式与服务器进行交互;
 * TCP_socket:TCP socket,与MySQL服务不同机器的连接访问;
 * Unix_socket:Unix socket,与MySQL服务相同主机的连接访问;
 * 
 * 说明:
 * 这里是连接与线程管理相关的接口,vio是socket的封装,二者不同
*/
/**
  This abstract base class represents how connections are processed,
  most importantly how they map to OS threads.
*/
class Connection_handler {
 protected:
  friend class Connection_handler_manager;

  Connection_handler() {}
  virtual ~Connection_handler() {}

  /**
    Add a connection.

    @param channel_info     Pointer to the Channel_info object.

    @note If this function is successful (returns false), the ownership of
    channel_info is transferred. Otherwise the caller is responsible for
    its destruction.

    @return true if processing of the new connection failed, false otherwise.
  */
  virtual bool add_connection(Channel_info *channel_info) = 0;

  /**
    @return Maximum number of threads that can be created
            by this connection handler.
  */
  virtual uint get_max_threads() const = 0;
};

#endif  // CONNECTION_HANDLER_INCLUDED
