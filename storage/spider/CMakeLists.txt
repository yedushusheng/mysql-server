#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHAVE_HANDLERSOCKET")
#SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DHAVE_HANDLERSOCKET")

IF(HAVE_WVLA)
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-vla")
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wno-vla")
ENDIF()

IF(MSVC)
  # Temporarily disable "conversion from size_t .."
  IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4267")
  ENDIF()
ENDIF()

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/sql/tdsql
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/alloc
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/time
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/config
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/json
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/container
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/string
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/interval
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/log
  ${CMAKE_SOURCE_DIR}/sql/tdsql/common/zookeeper_acl
  ${CMAKE_SOURCE_DIR}/sql/tdsql/ddl
  ${CMAKE_SOURCE_DIR}/sql/tdsql/meta_data
  ${CMAKE_SOURCE_DIR}/sql/tdsql/rpc
  ${CMAKE_SOURCE_DIR}/extra/libevent/libevent-2.1.11-stable/include
  ${PROJECT_BINARY_DIR}/extra/libevent/libevent-2.1.11-stable/include)

SET(SPIDER_SOURCES
  spd_param.cc spd_sys_table.cc spd_trx.cc spd_db_conn.cc spd_conn.cc
  spd_table.cc spd_direct_sql.cc spd_ping_table.cc
  spd_copy_tables.cc spd_i_s.cc spd_malloc.cc ha_spider.cc
  spd_db_mysql.cc spd_group_by_handler.cc ../../sql/partitioning/partition_base.cc ha_spiderpart.cc
  hs_client/config.cpp hs_client/escape.cpp hs_client/fatal.cpp
  hs_client/hstcpcli.cpp hs_client/socket.cpp hs_client/string_util.cpp
  spd_auto_partition.cc
)

SET(SPIDER_SOURCES_UDF
    spd_udf.cc spd_udf.def
)

IF(EXISTS ${PROJECT_SOURCE_DIR}/storage/mysql_storage_engine.cmake)
  SET(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DUSE_SYMDIR /Zi")
    #"${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DUSE_SYMDIR /Zi")
  SET(CMAKE_C_FLAGS_DEBUG
    "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DUSE_SYMDIR /Zi")
    #"${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DUSE_SYMDIR /Zi")
  SET(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /MAP /MAPINFO:EXPORTS")
  INCLUDE("${PROJECT_SOURCE_DIR}/storage/mysql_storage_engine.cmake")

  INCLUDE_DIRECTORIES(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/sql
    ${CMAKE_SOURCE_DIR}/regex)

  MYSQL_STORAGE_ENGINE(SPIDER)
ELSEIF(PLUGIN_PARTITION MATCHES "^NO$")
  MESSAGE(STATUS "Spider is skipped because partitioning is disabled")
ELSE()
  INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/spider/hs_client)

INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})

 MYSQL_ADD_PLUGIN(tdsql_udf ${SPIDER_SOURCES_UDF} STORAGE_ENGINE MODULE_OUTPUT_NAME "tdsql_udf")
 MYSQL_ADD_PLUGIN(tdsql ${SPIDER_SOURCES} STORAGE_ENGINE DEFAULT STATIC_ONLY)
ENDIF()

# off the warnings temporarily
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -W")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -W")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W")

IF(MSVC AND 0)
  IF (CMAKE_BUILD_TYPE STREQUAL "Debug")
    ADD_CUSTOM_COMMAND(TARGET tdsql
                       POST_BUILD
                       COMMAND if not exist ..\\..\\sql\\lib mkdir ..\\..\\sql\\lib\\plugin
                       COMMAND copy Debug\\ha_spider.dll ..\\..\\sql\\lib\\plugin\\ha_spider.dll)
  ENDIF()
ENDIF()
