#include "sql/parallel_query/spider_conn_worker.h"

#include "include/sql_common.h"
#include "my_inttypes.h"
#include "mysql.h"
#include "mysql_async.h"
#include "mysqld_error.h"
#include "parallel_query/executor.h"
#include "storage/spider/spd_db_include.h"
#include "storage/spider/spd_include.h"
#include "storage/spider/ha_spider.h"
#include "storage/spider/spd_db_conn.h"
#include "storage/spider/ha_spiderpart.h"
#include "storage/spider/spd_conn.h"

extern MYSQL *SPIDER_CONN_get_mysql(SPIDER_CONN *spider_conn);
extern int spider_db_query(SPIDER_CONN *conn, const char *query, uint length,
                           int quick_mode, int *need_mon);

namespace pq {
static constexpr uint spider_non_shardid = UINT_MAX;

SPIDER_CONN *GetSpiderConn(THD *thd, TABLE *table, uint shardid) {
  SPIDER_CONN *spider_conn =
      shardid == spider_non_shardid
          ? down_cast<ha_spider *>(table->file)->spider_get_conn_by_idx(0)
          : down_cast<ha_spiderpart *>(table->file)
                ->get_spider_conn_for_part(shardid);

  if (!spider_conn) return spider_conn;

  spider_conn->thd = thd;
  spider_mta_conn_mutex_unlock(spider_conn);

  return spider_conn;
}

static bool conn_choose_by_part_info(TABLE *table) {
  auto table_type = table->s->tdsql_table_type;
  return table_type != tdsql::ddl::TD_NOSHARD_TABLE &&
         table_type != tdsql::ddl::TD_ALLSET_TABLE;
}

bool MySQLClientSpider::connect(THD *thd) {
  uint part_id;
  // For the spider table without shardkey or with all set, it only needs to
  // get the connection to the first set. For the spider table with
  // shardkey, there is one more partitions.
  if (conn_choose_by_part_info(m_spider_table)) {
    auto *part_info = m_spider_table->part_info;
    part_id = part_info->get_first_used_partition();
    uint part_index = m_part_index;
    while (part_index-- > 0) {
      part_id = part_info->get_next_used_partition(part_id);
    }
  } else
    part_id = spider_non_shardid;

  auto *spider_conn = GetSpiderConn(thd, m_spider_table, part_id);
  if (!spider_conn) {
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
             "Fail to get SPIDER_CONN");
    return true;
  }

  m_spider_conn = spider_conn;

  m_mysql = SPIDER_CONN_get_mysql(m_spider_conn);

  assert(m_mysql);
  m_owned_mysql = false;

  m_pollfd.fd = mysql_get_socket_descriptor(m_mysql);
  m_pollfd.events = POLLIN;
  return false;
}

void MySQLClientSpider::terminate(THD *thd) {
  if (!m_query_sent) return;

  // If killed is set, spider should do the dirty work for us in transaction
  // rollback. but it does not work because of some issues. e.g.
  // spider_send_kill() refuses to connect foreign server if it found killed
  // is set.
  if (!thd->killed) return;
  auto old_killed = thd->killed.load();
  thd->killed = THD::NOT_KILLED;
  spider_send_kill(m_spider_conn, thd->killed);
  thd->killed = old_killed;
}

bool MySQLClientSpider::send_query(const char *query, ulong length,
                                   bool *send_complete) {
  if (!m_query_sent) {
    int error_num = 0;
    int need_mon = 0;
    if ((error_num = spider_db_query(m_spider_conn, query, length, false,
                                     &need_mon, false))) {
      return true;
    }

    m_query_sent = true;
  }

  // It's in wait mode if send_complete is null
  if (send_complete && !is_readable()) return false;

  // Read the query result if have received.
  if (m_mysql->methods->read_query_result(m_mysql) != 0) {
    report_error();
    return true;
  }

  if (send_complete) *send_complete = true;

  return false;
}
}  // namespace pq
