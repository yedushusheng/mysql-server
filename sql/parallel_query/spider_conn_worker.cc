#include "sql/parallel_query/spider_conn_worker.h"

#include "my_inttypes.h"
#include "mysql.h"
#include "mysql_async.h"
#include "mysqld_error.h"
#include "parallel_query/executor.h"
#include "storage/spider/spd_db_include.h"
#include "storage/spider/spd_include.h"
#include "storage/spider/ha_spiderpart.h"
#include "storage/spider/spd_db_conn.h"
#include "storage/spider/spd_conn.h"

extern MYSQL *SPIDER_CONN_get_mysql(SPIDER_CONN *spider_conn);

namespace pq {
static SPIDER_CONN *GetSpiderConn(THD *thd, handler *file, uint partid) {
  auto *spiderpart = down_cast<ha_spiderpart *>(file);
  auto *spider_conn = spiderpart->get_spider_conn_for_part(partid);
  if (!spider_conn) return nullptr;

  spider_conn->thd = thd;
  spider_mta_conn_mutex_unlock(spider_conn);

  return spider_conn;
}

bool MySQLClientSpider::connect(THD *thd, WorkerShareState *share_state, TABLE *table) {
  uint *ppartid = static_cast<uint *>(share_state->data);
  if (!ppartid) {
    share_state->data = ppartid = new (thd->mem_root) uint;
    *ppartid = table->part_info->get_first_used_partition();
  } else {
    *ppartid = table->part_info->get_next_used_partition(*ppartid);
  }
  auto *spider_conn = GetSpiderConn(thd, table->file, *ppartid);
  if (!spider_conn) {
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0), "Fail to get SPIDER_CONN");
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
}
