#ifndef PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#define PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#include "sql/parallel_query/mysqlclient_worker.h"

namespace pq {
class MySQLClientSpider : public MySQLClientSync {
 public:
  MySQLClientSpider(SPIDER_CONN *conn) : m_spider_conn(conn) {}
  bool connect(THD *thd) override;
  bool send_query(const char *query, ulong length,
                  bool *send_complete) override;
  void terminate(THD *thd) override;

 private:
  SPIDER_CONN *m_spider_conn;
};
}  // namespace pq
#endif
