#ifndef PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#define PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#include "sql/parallel_query/mysqlclient_worker.h"

class st_spider_conn;
typedef st_spider_conn SPIDER_CONN;
namespace pq {
  class MySQLClientSpider : public MySQLClientSync {
  public:
    bool connect(THD *thd, WorkerShareState *share_state, TABLE *table) override;
    void terminate(THD *thd) override;

  private:
    SPIDER_CONN *m_spider_conn{nullptr};
  };
}
#endif
