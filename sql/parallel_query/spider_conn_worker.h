#ifndef PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#define PARALLEL_QUERY_SPIDER_CONN_WORKER_H
#include "sql/parallel_query/mysqlclient_worker.h"

class st_spider_conn;
typedef st_spider_conn SPIDER_CONN;
namespace pq {
class MySQLClientSpider : public MySQLClientSync {
 public:
  MySQLClientSpider(TABLE *spider_table, uint part_index)
      : m_spider_table(spider_table), m_part_index(part_index) {}
  bool connect(THD *thd) override;
  bool send_query(const char *query, ulong length,
                  bool *send_complete) override;
  void terminate(THD *thd) override;

 private:
  SPIDER_CONN *m_spider_conn{nullptr};
  TABLE *m_spider_table;
  uint m_part_index;
};
}  // namespace pq
#endif
