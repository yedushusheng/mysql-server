#ifndef PARALLEL_QUERY_MYSQLCLIENT_WORKER_H
#define PARALLEL_QUERY_MYSQLCLIENT_WORKER_H
#include <poll.h>
#include <string>
#include "mysql.h"  // MYSQL

class THD;
class TABLE;
class String;

namespace pq {
namespace comm {
class Event;
}
class PlanDeparser;

class MySQLClient {
 public:
  virtual ~MySQLClient() {}
  virtual bool connect(THD *thd) = 0;
  virtual bool send_query(const char *query, ulong length,
                          bool *send_complete) = 0;
  virtual bool fetch_row(bool *fetch_complete) = 0;
  virtual void terminate(THD *thd) = 0;
  virtual bool use_result();
  MYSQL *mysql() const { return m_mysql; }
  MYSQL_ROW take_row() {
    auto row = m_row;
    m_row = nullptr;
    return row;
  }
  bool has_row() { return m_row != nullptr; }
  void reset_row() { m_row = nullptr; }
  MYSQL_RES *result() { return m_result; }

 protected:
  void report_error();
  void free_result();

  MYSQL *m_mysql{nullptr};
  MYSQL_RES *m_result{nullptr};
  /// The m_row is reset after it is consumed.
  MYSQL_ROW m_row{nullptr};
};

class MySQLClientSync : public MySQLClient {
 public:
  ~MySQLClientSync();
  bool connect(THD *thd) override;
  bool send_query(const char *query, ulong length,
                  bool *send_complete) override;
  bool fetch_row(bool *fetch_complete) override;
  // Not implemented yet
  void terminate(THD *) override {}
  bool use_result() override {
    if (MySQLClient::use_result()) return true;
    return false;
  }
  virtual bool do_send_query(const char *query, ulong length);

  bool is_readable();

  void close() {
    free_result();
    if (m_owned_mysql) mysql_close(m_mysql);
  }

 protected:
  struct pollfd m_pollfd;
  bool m_owned_mysql{true};
  bool m_query_sent{false};
};

class MySQLClientAsync : public MySQLClient {
 public:
  ~MySQLClientAsync();
  bool connect(THD *thd) override;
  bool send_query(const char *query, ulong length,
                  bool *send_complete) override;
  bool fetch_row(bool *fetch_complete) override;
  // Not implemented yet
  void terminate(THD *) override {}
};

// Share between worker and row channel
class MySQLClientQueryExec {
 public:
  enum class Stage { None, Connected, QuerySending, RowReading, Done };
  MySQLClientQueryExec(PlanDeparser *deparser, TABLE *collector_table);
  ~MySQLClientQueryExec();
  bool Init(THD *thd, TABLE *parallel_table, uint worker_id);
  bool ExecuteQuery();
  /// *nbytesp == 0 means rows read out, *datap = nullptr means WOULD_BLOCK.
  bool ReadNextRow(std::size_t *nbytesp, void **datap, bool nowait);
  bool IsFinished() const { return m_error || m_stage == Stage::Done; }
  /// Drain out rows in a non-block mode, @return true if out of rows
  bool DrainOutRows();
  void Terminate(THD *thd, comm::Event *state_event);
  bool IsConnected() const { return m_stage != Stage::None; }
  bool IsQuerySent() const { return m_stage >= Stage::QuerySending; }
  bool AddSocketToEventService(comm::Event *event);
  void RemoveSocketFromEventService();

 private:
  bool Exec(bool nowait);
  bool SendQuery(bool nowait);
  bool ReadRow(bool nowait);

  MySQLClient *mysql{nullptr};
  Stage m_stage{Stage::None};

  // This is collector table, Data that read from mysql client will be
  // filled to its record[0].
  TABLE *m_table;
  bool m_error{false};

  PlanDeparser *m_deparser;
  bool added_to_event_service{false};
};
}  // namespace pq

#endif //PARALLEL_QUERY_MYSQLCLIENT_WORKER_H
