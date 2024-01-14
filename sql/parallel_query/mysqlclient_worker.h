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
class WorkerShareState;

class MySQLClient {
 public:
  virtual ~MySQLClient() {}
  virtual bool connect(THD *thd, WorkerShareState *share_state,
                       TABLE *table) = 0;
  virtual bool send_query(const char *query, ulong length,
                          bool *send_complete) = 0;
  virtual bool fetch_row(bool *fetch_complete) = 0;
  virtual void terminate(THD *thd) = 0;
  virtual bool use_result();
  MYSQL *mysql() const { return m_mysql; }
  MYSQL_ROW row() { return m_row; }
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
  bool connect(THD *thd, WorkerShareState *share_state, TABLE *table) override;
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
  bool connect(THD *thd, WorkerShareState *share_state, TABLE *table) override;
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
  MySQLClientQueryExec(PlanDeparser *deparser, TABLE *table);
  ~MySQLClientQueryExec();
  bool Init(THD *thd, TABLE *spider_table, WorkerShareState *m_share_state);
  bool SendQuery(bool nowait);
  bool ReadRow(bool nowait);
  /// Return true if current row is not nullptr (that means it is actually
  /// reset) .
  bool ResetRow();
  bool Exec(bool nowait);
  bool IsFinished() const { return m_error || m_stage == Stage::Done; }
  /// *nbytesp == 0 means rows read out, *datap = nullptr means WOULD_BLOCK.
  bool ReadNextRow(std::size_t *nbytesp, void **datap, bool nowait);
  void Terminate(THD *thd, comm::Event *state_event);
  bool IsConnected() const { return m_stage != Stage::None; }
  bool IsQuerySent() const { return m_stage >= Stage::QuerySending; }
  bool AddSocketToEventService(comm::Event *event);
  void RemoveSocketFromEventService();

private:
  MySQLClient* mysql;
  Stage m_stage{Stage::None};

  TABLE *m_table;
  bool m_error{false};

  PlanDeparser *m_deparser;
  bool added_to_event_service{false};
};
}  // namespace pq

#endif //PARALLEL_QUERY_MYSQLCLIENT_WORKER_H
