#include "sql/parallel_query/mysqlclient_worker.h"
#include <sys/poll.h>

#include "my_inttypes.h"
#include "mysql.h"
#include "mysql/plugin_auth_common.h"
#include "mysql_async.h"
#include "mysqld_error.h"
#include "sql/enum_query_type.h"
#include "sql/field.h"
#include "sql/item_sum.h"
#include "sql/my_decimal.h"
#include "sql/mysqld.h"
#include "sql/parallel_query/event_service.h"
#include "sql/parallel_query/plan_deparser.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/worker.h"
#include "sql/parallel_query/spider_conn_worker.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql_common.h"

namespace pq {

void MySQLClient::report_error() {
  if (m_mysql)
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0), mysql_error(m_mysql));
  else
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
             "error in mysql_init()");
}

bool MySQLClient::use_result() {
  if (!(m_result = (*m_mysql->methods->use_result)(m_mysql))) {
    report_error();
    return true;
  }

  return false;
}

void MySQLClient::free_result() {
  mysql_free_result(m_result);
  m_result = nullptr;
}

MySQLClientSync::~MySQLClientSync() { close(); }

bool MySQLClientSync::connect(THD *, WorkerShareState *, TABLE *) {
  if (!(m_mysql = mysql_init(nullptr))) {
    report_error();
    return true;
  }

  if (mysql_real_connect(m_mysql, "127.0.0.1", "root", "", "test", mysqld_port,
                         nullptr, 0) == nullptr) {
    report_error();
    return true;
  }

  m_pollfd.fd = mysql_get_socket_descriptor(m_mysql);
  m_pollfd.events = POLLIN;

  return false;
}

bool MySQLClientSync::is_readable() {
  auto *vio = m_mysql->net.vio;
  if ((vio->has_data(vio))) return true;
  if (poll(&m_pollfd, 1, 0) == 1) {
    assert(m_pollfd.revents & POLLIN);
    return true;
  }
  return false;
}

bool MySQLClientSync::do_send_query(const char *query, ulong length) {
  return mysql_send_query(m_mysql, query, length) != 0;
}

bool MySQLClientSync::send_query(const char *query, ulong length,
                                 bool *send_complete) {
  if (!m_query_sent) {
    if (do_send_query(query, length)) {
      report_error();
      return true;
    }

    m_query_sent = true;
  }

  // Its in wait mode if send_complete is null
  if (send_complete && !is_readable()) return false;

  if (m_mysql->methods->read_query_result(m_mysql) != 0) {
    report_error();
    return true;
  }

  if (send_complete) *send_complete = true;

  return false;
}

bool MySQLClientSync::fetch_row(bool *fetch_complete) {
  // fetch_complete is null means it's in wait mode
  assert(!fetch_complete || !(*fetch_complete));

  // Row can be fetched in Worker::Start() then this function is called
  // again in ReadNextRow(), so just return that row.
  if (m_row) {
    if (fetch_complete) *fetch_complete = true;
    return false;
  }

  if (fetch_complete && !is_readable()) return false;


  m_row = mysql_fetch_row(m_result);

  if (!m_row && mysql_errno(m_mysql) != 0) {
    report_error();
    return true;
  }

  if (fetch_complete) *fetch_complete = true;

  return false;
}


MySQLClientAsync::~MySQLClientAsync() {
  free_result();
  mysql_close(m_mysql);
}

bool MySQLClientAsync::connect(THD *,WorkerShareState *, TABLE *) {
  if (!(m_mysql = mysql_init(nullptr))) {
    report_error();
    return true;
  }

  net_async_status status;
  while ((status = mysql_real_connect_nonblocking(
              m_mysql, "127.0.0.1", "root", "", "test", mysqld_port, nullptr,
              0)) == NET_ASYNC_NOT_READY)
    ;
  if (status == NET_ASYNC_ERROR) {
    report_error();
    return true;
  }

  return false;
}

bool MySQLClientAsync::send_query(const char *query, ulong length,
                                  bool *send_complete) {
  auto status = mysql_real_query_nonblocking(m_mysql, query, length);

  while (!send_complete && status == NET_ASYNC_NOT_READY)
    status = mysql_real_query_nonblocking(m_mysql, query, length);

  if (status == NET_ASYNC_ERROR) {
    report_error();
    return true;
  }

  if (send_complete && status == NET_ASYNC_COMPLETE) *send_complete = true;

  return false;
}

bool MySQLClientAsync::fetch_row(bool *fetch_complete) {
  auto status = mysql_fetch_row_nonblocking(m_result, &m_row);
  while (!fetch_complete && status == NET_ASYNC_NOT_READY)
    status = mysql_fetch_row_nonblocking(m_result, &m_row);

  if (status == NET_ASYNC_ERROR) {
    report_error();
    return true;
  }

  if (fetch_complete && status == NET_ASYNC_COMPLETE) *fetch_complete = true;
  if (!m_row && mysql_errno(m_mysql) != 0) {
    report_error();
    return true;
  }

  return false;
}

MySQLClientQueryExec::MySQLClientQueryExec(PlanDeparser *deparser, TABLE *table)
    : m_table(table), m_deparser(deparser) {
    mysql = new MySQLClientSpider;
 }

MySQLClientQueryExec::~MySQLClientQueryExec() {
  if (added_to_event_service) RemoveSocketFromEventService();
  delete mysql;
}

bool MySQLClientQueryExec::Init(THD *thd, TABLE *spider_table, WorkerShareState *share_state) {
  if (mysql->connect(thd, share_state, spider_table)) {
    m_error = true;
    return true;
  }

  m_stage = Stage::Connected;

  return false;
}

bool MySQLClientQueryExec::AddSocketToEventService(comm::Event *event) {
  if (added_to_event_service &&
      comm::EventServiceRemoveFd(mysql_get_socket_descriptor(mysql->mysql())))
    return true;
  added_to_event_service = false;
  if (comm::EventServiceAddFd(mysql_get_socket_descriptor(mysql->mysql()), event,
                              true))
    return true;
  added_to_event_service = true;
  return false;
}

void MySQLClientQueryExec::RemoveSocketFromEventService() {
  assert(added_to_event_service);
  comm::EventServiceRemoveFd(mysql_get_socket_descriptor(mysql->mysql()));
  added_to_event_service = false;
}

void MySQLClientQueryExec::Terminate(THD *thd, comm::Event *state_event) {
  if (m_stage == Stage::QuerySending || m_stage == Stage::RowReading) {
    AddSocketToEventService(state_event);
    mysql->terminate(thd);
  }
}

bool MySQLClientQueryExec::SendQuery(bool nowait) {
  assert(m_stage == Stage::QuerySending);
  auto *statement = m_deparser->statement();

  bool send_complete = !nowait;
  if (mysql->send_query(statement->ptr(), statement->length(),
                       nowait ? &send_complete : nullptr)) {
    m_error = true;
    return true;
  }

  assert(nowait || send_complete);

  if (!send_complete) return false;

  if (mysql->use_result()) {
    m_error = true;
    return true;
  }

  m_stage = Stage::RowReading;

  return false;
}

bool MySQLClientQueryExec::ReadRow(bool nowait) {
  assert(m_stage == Stage::RowReading);
  bool fetch_complete = !nowait;

  if (mysql->fetch_row(nowait ? &fetch_complete : nullptr)) {
    m_error = true;
    return true;
  }

  assert(nowait || fetch_complete);

  if (fetch_complete && !mysql->row()) m_stage = Stage::Done;

  return false;
}

bool MySQLClientQueryExec::ResetRow() {
  if (mysql->row() == nullptr) return false;

  mysql->reset_row();

  return true;
}

static double row_field_to_double(char *from, size_t len,
                                  const CHARSET_INFO *cs) {
  int conv_error;
  const char *end;
  double nr = my_strntod(cs, from, len, &end, &conv_error);
  assert(conv_error == 0);
  return nr;
}

static ulonglong row_field_to_longlong(char *from, size_t len,
                                       const CHARSET_INFO *cs) {
  const char *end;
  ulonglong tmp;
  int conv_err = 0;
  tmp = cs->cset->strntoull10rnd(cs, from, len, false, &end, &conv_err);
  assert(conv_err == 0);
  return tmp;
}

bool MySQLClientQueryExec::ReadNextRow(std::size_t *nbytesp, void **datap,
                                       bool nowait) {
  if (Exec(nowait)) return true;

  if (m_stage == Stage::Done) {
    *nbytesp = 0;
    return false;
  }

  *nbytesp = m_table->s->reclength;
  auto row = mysql->row();

  if (!row) {
    *datap = nullptr;  // would block;
    return false;
  }

  auto result = mysql->result();
  auto *lengths = mysql_fetch_lengths(result);
  if (m_deparser->CountAppended() &&
      **(row + mysql_num_fields(result) - 1) == '0') {
    *nbytesp = 0;
    ResetRow();
    return false;
  }

  for (uint i = 0; i < m_table->s->fields; i++) {
    auto *to = m_table->field[i];
    auto *defld = m_deparser->DeparseField(i);
    auto findex = defld->source_field_index;
    if (!*(row + findex)) {
      to->set_null();
      continue;
    }
    assert(defld->m_type != DeField::COUNT);
    if (defld->m_type == DeField::NORMAL_ITEM) {
      to->store(*(row + findex), lengths[findex], &my_charset_bin);
      to->set_notnull();
      continue;
    }
    assert(defld->m_type == DeField::AVG);
    auto *item = down_cast<Item_sum_avg *>(defld->item);
    uchar *res = to->field_ptr();
    auto cindex = defld->source_field_aux_index;
    ulonglong count = row_field_to_longlong(*(row + cindex), lengths[cindex],
                                            &my_charset_bin);
    if (item->result_type() == DECIMAL_RESULT) {
      my_decimal dec;
      str2my_decimal(E_DEC_FATAL_ERROR, *(row + findex), lengths[findex],
                     &my_charset_bin, &dec);
      my_decimal2binary(E_DEC_FATAL_ERROR, &dec, res, item->f_precision,
                        item->f_scale);
      res += item->dec_bin_size;
    } else {
      double sum = row_field_to_double(*(row + findex), lengths[findex],
                                       &my_charset_bin);
      float8store(res, sum);
      res += sizeof(double);
    }
    int8store(res, count);
  }

  *datap = m_table->record[0];

  ResetRow();

  return false;
}

bool MySQLClientQueryExec::Exec(bool nowait) {
  switch (m_stage) {
    case Stage::None:
      assert(0);
      break;
    case Stage::Connected:
      m_stage = Stage::QuerySending;
      [[fallthrough]];
    case Stage::QuerySending:
      if (SendQuery(nowait)) return true;
      if (m_stage == Stage::QuerySending) break;
      [[fallthrough]];
    case Stage::RowReading:
      if (ReadRow(nowait)) return true;
      break;
    case Stage::Done:
      break;
  }

  return false;
}

namespace comm {

class MySQLClientChannel : public RowChannel {
 public:
  explicit MySQLClientChannel(MySQLClientQueryExec *query_exec)
      : m_query_exec(query_exec) {}

  bool Init(THD *, Event *event, bool receiver [[maybe_unused]]) override {
    assert(receiver);
    return m_query_exec->AddSocketToEventService(event);
  }

  Result Send(std::size_t, const void *, bool) override {
    assert(false);
    return Result::SUCCESS;
  }

  Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                 MessageBuffer *) override {
    if (m_query_exec->ReadNextRow(nbytesp, datap, nowait)) return Result::ERROR;
    if (*nbytesp == 0) return Result::DETACHED;
    if (!*datap) return Result::WOULD_BLOCK;
    return Result::SUCCESS;
  }

  Result SendEOF() override {
    assert(false);
    assert(!IsClosed());
    return Result::SUCCESS;
  }

  void Close() override {
    m_query_exec->RemoveSocketFromEventService();
    m_closed = true;
  }

  bool IsClosed() const override { return m_closed; }

 private:
  MySQLClientQueryExec *m_query_exec;
  bool m_closed{false};
};
}  // namespace comm

/**
  Provide a mysql client based remote worker
*/
class MySQLClientWorker : public Worker {
 public:
  MySQLClientWorker(uint id, comm::Event *state_event, THD *thd,
                    PartialPlan *partial_plan, WorkerShareState *share_state,
                    TABLE *m_collector_table)
      : Worker(id, state_event),
        m_query_executor(partial_plan->Deparser(), m_collector_table),
        m_share_state(share_state),
        m_parallel_table(partial_plan->GetParallelScanInfo().table),
        m_thd(thd) {}

  /// The receiver row channel created inside of worker, @param comm_event
  /// is for receiver waiting.
  bool Init(comm::Event *comm_event) override;
  bool Start() override;
  void Terminate() override;
  Diagnostics_area *stmt_da(bool, ha_rows *, ha_rows *) override {
    return nullptr;
  }
  bool IsRunning() override;
  bool IsStartFailed() override { return !m_query_executor.IsConnected(); }

  std::string *QueryPlanTimingData() override { return nullptr; }
  void CollectStatusVars(THD *) override {}

 private:
  MySQLClientQueryExec m_query_executor;
  WorkerShareState *m_share_state;
  TABLE *m_parallel_table;
  THD *m_thd;
};

bool MySQLClientWorker::Init(comm::Event *comm_event) {
  if (m_query_executor.Init(m_thd, m_parallel_table, m_share_state)) return true;

  if (!(m_receiver_channel = new (m_thd->mem_root)
            comm::MySQLClientChannel(&m_query_executor)) ||
      m_receiver_channel->Init(m_thd, comm_event, true))
    return true;
  return false;
}

bool MySQLClientWorker::Start() { return m_query_executor.Exec(true); }

void MySQLClientWorker::Terminate() {
  m_query_executor.Terminate(m_thd, m_state_event);
}

bool MySQLClientWorker::IsRunning() {
  if (!m_query_executor.IsConnected() || !m_query_executor.IsQuerySent() ||
      m_query_executor.IsFinished())
    return false;

  // Could be LIMIT and roll stage to finish, drain out data
  while (true) {
    if (m_query_executor.Exec(true) || m_query_executor.IsFinished())
      return false;
    // The socket would be block if it is not finished and ResetRow() return
    // false.
    if (!m_query_executor.ResetRow()) return true;
  }

  return !m_query_executor.IsFinished();
}

Worker *CreateMySQLClientWorker(uint id, comm::Event *state_event, THD *thd,
                                PartialPlan *partial_plan,
                                WorkerShareState *worker_share_state,
                                TABLE *collector_table) {
  return new (thd->mem_root) MySQLClientWorker(
      id, state_event, thd, partial_plan, worker_share_state, collector_table);
}
}  // namespace pq
