#include "sql/parallel_query/worker.h"
#include "sql/mysqld.h"
#include "sql/parallel_query/message_queue.h"
#include "sql/parallel_query/planner.h"
#include "sql/query_options.h"
#include "sql/query_result.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"
#include "sql/transaction.h"
#include "sql/item.h"

// A helper function of worker thread enter entry
static void *launch_worker_thread_handle(void *arg) {
  pq::Worker *worker = static_cast<pq::Worker *>(arg);
  worker->ThreadMainEntry();
  return nullptr;
}

namespace pq {
constexpr uint message_queue_ring_size = 65536;

class Query_result_to_collector : public Query_result_interceptor {
 private:
  RowExchangeWriter *m_row_exchange_writer;
  Temp_table_param *m_tmp_table_param;
  TABLE *m_table{nullptr};

 public:
  Query_result_to_collector(RowExchangeWriter *row_exchange_writer,
                            Temp_table_param *tmp_table_param)
      : m_row_exchange_writer(row_exchange_writer),
        m_tmp_table_param(tmp_table_param) {}

  void cleanup(THD *thd) override {
    Query_result_interceptor::cleanup(thd);
    m_row_exchange_writer->WriteEOF();
    if (m_table) {
      close_tmp_table(m_table);
      free_tmp_table(m_table);
      m_table = nullptr;
    }
  }

  bool prepare(THD *thd, const mem_root_deque<Item *> &items,
               Query_expression *u) override {
    assert(u->is_simple());
    Query_result_interceptor::prepare(thd, items, u);

    Temp_table_param tmp_table_param;
    tmp_table_param.skip_create_table = true;
    tmp_table_param.func_count = m_tmp_table_param->func_count;
#if 0
    // See create_tmp_table, worker can set Item::marker by
    // make_tmp_tables_info() which can affect this table difference with
    // leader.
    for (auto *item : items) {
      if (item->marker != Item::MARKER_BIT) continue;
      item->marker = Item::MARKER_NONE;
    }
#endif
    if (!(m_table = create_tmp_table(
              thd, &tmp_table_param, items, nullptr, false, true,
              unit->first_query_block()->active_options() |
                  TMP_TABLE_ALL_COLUMNS,
              HA_POS_ERROR, nullptr)))
      return true;
    return false;
  }

  bool send_data(THD *thd, const mem_root_deque<Item *> &items) override {
    if (fill_record(thd, m_table, m_table->visible_field_ptr(), items, nullptr,
                    nullptr, false))
      return true;

    RowExchangeResult res = m_row_exchange_writer->Write(
        m_table->record[0], (size_t)m_table->s->reclength);

    assert(res != RowExchangeResult::WOULD_BLOCK &&
           res != RowExchangeResult::DETACHED);

    if (unlikely(res != RowExchangeResult::SUCCESS)) return true;

    thd->inc_sent_row_count(1);
    return false;
  }

  bool send_eof(THD *) override {
    m_row_exchange_writer->WriteEOF();
    return false;
  }
};

Worker::Worker(THD *thd, uint worker_id, PartialPlan *plan,
               mysql_mutex_t *state_lock, mysql_cond_t *state_cond)
    : m_leader_thd(thd),
      m_id(worker_id),
      m_query_plan(plan),
      m_row_exchange_writer(&m_row_exchange),
      m_state_lock(state_lock),
      m_state_cond(state_cond) {}

bool Worker::Init() {
  if (!(m_message_queue = new (m_leader_thd->mem_root)
            MemMessageQueue(message_queue_ring_size)) ||
      m_message_queue->Init(m_leader_thd) ||
      m_row_exchange.Init(m_leader_thd->mem_root,
                          [this](uint) { return m_message_queue; }) ||
      m_row_exchange_writer.Init(m_leader_thd))
    return true;

  return false;
}

int Worker::Start() {
  my_thread_handle th;
  mysql_mutex_lock(m_state_lock);
  m_state = State::Starting;
  mysql_mutex_unlock(m_state_lock);
  int res = mysql_thread_create(PSI_INSTRUMENT_ME, &th, &connection_attrib,
                                launch_worker_thread_handle, (void *)this);
  if (res != 0) {
    mysql_mutex_lock(m_state_lock);
    m_state = State::StartFailed;
    mysql_mutex_unlock(m_state_lock);
  }

  return res;
}

bool Worker::IsRunning() {
  mysql_mutex_assert_owner(m_state_lock);

  return m_state == State::Started || m_state == State::Starting;
}

void Worker::Terminate() {
  THD *thd = &m_thd;

  if (m_terminate_requested || thd->killed) return;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_QUERY);
  m_terminate_requested = true;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

void Worker::InitExecThdFromLeader() {
  THD *thd = &m_thd;
  THD *leader_thd = m_leader_thd;

  // FIXME: XXX transaction state of thd may be changed by Attachable_trx. If
  // so, the transation state may be invalid when the workers finished
  thd->tx_isolation = leader_thd->tx_isolation;

  thd->set_time(&leader_thd->start_time);
  thd->set_db(leader_thd->db());
  mysql_mutex_lock(&leader_thd->LOCK_thd_query);
  thd->set_query(leader_thd->query());
  mysql_mutex_unlock(&leader_thd->LOCK_thd_query);
  thd->set_query_id(leader_thd->query_id);
  thd->set_security_context(leader_thd->security_context());

  thd->first_successful_insert_id_in_prev_stmt =
      leader_thd->first_successful_insert_id_in_prev_stmt;
}

void Worker::ThreadMainEntry() {
  mysql_mutex_lock(m_state_lock);
  m_state = State::Started;
  mysql_mutex_unlock(m_state_lock);

  THD *thd = &m_thd;

  THD_CHECK_SENTRY(thd);
  my_thread_init();

  // XXX HAVE_PSI_THREAD_INTERFACE process

  thd->thread_stack = (char *)&thd;  // remember where our stack is

  thd->set_new_thread_id();
  thd->store_globals();

  InitExecThdFromLeader();

  thd->m_digest = &thd->m_digest_state;
  thd->m_digest->reset(thd->m_token_array, get_max_digest_length());
  THD_STAGE_INFO(thd, stage_starting);
  thd->tx_isolation = leader_thd()->tx_isolation;

  ExecuteQuery();

  THD_CHECK_SENTRY(thd);

  mysql_mutex_lock(m_state_lock);
  m_state = State::Finished;
  mysql_cond_broadcast(m_state_cond);
  mysql_mutex_unlock(m_state_lock);

  my_thread_end();
  my_thread_exit(nullptr);
}

bool Worker::PrepareQueryPlan() {
  THD *thd = &m_thd;
  LEX *lex = thd->lex, *orig_lex = m_leader_thd->lex;

  if (lex_start(thd)) return true;

  lex->is_partial_plan = true;

  // Inherit some properties of leader
  //
  // The sql_command to sync up locking behavior etc. The storage like innodb
  // depends on this.
  lex->sql_command = orig_lex->sql_command;
  lex->explain_format = orig_lex->explain_format;
  lex->is_explain_analyze = orig_lex->is_explain_analyze;

  Query_block *from_query_block = m_query_plan->QueryBlock();
  JOIN *from_join = from_query_block->join;

  // XXX MDL lock needs some process
  Query_result_interceptor *query_result =
      new (thd->mem_root) Query_result_to_collector(
          &m_row_exchange_writer, &from_join->tmp_table_param);

  if (!query_result) {
    // Let row exchange reader side return
    m_row_exchange_writer.WriteEOF();
    return true;
  }
  lex->result = query_result;

  Query_block *query_block = lex->query_block;

  // Clone partial query plan and open tables
  if (add_tables_to_query_block(thd, query_block,
                                from_query_block->leaf_tables))
    return true;

  if (open_tables_for_query(thd, lex->query_tables, 0) ||
      lock_tables(thd, lex->query_tables, lex->table_count, 0))
    return true;

  Query_expression *unit = lex->unit,
                   *from_unit = m_query_plan->QueryExpression();

  Item_clone_context clone_context(thd, query_block);
  clone_context.set_fix_func(
      Item_clone_context::FIELD_FIX_FUNC, [query_block](Item *item, uchar *) {
        Item_field *item_field = down_cast<Item_field *>(item);
        item_field->table_ref =
            query_block->find_identical_table_with(item_field->table_ref);
        TABLE *table = item_field->table_ref->table;
        item_field->field = table->field[item_field->field_index];
        return false;
      });

  if (unit->clone_from(thd, from_unit, &clone_context)) return true;
  if (query_block->change_query_result(thd, query_result, nullptr)) return true;

  unit->set_query_result(query_result);

  thd->lex->set_current_query_block(query_block);
  thd->query_plan.set_query_plan(lex->sql_command, lex, false);
  return false;
}

void Worker::ExecuteQuery() {
  THD *thd = &m_thd;

  if (PrepareQueryPlan()) {
    assert(thd->is_error() || thd->killed);
    return;
  }

  Query_expression *unit = thd->lex->unit;
  // XXX transaction read view clone
  // XXX explain analyze support
  unit->execute(thd);

  Cleanup();
}

void Worker::Cleanup() {
  THD *thd = &m_thd;
  LEX *lex = thd->lex;
  Query_expression *unit = lex->unit;

  THD_STAGE_INFO(thd, stage_end);

  // In order to call JOIN::cleanup()
  unit->cleanup(thd, false);

  lex->clear_values_map();

  // Perform statement-specific cleanup for Query_result
  if (lex->result != NULL) lex->result->cleanup(thd);

  if (thd->is_error())
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);

  // In order to call JOIN::destroy()
  unit->cleanup(thd, true);
  thd->update_previous_found_rows();

  THD_STAGE_INFO(thd, stage_closing_tables);
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();
  THD_STAGE_INFO(thd, stage_freeing_items);
  thd->end_statement();
  thd->cleanup_after_query();
  THD_STAGE_INFO(thd, stage_cleaning_up);
  thd->reset_query();
  thd->set_command(COM_SLEEP);
  thd->set_proc_info(nullptr);
  thd->lex->sql_command = SQLCOM_END;

  thd->release_resources();
  thd->mem_root->Clear();
}

Diagnostics_area *Worker::stmt_da(ha_rows *found_rows, ha_rows *examined_rows) {
  if (m_state != State::Finished) return nullptr;

  THD *thd = &m_thd;

  *found_rows = thd->previous_found_rows;
  *examined_rows = thd->get_examined_row_count();

  Diagnostics_area *da = thd->get_stmt_da();
  if (da->current_statement_cond_count() == 0) return nullptr;
  if (da->is_error() && ((m_terminate_requested &&
                          da->mysql_errno() == ER_QUERY_INTERRUPTED)))
    return nullptr;

  return da;
}
}  // namespace pq