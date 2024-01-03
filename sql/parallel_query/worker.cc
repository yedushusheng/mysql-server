#include "sql/parallel_query/worker.h"
#include "scope_guard.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/explain_access_path.h"
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

    auto res = m_row_exchange_writer->Write(m_table->record[0],
                                            (size_t)m_table->s->reclength);

    assert(res != RowExchange::Result::ERROR &&
           res != RowExchange::Result::END);

    if (unlikely(res != RowExchange::Result::SUCCESS)) return true;

    thd->inc_sent_row_count(1);

    DBUG_EXECUTE_IF("pq_simulate_one_worker_part_result_error", {
      if (m_worker_id == 1 && thd->get_sent_row_count() == 10) {
        my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 1);
        return true;
      }
    });

    return false;
  }

  bool send_eof(THD *) override {
    m_row_exchange_writer->WriteEOF();
    return false;
  }

#ifndef NDEBUG
  uint m_worker_id{0};
#endif
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
#if defined(ENABLED_DEBUG_SYNC)
  debug_sync_set_eval_id(&m_thd, m_id);
  debug_sync_clone_actions(&m_thd, m_leader_thd);
#endif

  if (!(m_message_queue = new (m_leader_thd->mem_root)
            MemMessageQueue(message_queue_ring_size)) ||
      m_message_queue->Init(m_leader_thd) ||
      m_row_exchange.Init(m_leader_thd->mem_root,
                          [this](uint) { return m_message_queue; }) ||
      m_row_exchange_writer.Init(m_leader_thd, nullptr))
    return true;
  if (m_leader_thd->lex->is_explain_analyze)
    m_query_plan_timing_data.reset(new std::string);

  return false;
}

int Worker::Start() {
  my_thread_handle th;
  SetState(State::Starting);
  int res = mysql_thread_create(PSI_INSTRUMENT_ME, &th, &connection_attrib,
                                launch_worker_thread_handle, (void *)this);
  if (res != 0) SetState(State::StartFailed);

  return res;
}

bool Worker::IsRunning(bool need_state_lock) {
  if (need_state_lock)
    mysql_mutex_lock(m_state_lock);
  else {
    mysql_mutex_assert_owner(m_state_lock);
  }
  bool is_running = (m_state == State::Started || m_state == State::Cleaning ||
                     m_state == State::Starting);
  if (need_state_lock) mysql_mutex_unlock(m_state_lock);
  return is_running;
}

bool Worker::IsStartFailed() const {
  mysql_mutex_lock(m_state_lock);
  bool res = (m_state == State::StartFailed);
  mysql_mutex_unlock(m_state_lock);
  return res;
}

void Worker::SetState(Worker::State state) {
  mysql_mutex_lock(m_state_lock);
  m_state = state;
  if (state == State::Finished) mysql_cond_broadcast(m_state_cond);
  mysql_mutex_unlock(m_state_lock);
}

void Worker::Terminate() {
  THD *thd = &m_thd;

  if (m_terminate_requested || thd->killed) return;

  mysql_mutex_lock(m_state_lock);
  bool need_send_kill = IsRunning(false) && m_state != State::Cleaning;
  mysql_mutex_unlock(m_state_lock);
  if (!need_send_kill) return;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_QUERY);
  m_terminate_requested = true;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

void Worker::InitExecThdFromLeader() {
  THD *thd = &m_thd;
  THD *from = m_leader_thd;

  // FIXME: XXX transaction state of thd may be changed by Attachable_trx. If
  // so, the transation state may be invalid when the workers finished
  thd->tx_isolation = from->tx_isolation;

  thd->set_time(&from->start_time);
  thd->set_db(from->db());
  mysql_mutex_lock(&from->LOCK_thd_query);
  thd->set_query(from->query());
  mysql_mutex_unlock(&from->LOCK_thd_query);
  thd->set_query_id(from->query_id);
  thd->set_security_context(from->security_context());

  thd->m_digest = &thd->m_digest_state;
  thd->m_digest->reset(thd->m_token_array, get_max_digest_length());

  //  LAST_INSERT_ID() push down need this, see class Item_func_last_insert_id;
  thd->first_successful_insert_id_in_prev_stmt =
      from->first_successful_insert_id_in_prev_stmt;

  //myrock_encode function need this.
  thd->save_raw_record = from->save_raw_record;

  // Thank add_to_status(), Leader will count workers created
  thd->status_var.pq_workers_created = 1;
}

void Worker::ThreadMainEntry() {
  SetState(State::Started);

  THD *thd = &m_thd;

  THD_CHECK_SENTRY(thd);
  my_thread_init();

  // XXX HAVE_PSI_THREAD_INTERFACE process

  thd->thread_stack = (char *)&thd;  // remember where our stack is

  thd->set_new_thread_id();
  thd->store_globals();

  InitExecThdFromLeader();

  // XXX Note, should after store_globals() calling because
  // THR_mysys is allocated by set_my_thread_var_id() called by in it.
  DBUG_RESTORE_CSSTACK(dbug_cs_stack_clone);

  THD_STAGE_INFO(thd, stage_starting);

  thd->mdl_context.join_lock_group(&m_leader_thd->mdl_context);

  thd->m_digest = &thd->m_digest_state;
  thd->m_digest->reset(thd->m_token_array, get_max_digest_length());
  THD_STAGE_INFO(thd, stage_starting);
  thd->tx_isolation = leader_thd()->tx_isolation;

  // Clone snapshot always return false, currently
  ha_clone_consistent_snapshot(&m_thd, m_leader_thd);

  ExecuteQuery();

  THD_CHECK_SENTRY(thd);

  SetState(State::Finished);

  my_thread_end();
  my_thread_exit(nullptr);
}

class PartialItemCloneContext : public Item_clone_context {
 public:
  PartialItemCloneContext(THD *thd, Query_block *query_block,
                          ItemRefCloneResolver *ref_resolver, THD *leader_thd)
      : Item_clone_context(thd, query_block, ref_resolver),
        m_leader_thd(leader_thd) {}

  using Item_clone_context::Item_clone_context;
  void rebind_field(Item_field *item_field,
                    const Item_field *from_field) override {
    item_field->table_ref =
        m_query_block->find_identical_table_with(from_field->table_ref);
    TABLE *table = item_field->table_ref->table;
    item_field->field = table->field[from_field->field_index];
    item_field->set_result_field(item_field->field);
    item_field->field_index = from_field->field_index;
  }

  void rebind_hybrid_field(Item_sum_hybrid_field *item_hybrid,
                           const Item_sum_hybrid_field *from_item) override {
    // Rebind to current worker leaf table
    Field *orig_field = from_item->get_field();
    TABLE_LIST *table_ref = m_query_block->find_identical_table_with(
        orig_field->table->pos_in_table_list);
    assert(table_ref);
    item_hybrid->set_field(table_ref->table->field[orig_field->field_index()]);
  }

  bool resolve_view_ref(Item_view_ref *item,
                        const Item_view_ref *from) override {
    if (!(item->ref = (Item **)new (mem_root()) Item *) ||
        !(*item->ref = (*from->ref)->clone(this)))
      return true;
    return false;
  }

  void rebind_user_var(Item_func_get_user_var *item) override {
    const std::string key(item->name.ptr(), item->name.length());
    mysql_mutex_lock(&m_leader_thd->LOCK_thd_data);
    user_var_entry *entry = find_or_nullptr(m_leader_thd->user_vars, key);
    mysql_mutex_unlock(&m_leader_thd->LOCK_thd_data);
    item->set_var_entry(entry);
  }

 private:
  THD *m_leader_thd;
};

bool Worker::AttachTablesParallelScan() {
  auto &psinfo = m_query_plan->TablesParallelScan();
  THD *thd = &m_thd;
  auto *query_block = thd->lex->query_block;
  auto *leaf_tables = query_block->leaf_tables;

  // Currently, only support one table
  assert(leaf_tables && !leaf_tables->next_leaf);
  TABLE *table = leaf_tables->table;
  table->parallel_scan_handle = psinfo.table->parallel_scan_handle;
  int res;
  if ((res = table->file->attach_parallel_scan(table->parallel_scan_handle)) !=
      0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

bool Worker::PrepareQueryPlan() {
  THD *thd = &m_thd;
  LEX *lex = thd->lex, *orig_lex = m_leader_thd->lex;

  if (lex_start(thd)) {
    NotifyAbort();
    return true;
  }

  lex->is_partial_plan = true;

  // Inherit some properties of leader
  //
  // The sql_command to sync up locking behavior etc. The storage like innodb
  // depends on this.
  lex->sql_command = orig_lex->sql_command;
  lex->explain_format = orig_lex->explain_format;
  lex->is_explain_analyze = orig_lex->is_explain_analyze;

  auto *from_query_block = m_query_plan->QueryBlock();
  auto *from_join = from_query_block->join;
  auto *query_result = new (thd->mem_root) Query_result_to_collector(
      &m_row_exchange_writer, &from_join->tmp_table_param);

  if (!query_result) {
    NotifyAbort();
    return true;
  }
  lex->result = query_result;
#ifndef NDEBUG
  query_result->m_worker_id = m_id;
#endif
  auto *query_block = lex->query_block;

  // Clone partial query plan and open tables
  if (add_tables_to_query_block(thd, query_block,
                                from_query_block->leaf_tables))
    return true;

  if (open_tables_for_query(thd, lex->query_tables, 0) ||
      lock_tables(thd, lex->query_tables, lex->table_count, 0))
    return true;

  auto *unit = lex->unit,
                   *from_unit = m_query_plan->QueryExpression();
  ItemRefCloneResolver ref_clone_resolver(thd->mem_root, query_block);
  PartialItemCloneContext clone_context(thd, query_block, &ref_clone_resolver,
                                        m_leader_thd);
  if (unit->clone_from(thd, from_unit, &clone_context)) return true;

  clone_context.final_resolve_refs();

  if (query_block->change_query_result(thd, query_result, nullptr)) return true;

  unit->set_query_result(query_result);

  thd->lex->set_current_query_block(query_block);
  thd->query_plan.set_query_plan(lex->sql_command, lex, false);

  // Now attach tables' parallel scan
  if (AttachTablesParallelScan()) return true;

  return false;
}

void Worker::ExecuteQuery() {
  THD *thd = &m_thd;
  auto *lex = thd->lex;

  DEBUG_SYNC(thd, "before_pqworker_exec_query");

  if (PrepareQueryPlan()) {
    assert(thd->is_error() || thd->killed);
    goto cleanup;
  }

  if (lex->unit->execute(thd)) {
    assert(thd->is_error() || thd->killed);
    goto cleanup;
  }

  if (lex->is_explain_analyze) {
    auto *unit = lex->unit;
    auto *join = unit->first_query_block()->join;

    assert(!unit->is_union());

    PrintQueryPlanTiming(unit->root_access_path(), join, true,
                         m_query_plan_timing_data.get());
  }

  DEBUG_SYNC(thd, "after_pqworker_exec_query");

 cleanup:
  EndQuery();
}

void Worker::EndQuery() {
  THD *thd = &m_thd;
  LEX *lex = thd->lex;
  Query_expression *unit = lex->unit;
  SetState(State::Cleaning);

  THD_STAGE_INFO(thd, stage_end);

  // In order to call JOIN::cleanup()
  if (unit) unit->cleanup(thd, false);

  lex->clear_values_map();

  // Perform statement-specific cleanup for Query_result
  if (lex->result != NULL) lex->result->cleanup(thd);

  if (thd->is_error())
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);

  // In order to call JOIN::destroy()
  if (unit) unit->cleanup(thd, true);
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

void Worker::NotifyAbort() { m_row_exchange_writer.WriteEOF(); }

Diagnostics_area *Worker::stmt_da(ha_rows *found_rows, ha_rows *examined_rows) {
  assert(!IsRunning(true));

  if (m_state != State::Finished) return nullptr;

  THD *thd = &m_thd;

  *found_rows = thd->previous_found_rows;
  *examined_rows = thd->get_examined_row_count();

  Diagnostics_area *da = thd->get_stmt_da();
  // The INTERRUPT is sent by leader to end workers
  // XXX should we copy other sql conditions to leader?
  if (m_terminate_requested && da->is_error() &&
      da->mysql_errno() == ER_QUERY_INTERRUPTED)
    return nullptr;

  if (!da->is_error() && da->current_statement_cond_count() == 0)
    return nullptr;
  return da;
}
}  // namespace pq
