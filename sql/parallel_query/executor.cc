#include "sql/parallel_query/executor.h"

#include <chrono>
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/filesort.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/explain_access_path.h"
#include "sql/mysqld.h"
#include "sql/opt_explain_traditional.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/worker.h"
#include "sql/query_result.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"
#include "sql/table.h"
#include "sql/transaction.h"

namespace pq {

Collector::Collector(uint num_workers, PartialPlan *partial_plan)
    : m_partial_plan(partial_plan), m_workers(num_workers) {
  mysql_mutex_init(PSI_INSTRUMENT_ME, &m_worker_state_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_INSTRUMENT_ME, &m_worker_state_cond);
}

Collector::~Collector() {
  if (m_table) {
    close_tmp_table(m_table);
    free_tmp_table(m_table);
  }
  // tmp_buffer in Sort_param needs to destroy because it may contain alloced
  // memory.
  destroy(m_merge_sort);

  Reset();

  mysql_mutex_destroy(&m_worker_state_lock);
  mysql_cond_destroy(&m_worker_state_cond);
}

bool Collector::CreateCollectorTable() {
  Query_block *block = partial_plan()->QueryBlock();
  JOIN *join = block->join;
  THD *thd = join->thd;
  Temp_table_param *tmp_table_param;
  if (!(tmp_table_param =
            new (thd->mem_root) Temp_table_param(join->tmp_table_param)))
    return true;
  tmp_table_param->skip_create_table = true;
  tmp_table_param->func_count = join->tmp_table_param.func_count;
  tmp_table_param->sum_func_count = join->tmp_table_param.sum_func_count;

  m_table =
      create_tmp_table(thd, tmp_table_param, block->fields, nullptr, false,
                       true, block->active_options() | TMP_TABLE_ALL_COLUMNS,
                       HA_POS_ERROR, "collector");

  if (!m_table) return true;

  bitmap_set_all(m_table->read_set);

  return false;
}

void Collector::PrepareExecution(THD *thd) {
  DBUG_SAVE_CSSTACK(&dbug_cs_stack_clone);

  thd->mdl_context.init_lock_group();
}

void Collector::Destroy(THD *thd) { thd->mdl_context.deinit_lock_group(); }

bool Collector::CreateMergeSort(JOIN *join, ORDER *merge_order) {
  THD *thd = join->thd;
  // Merge sort always reads collector table.
  assert(join->current_ref_item_slice == REF_SLICE_SAVED_BASE);
  if (!(m_merge_sort = new (thd->mem_root)
            Filesort(thd, {m_table}, /*keep_buffers=*/false, merge_order,
                     HA_POS_ERROR, /*force_stable_sort=*/false,
                     /*remove_duplicates=*/false,
                     /*force_sort_positions=*/true, /*unwrap_rollup=*/false)))
    return true;

  return false;
}

bool Collector::InitParallelScan() {
  auto &psinfo = m_partial_plan->TablesParallelScan();
  auto *table = psinfo.table;
  int res;

  if ((res = table->file->init_parallel_scan(&table->parallel_scan_handle,
                                             &psinfo.suggested_ranges,
                                             &psinfo.scan_desc)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

bool Collector::Init(THD *thd) {
  // Here reserved 0 as leader's id. If you use Worker::m_id as a 0-based index,
  // you should use m_id - 1
  uint wid = 1;
  for (auto *&worker : m_workers) {
    worker = CreateLocalWorker(wid++, thd, partial_plan(), &m_worker_state_lock,
                               &m_worker_state_cond);
    if (!worker || worker->Init(m_receiver_exchange.event())) return true;
  }

  if (m_receiver_exchange.Init(
          thd->mem_root, m_workers.size(),
          [this](uint widx) { return m_workers[widx]->receiver_channel(); }))
    return true;

  if (!(m_row_exchange_reader = comm::CreateRowExchangeReader(
            thd->mem_root, &m_receiver_exchange, m_table, m_merge_sort)))
    return true;

  // Do initialize parallel scan for tables before workers are started
  if (InitParallelScan()) return true;

  DEBUG_SYNC(thd, "before_launch_pqworkers");

  bool has_failed_worker;
  if (LaunchWorkers(thd, &has_failed_worker)) return true;

  MY_BITMAP closed_queues;
  if (has_failed_worker) {
    if (bitmap_init(&closed_queues, nullptr, NumWorkers())) return true;
    // Close the queues which workers are launched failed.
    wid = 0;
    for (auto *worker : m_workers) {
      if (worker->IsStartFailed()) bitmap_set_bit(&closed_queues, wid++);
    }
  }
  // Initialize row exchange reader after workers are started. The reader with
  // merge sort do a block read in Init().
  bool res = m_row_exchange_reader->Init(thd);

  if (has_failed_worker) bitmap_free(&closed_queues);
  return res;
}

void Collector::Reset() {
  // Reset could be called multiple times without Collector::Init() calling, see
  // subselect_hash_sj_engine::exec()
  if (!m_row_exchange_reader) return;
  destroy(m_row_exchange_reader);
  m_row_exchange_reader = nullptr;
  m_receiver_exchange.Reset();

  for (auto *&worker : m_workers) destroy(worker);
}

bool Collector::LaunchWorkers(THD *thd, bool *has_failed_worker) {
  *has_failed_worker = false;
  bool all_start_error = true;

  for (auto *worker : m_workers) {
    SET_DBUG_CS_STACK_CLONE(worker, &dbug_cs_stack_clone);
    auto res = worker->Start();
    if (!res && all_start_error) all_start_error = false;
    *has_failed_worker |= res;
  }
  if (all_start_error) return all_start_error;

  if (*has_failed_worker) thd->clear_error();

  return false;
}

void Collector::TerminateWorkers() {
  // Note, worker could fail to allocate see Init()
  for (auto *worker : m_workers) {
      if (!worker) continue;
      worker->Terminate();
  }
  // Wait all workers to exit.
  mysql_mutex_lock(&m_worker_state_lock);
  while (true) {
    uint left_workers = m_workers.size();
    for (auto *worker : m_workers) {
      if (!worker || !worker->IsRunning(false)) --left_workers;
    }
    if (left_workers == 0) break;
    mysql_cond_wait(&m_worker_state_cond, &m_worker_state_lock);
  }

  mysql_mutex_unlock(&m_worker_state_lock);
}

Collector::CollectResult Collector::Read(THD *thd) {
  auto res = m_row_exchange_reader->Read(thd, m_table->record[0],
                                         m_table->s->reclength);
  if (res == CollectResult::SUCCESS) ++thd->status_var.pq_rows_exchanged;

#ifndef NDEBUG
  if (res == CollectResult::ERROR && !thd->killed && !thd->is_error()) {
    bool found = false;
    for (auto *worker : m_workers) {
      if (worker->is_error()) {
        found = true;
      }
    }
    assert(found);
  }
#endif
  return res;
}

Diagnostics_area *Collector::combine_workers_stmt_da(THD *thd,
                                                     ha_rows *found_rows) {
  Diagnostics_area *cond_da = nullptr;
  bool is_error = false;

  if (found_rows) *found_rows = 0;
  for (auto *worker : m_workers) {
    if (!worker) continue;
    ha_rows cur_found_rows = 0;
    ha_rows cur_examined_rows = 0;
    Diagnostics_area *da =
        worker->stmt_da(true, &cur_found_rows, &cur_examined_rows);

    if (found_rows) *found_rows += cur_found_rows;

    thd->inc_examined_row_count(cur_examined_rows);

    if (!da) continue;

    is_error = da->is_error();

    // Just copy first sql conditions if there is error in any workers,
    // otherwise just saves the error worker.
    if (!cond_da || is_error) cond_da = da;

    if (is_error) break;
  }

  return cond_da;
}

void Collector::CollectStatusFromWorkers(THD *thd) {
  for (auto *worker : m_workers) {
    if (worker) worker->CollectStatusVars(thd);
  }
}

void Collector::End(THD *thd, ha_rows *found_rows) {
  if (is_ended) return;
  TerminateWorkers();

  // XXX moves this to elsewhere if we support multiple collector
  CollectStatusFromWorkers(thd);

  is_ended = true;

  auto *combined_da = combine_workers_stmt_da(thd, found_rows);
  if (!combined_da) return;

  Diagnostics_area *da = thd->get_stmt_da();
  da->copy_sql_conditions_from_da(thd, combined_da);
  if (!combined_da->is_error()) return;

  bool old_ow_status = da->set_overwrite_status(true);
  da->set_error_status(combined_da->mysql_errno(), combined_da->message_text(),
                       combined_da->returned_sqlstate());
  da->set_overwrite_status(old_ow_status);

  // reset XA state error according to new THD error.
  XID_STATE *xid_state = thd->get_transaction()->xid_state();
  if (xid_state) xid_state->set_error(thd);
}

JOIN *Collector::PartialJoin() const { return partial_plan()->Join(); }

AccessPath *Collector::PartialRootAccessPath() const {
  return partial_plan()->Join()->root_access_path();
}

using duration_type = std::chrono::steady_clock::time_point::duration;
static std::string::size_type ReadOneIteratorTimingData(
    const char *timing_data, std::string::size_type cursor, uint64_t *num_rows,
    uint64_t *num_init_calls, duration_type *time_spent_in_first_row,
    duration_type *time_spent_in_other_rows) {
  auto offset = 0;
  auto size_of_var = sizeof num_rows;
  memcpy(num_rows, timing_data + cursor, size_of_var);
  offset += size_of_var;
  memcpy(num_init_calls, timing_data + cursor + offset, size_of_var);
  offset += size_of_var;
  size_of_var = sizeof time_spent_in_first_row;
  memcpy(time_spent_in_first_row, timing_data + cursor + offset, size_of_var);
  offset += size_of_var;
  memcpy(time_spent_in_other_rows, timing_data + cursor + offset, size_of_var);
  offset += size_of_var;

  return offset;
}

static std::pair<std::string *, std::string *> GetOutputWorkersTimingData(
    Collector *collector) {
  std::string *min_worker_timing_data{nullptr},
      *max_worker_timing_data{nullptr};
  duration_type min_total_rows_spent{duration_type::max()},
      max_total_rows_spent{duration_type::zero()};

  collector->ForEachWorker([&](Worker *worker) {
    auto *timing_data = worker->QueryPlanTimingData();
    if (!timing_data) return;
    // Only needs check root iterator
    uint64 num_rows, num_init_calls;
    duration_type time_spent_in_first_row, time_spent_in_other_rows;
    ReadOneIteratorTimingData(timing_data->c_str(), 0, &num_rows,
                              &num_init_calls, &time_spent_in_first_row,
                              &time_spent_in_other_rows);
    auto total_all_rows_spent =
        time_spent_in_first_row + time_spent_in_other_rows;

    if (total_all_rows_spent < min_total_rows_spent) {
      min_worker_timing_data = timing_data;
      min_total_rows_spent = total_all_rows_spent;
    }

    if (total_all_rows_spent > max_total_rows_spent) {
      max_worker_timing_data = timing_data;
      max_total_rows_spent = total_all_rows_spent;
    }
  });

  return {min_worker_timing_data, max_worker_timing_data};
}

/**
   A fake iterator for parallel partial plan timing, all partial access paths
   share same object. It just forwards to collector.
 */
class FakeTimingIterator : public RowIterator {
 public:
  FakeTimingIterator(THD *thd, Collector *collector)
      : RowIterator(thd), m_collector(collector) {}
  /**
    This class just for EXPLAIN ANALYZE, these methods should not be called.
  */
  bool Init() override {
    assert(false);
    return false;
  }
  int Read() override {
    assert(false);
    return -1;
  }
  void SetNullRowFlag(bool) override { assert(false); }
  void UnlockRow() override { assert(false); }

  /**
    Partial plan is executed by multiple workers, Here output minimum
    last_row_spent and maximum last_row_sent worker's timing data.
  */
  std::string TimingString(bool) const override {
    uint64_t num_rows1, num_rows2, num_init_calls1, num_init_calls2;
    duration_type time_spent_in_first_row1, time_spent_in_first_row2,
        time_spent_in_other_rows1, time_spent_in_other_rows2;

    if (m_timing_data_cursor == 0)
      const_cast<FakeTimingIterator *>(this)->m_timing_data =
          GetOutputWorkersTimingData(m_collector);

    assert(m_timing_data_cursor < m_timing_data.first->size() &&
           m_timing_data_cursor < m_timing_data.second->size());
    ReadOneIteratorTimingData(m_timing_data.first->c_str(),
                              m_timing_data_cursor, &num_rows1,
                              &num_init_calls1, &time_spent_in_first_row1,
                              &time_spent_in_other_rows1);

    const_cast<FakeTimingIterator *>(this)->m_timing_data_cursor +=
        ReadOneIteratorTimingData(m_timing_data.second->c_str(),
                                  m_timing_data_cursor, &num_rows2,
                                  &num_init_calls2, &time_spent_in_first_row2,
                                  &time_spent_in_other_rows2);

    char buf[1024];
    if (num_init_calls1 == 0 && num_init_calls2 == 0) {
      snprintf(buf, sizeof(buf), "(never executed)");
      return buf;
    }

    double first_row_ms1 =
        std::chrono::duration<double>(time_spent_in_first_row1).count() * 1e3;
    double last_row_ms1 =
        std::chrono::duration<double>(time_spent_in_first_row1 +
                                      time_spent_in_other_rows1)
            .count() *
        1e3;
    double first_row_ms2 =
        std::chrono::duration<double>(time_spent_in_first_row2).count() * 1e3;
    double last_row_ms2 =
        std::chrono::duration<double>(time_spent_in_first_row2 +
                                      time_spent_in_other_rows2)
            .count() *
        1e3;
    if (m_collector->NumWorkers() == 1)
      snprintf(buf, sizeof(buf),
               "(actual time=%.3f..%.3f rows=%lld loops=%" PRIu64 ")",
               first_row_ms2 / num_init_calls2, last_row_ms2 / num_init_calls2,
               llrintf(static_cast<double>(num_rows2) / num_init_calls2),
               num_init_calls2);
    else
      snprintf(
          buf, sizeof(buf),
          "(actual time=%.3f..%.3f/%.3f..%.3f rows=%lld/%lld loops=%" PRIu64
          "/%" PRIu64 ")",
          num_init_calls1 > 0 ? first_row_ms1 / num_init_calls1 : 0,
          first_row_ms2 / num_init_calls2,
          num_init_calls1 > 0 ? last_row_ms1 / num_init_calls1 : 0,
          last_row_ms2 / num_init_calls2,
          num_init_calls1 > 0
              ? llrintf(static_cast<double>(num_rows1) / num_init_calls1)
              : 0,
          llrintf(static_cast<double>(num_rows2) / num_init_calls2),
          num_init_calls1, num_init_calls2);

    return buf;
  }

 private:
  Collector *m_collector;
  std::string::size_type m_timing_data_cursor{0};
  std::pair<std::string *, std::string *> m_timing_data;
};

std::string ExplainTableParallelScan(JOIN *join, TABLE *table) {
  std::string str;
  // For the union temporary scan, join is nullptr
  if (!join || !join->partial_plan) return str;

  auto &scaninfo = join->partial_plan->TablesParallelScan();
  if (table != scaninfo.table) return str;

  str = ", with parallel scan ranges: " +
        std::to_string(scaninfo.suggested_ranges);
  if (scaninfo.scan_desc.key_used != UINT16_MAX)
    str += ", prefix: " + std::to_string(scaninfo.scan_desc.key_used);
  return str;
}

RowIterator *NewFakeTimingIterator(THD *thd, Collector *collector) {
  return new (thd->mem_root) FakeTimingIterator(thd, collector);
}
class Query_result_to_collector : public Query_result_interceptor {
 private:
  comm::RowChannel *m_row_channel;
  comm::RowExchangeWriter *m_row_exchange_writer;
  Temp_table_param *m_tmp_table_param;
  TABLE *m_table{nullptr};

 public:
  Query_result_to_collector(comm::RowChannel *row_channel,
                            comm::RowExchangeWriter *row_exchange_writer,
                            Temp_table_param *tmp_table_param)
      : m_row_channel(row_channel),
        m_row_exchange_writer(row_exchange_writer),
        m_tmp_table_param(tmp_table_param) {}

  void cleanup(THD *thd) override {
    Query_result_interceptor::cleanup(thd);
    if (!m_row_channel->IsClosed()) m_row_channel->Close();

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

    if (m_row_exchange_writer->CreateRowSegmentCodec(thd->mem_root, m_table))
      return true;

    return false;
  }

  bool send_data(THD *thd, const mem_root_deque<Item *> &items) override {
    if (fill_record(thd, m_table, m_table->visible_field_ptr(), items, nullptr,
                    nullptr, false))
      return true;

    auto res = m_row_exchange_writer->Write(m_table->record[0],
                                            (size_t)m_table->s->reclength);

    assert(res != comm::RowExchange::Result::END);

    if (unlikely(res != comm::RowExchange::Result::SUCCESS)) return true;

    thd->inc_sent_row_count(1);

    DBUG_EXECUTE_IF("pq_simulate_one_worker_part_result_error", {
      if (thd->get_sent_row_count() == 2) {
        my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 1);
        return true;
      }
    });

    return false;
  }

  bool send_eof(THD *) override {
    m_row_exchange_writer->WriteEOF();
    m_row_channel->Close();
    return false;
  }
};

class PartialItemCloneContext : public Item_clone_context {
 public:
  PartialItemCloneContext(
      THD *thd, Query_block *query_block, ItemRefCloneResolver *ref_resolver,
      std::function<user_var_entry *(const std::string &)> find_user_var_entry)
      : Item_clone_context(thd, query_block, ref_resolver),
        m_find_user_var_entry(find_user_var_entry) {}

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
    user_var_entry *entry = m_find_user_var_entry(key);
    item->set_var_entry(entry);
  }

 private:
  std::function<user_var_entry *(const std::string &)> m_find_user_var_entry;
};

bool PartialExecutor::Init(comm::RowChannel *sender_channel,
                           comm::RowExchange *sender_exchange,
                           bool is_explain_analyze) {
  m_sender_channel = sender_channel;
  m_sender_exchange = sender_exchange;
  m_row_exchange_writer.SetExchange(m_sender_exchange);

  if (is_explain_analyze) m_query_plan_timing_data.reset(new std::string);
  return false;
}

bool PartialExecutor::PrepareQueryPlan(PartialExecutorContext *context) {
  THD *thd = m_thd;
  LEX *lex = thd->lex;

  if (lex_start(thd)) {
    NotifyAbort();
    return true;
  }

  lex->is_partial_plan = true;

  // Inherit some properties of leader
  //
  // The sql_command to sync up locking behavior etc. The storage like innodb
  // depends on this.
  lex->sql_command = context->sql_command;
  lex->is_explain_analyze = context->is_explain_analyze;
  if (lex->is_explain_analyze &&
      !(lex->explain_format = new (thd->mem_root) Explain_format_tree)) {
    NotifyAbort();
    return true;
  }

  auto *from_query_block = m_query_plan->QueryBlock();
  auto *from_join = from_query_block->join;
  auto *query_result = new (thd->mem_root) Query_result_to_collector(
      m_sender_channel, &m_row_exchange_writer, &from_join->tmp_table_param);

  DBUG_EXECUTE_IF("pq_simulate_worker_prepare_query_plan_error_1", {
    my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 2);
    query_result = nullptr;
  });

  if (!query_result) {
    NotifyAbort();
    return true;
  }
  lex->result = query_result;
  auto *query_block = lex->query_block;

  // Clone partial query plan and open tables
  if (add_tables_to_query_block(thd, query_block,
                                from_query_block->leaf_tables))
    return true;

  if (open_tables_for_query(thd, lex->query_tables, 0) ||
      lock_tables(thd, lex->query_tables, lex->table_count, 0))
    return true;

  DBUG_EXECUTE_IF("pq_simulate_worker_prepare_query_plan_error_2", {
    my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 3);
    return true;
  });

  auto *unit = lex->unit, *from_unit = m_query_plan->QueryExpression();
  ItemRefCloneResolver ref_clone_resolver(thd->mem_root, query_block);
  PartialItemCloneContext clone_context(thd, query_block, &ref_clone_resolver,
                                        context->find_user_var_entry);
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

bool PartialExecutor::AttachTablesParallelScan() {
  auto &psinfo = m_query_plan->TablesParallelScan();
  THD *thd = m_thd;
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

void PartialExecutor::NotifyAbort() { m_sender_channel->Close(); }

void PartialExecutor::ExecuteQuery(PartialExecutorContext *context) {
  THD *thd = m_thd;
  auto *lex = thd->lex;

  DEBUG_SYNC(thd, "before_pqworker_exec_query");

  if (PrepareQueryPlan(context)) {
    assert(thd->is_error() || thd->killed);
    goto cleanup;
  }

  if (lex->unit->execute(thd)) {
    assert(thd->is_error() || thd->killed);
  }

  // Always print plan timing even the worker is error or got killed, see also
  // send_kill_message();
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

void PartialExecutor::EndQuery() {
  THD *thd = m_thd;
  LEX *lex = thd->lex;
  Query_expression *unit = lex->unit;

  m_cleanup_func();

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
}

void PartialExecutor::InitExecThd(PartialExecutorContext *ctx,
                                  THD *mdl_group_leader) {
  THD *thd = m_thd;

  // FIXME: XXX transaction state of thd may be changed by Attachable_trx. If
  // so, the transation state may be invalid when the workers finished
  thd->tx_isolation = ctx->tx_isolation;
  thd->set_time(ctx->start_time);
  thd->set_db(ctx->db);
  thd->set_query(ctx->query);
  thd->set_query_id(ctx->query_id);
  thd->set_security_context(ctx->security_context);
  //  LAST_INSERT_ID() push down need this, see class Item_func_last_insert_id;
  thd->first_successful_insert_id_in_prev_stmt =
      ctx->first_successful_insert_id_in_prev_stmt;
  thd->save_raw_record = ctx->save_raw_record;

  // Variable from be MDL group leader on the worker's node, nullptr means
  // current worker is leader.
  if (mdl_group_leader)
    thd->mdl_context.join_lock_group(&mdl_group_leader->mdl_context);

  thd->m_digest = &thd->m_digest_state;
  thd->m_digest->reset(thd->m_token_array, get_max_digest_length());

  // Thank add_to_status(), Leader will count workers created
  thd->status_var.pq_workers_created = 1;
}

}  // namespace pq

CollectorIterator::CollectorIterator(THD *thd, pq::Collector *collector,
                                     ha_rows *examined_rows)
    : RowIterator(thd),
      m_collector(collector),
      m_examined_rows(examined_rows) {}

CollectorIterator::~CollectorIterator() {}

bool CollectorIterator::Init() {
  empty_record(table());
  return m_collector->Init(thd());
}

int CollectorIterator::Read() {
  using CollectResult = pq::Collector::CollectResult;
  /* Read data from workers and save it to record[0] of collector table */
  auto res = m_collector->Read(thd());
  if (res == CollectResult::END) return -1;
  if (res == CollectResult::ERROR) {
    if (thd()->killed) {
      thd()->send_kill_message();
      return 1;
    }
    return 1;
  }

  if (m_examined_rows != nullptr) {
    ++*m_examined_rows;
  }

  return 0;
}

void JOIN::end_parallel_plan(bool fill_send_records) {
  if (!parallel_plan) return;
  if (!calc_found_rows) fill_send_records = false;
  parallel_plan->EndCollector(thd, fill_send_records ? &send_records : nullptr);
}

void JOIN::destroy_parallel_plan() {
  if (!parallel_plan) return;

  ::destroy(parallel_plan);
  parallel_plan = nullptr;
}
