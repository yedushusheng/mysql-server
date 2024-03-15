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
bool PartialPlan::InitExecution(uint num_workers) {
  return m_dist_plan->InitExecution(this, num_workers);
}

Collector::Collector(dist::Adapter *dist_adapter, dist::NodeArray *exec_nodes,
                     PartialPlan *partial_plan, uint num_workers)
    : m_dist_adapter(dist_adapter),
      m_exec_nodes(exec_nodes),
      m_partial_plan(partial_plan),
      m_workers(num_workers) {}

Collector::~Collector() {
  if (m_table) {
    close_tmp_table(m_table);
    free_tmp_table(m_table);
  }

  // tmp_buffer in Sort_param needs to destroy because it may contain
  // alloced memory.
  destroy(m_merge_sort);

  Reset();
  ResetWorkersTimingData();
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

bool Collector::CreateMergeSort(JOIN *join, ORDER *merge_order,
                                bool remove_duplicates) {
  THD *thd = join->thd;
  // Merge sort always reads collector table.
  assert(join->current_ref_item_slice == REF_SLICE_SAVED_BASE);
  if (!(m_merge_sort = new (thd->mem_root) Filesort(
            thd, {m_table}, /*keep_buffers=*/false, merge_order, HA_POS_ERROR,
            /*force_stable_sort=*/false, remove_duplicates,
            /*force_sort_positions=*/true, /*unwrap_rollup=*/false)))
    return true;

  return false;
}

bool Collector::Init(THD *thd) {
  ResetWorkersTimingData();

  // Do initialize parallel scan for the plan with parallel scan before workers
  // are created and started. For 3.0, remote workers assign parallel scan jobs
  // in this function and parallel workers are created based on that.
  if (m_partial_plan->InitExecution(m_workers.size())) return true;

  // Execute pre-evaluate subselects before workers launching
  for (auto &cached_subs : m_preevaluate_subqueries) {
    if (cached_subs.cache_subselect(thd)) return true;
  }

  // Here reserved 0 as leader's id. If you use Worker::m_id as a 0-based index,
  // you should use m_id - 1
  for (uint widx = 0; widx < m_workers.size(); ++widx) {
    auto worker_id = widx + 1;
    auto *worker = m_dist_adapter->CreateParallelWorker(
        worker_id, &m_worker_state_event, partial_plan(), m_table,
        m_exec_nodes);
    m_workers[widx] = worker;
    if (!worker || worker->Init(m_receiver_exchange.event())) return true;
  }

  if (m_receiver_exchange.Init(
          thd->mem_root, m_workers.size(),
          [this](uint widx) { return m_workers[widx]->ReceiverChannel(); }))
    return true;

  if (!(m_row_exchange_reader = comm::CreateRowExchangeReader(
            thd->mem_root, &m_receiver_exchange, m_table, m_merge_sort)))
    return true;

  DEBUG_SYNC(thd, "before_launch_pqworkers");

  if (LaunchWorkers()) return true;

  // Initialize row exchange reader after workers are started. The reader with
  // merge sort do a block read in Init().
  if (m_row_exchange_reader->Init(thd)) return true;

  return false;
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

bool Collector::LaunchWorkers() {
  for (auto *worker : m_workers) {
    SET_DBUG_CS_STACK_CLONE(worker, &dbug_cs_stack_clone);
    if (worker->Start()) return true;
  }

  return false;
}

void Collector::TerminateWorkers() {
  // Note, worker could fail to allocate see Init()
  for (auto *worker : m_workers) {
    if (!worker) continue;
    worker->Terminate();
  }
  // Wait all workers to exit, NOTE, Don't check killed here otherwise
  // some workers would lost control.
  while (true) {
    auto left_workers = m_workers.size();
    for (auto *worker : m_workers) {
      if (!worker || !worker->IsRunning()) --left_workers;
    }
    if (left_workers == 0) break;

    m_worker_state_event.Wait(nullptr);
  }
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

AccessPath *Collector::Explain(std::vector<std::string> &description) {
  char buff[64];
  snprintf(buff, sizeof(buff), "Gather (slice: 1, workers: %zu)",
           m_workers.size());
  std::string ret(buff);

  description.push_back(std::move(ret));

  if (m_merge_sort) {
    std::string str;
    if (m_merge_sort->m_remove_duplicates)
      str = "Merge sort with duplicate removal: ";
    else
      str = "Merge sort: ";

    bool first = true;
    for (unsigned i = 0; i < m_merge_sort->sort_order_length(); ++i) {
      if (first) {
        first = false;
      } else {
        str += ", ";
      }

      const st_sort_field *order = &m_merge_sort->sortorder[i];
      str += ItemToString(order->item);
      if (order->reverse) {
        str += " DESC";
      }
    }
    description.push_back(std::move(str));
  }
  if (!m_preevaluate_subqueries.is_empty()) {
    std::string str("Pre-evaluated subqueries:");
    for (auto &item : m_preevaluate_subqueries) {
      snprintf(buff, sizeof(buff), " select #%u,", item.query_block_number());
      str += buff;
    }
    str.resize(str.size() - 1);
    description.push_back(std::move(str));
  }
  bool hide_partial_tree = false;
  std::string partial_desc = m_partial_plan->ExplainPlan(&hide_partial_tree);
  if (partial_desc.size() > 0) description.push_back(std::move(partial_desc));
  return hide_partial_tree ? nullptr : PartialRootAccessPath();
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

void Collector::CollectTimingDataFromWorkers(MEM_ROOT *mem_root) {
  assert(!m_workers_timing_data);
  m_workers_timing_data = new (mem_root) Mem_root_array<std::string>(mem_root);
  // Workers could be invalid since the whole query block could not execute at
  // all. e.g. LIMIT with UNION ALL
  for (auto *worker : m_workers) {
    if (worker) m_workers_timing_data->push_back(worker->QueryPlanTimingData());
  }
}

void Collector::End(THD *thd, ha_rows *found_rows) {
  if (is_ended) return;
  TerminateWorkers();

  // XXX moves this to elsewhere if we support multiple collector
  CollectStatusFromWorkers(thd);
  if (thd->lex->is_explain_analyze) CollectTimingDataFromWorkers(thd->mem_root);

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
  da->set_last_error_tdstore(combined_da->last_error_tdstore());

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

std::pair<std::string *, std::string *> Collector::WorkersTimingData() const {
  std::string *min_worker_timing_data{nullptr},
      *max_worker_timing_data{nullptr};
  duration_type min_total_rows_spent{duration_type::max()},
      max_total_rows_spent{duration_type::zero()};
  for (auto &timing_data : *m_workers_timing_data) {
    if (timing_data.empty()) continue;
    // Only needs check root iterator
    uint64 num_rows, num_init_calls;
    duration_type time_spent_in_first_row, time_spent_in_other_rows;
    ReadOneIteratorTimingData(timing_data.c_str(), 0, &num_rows,
                              &num_init_calls, &time_spent_in_first_row,
                              &time_spent_in_other_rows);
    if (num_init_calls == 0) continue;
    auto total_all_rows_spent =
        time_spent_in_first_row + time_spent_in_other_rows;

    if (total_all_rows_spent < min_total_rows_spent) {
      min_worker_timing_data = &timing_data;
      min_total_rows_spent = total_all_rows_spent;
    }

    if (total_all_rows_spent > max_total_rows_spent) {
      max_worker_timing_data = &timing_data;
      max_total_rows_spent = total_all_rows_spent;
    }
  }

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
    uint64_t num_rows1, num_rows2, num_init_calls1 = 0, num_init_calls2 = 0;
    duration_type time_spent_in_first_row1, time_spent_in_first_row2,
        time_spent_in_other_rows1, time_spent_in_other_rows2;

    if (!m_output_workers_chosen) {
      assert(m_timing_data_cursor == 0);
      const_cast<FakeTimingIterator *>(this)->m_timing_data =
          m_collector->WorkersTimingData();
      const_cast<FakeTimingIterator *>(this)->m_output_workers_chosen = true;
    }

    // Here Collector::WorkersTimingData() could return {nullptr, nullptr} if
    // there is no worker in colloctor because EXPLAIN ANALYZE prints a query
    // plan even the session got killed, see also THD::running_explain_analyze.
    if (m_timing_data.first) {
      assert(m_timing_data.second);
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
    }

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
    if (m_collector->NumWorkers() == 1 ||
        m_timing_data.first == m_timing_data.second)
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
          num_init_calls1 > 0 ? last_row_ms1 / num_init_calls1 : 0,
          first_row_ms2 / num_init_calls2, last_row_ms2 / num_init_calls2,
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
  bool m_output_workers_chosen{false};
  std::pair<std::string *, std::string *> m_timing_data;
};

std::string ExplainTableParallelScan(JOIN *join, TABLE *table) {
  std::string str;
  // For the union temporary scan, join is nullptr
  if (!join || !join->partial_plan) return str;

  auto *partial_plan = join->partial_plan;
  if (!partial_plan->HasParallelScan() ||
      !partial_plan->IsParallelScanTable(table))
    return str;

  auto &scaninfo = join->partial_plan->GetParallelScanInfo();

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
  comm::RowExchangeWriter *m_row_exchange_writer;
  Temp_table_param *m_tmp_table_param;
  TABLE *m_table{nullptr};

 public:
  Query_result_to_collector(comm::RowExchangeWriter *row_exchange_writer,
                            Temp_table_param *tmp_table_param)
      : m_row_exchange_writer(row_exchange_writer),
        m_tmp_table_param(tmp_table_param) {}

  void cleanup(THD *thd) override {
    Query_result_interceptor::cleanup(thd);

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
    return false;
  }

  TABLE *table() const { return m_table; }
};

class PartialItemCloneContext : public Item_clone_context {
 public:
  PartialItemCloneContext(
      THD *thd, Query_block *query_block, ItemRefCloneResolver *ref_resolver,
      std::function<user_var_entry *(const std::string &)> find_user_var_entry,
      CachedSubselects *cached_subselects)
      : Item_clone_context(thd, query_block, ref_resolver),
        m_find_user_var_entry(find_user_var_entry),
        m_cached_subselects(cached_subselects) {}

  void rebind_field(Item_field *item_field,
                    const Item_field *from_field) override {
    item_field->table_ref = find_field_table(from_field->table_ref);
    TABLE *table = item_field->table_ref->table;
    item_field->field = table->field[from_field->field_index];
    item_field->set_result_field(item_field->field);
    item_field->field_index = from_field->field_index;
    if (item_field->table_ref->query_block != m_query_block)
      item_field->depended_from = item_field->table_ref->query_block;    
  }

  void rebind_hybrid_field(Item_sum_hybrid_field *item_hybrid,
                           const Item_sum_hybrid_field *from_item) override {
    // Rebind to current worker leaf table
    Field *orig_field = from_item->get_field();
    TABLE_LIST *table_ref =
        find_field_table(orig_field->table->pos_in_table_list);
    item_hybrid->set_field(table_ref->table->field[orig_field->field_index()]);
  }

  bool resolve_view_ref(Item_view_ref *item,
                        const Item_view_ref *from) override {
    if (ResolveItemRefByInlineClone(item, from, this)) return true;
    if (from->get_first_inner_table()) {
      auto *table = find_field_table(from->get_first_inner_table());
      item->set_first_inner_table(table);
    }

    return false;
  }

  void rebind_user_var(Item_func_get_user_var *item) override {
    const std::string key(item->name.ptr(), item->name.length());
    user_var_entry *entry = m_find_user_var_entry(key);
    item->set_var_entry(entry);
  }

  Item *get_replacement_item(const Item *item) override {
    assert(item->type() == Item::SUBSELECT_ITEM);
    for (auto &subs_item : *m_cached_subselects) {
      if (subs_item.subselect() != item) continue;
      auto *new_item = subs_item.clone(this);
      return new_item;
    }

    // A pushed down sub-queries may be referred by multiple locations. we just
    // clone it once.
    auto *subselect = down_cast<const Item_subselect *>(item);
    Query_expression *new_item_unit = nullptr;
    for (auto *inner_unit = m_query_block->first_inner_query_expression();
         inner_unit; inner_unit = inner_unit->next_query_expression()) {
      if (inner_unit->m_id == subselect->unit->m_id) {
        new_item_unit = inner_unit;
        break;
      }
    }
    assert(new_item_unit);
    if (new_item_unit->item) return new_item_unit->item;

    return nullptr;
  }

  bool use_same_subselect() const override { return false; }

 private:
  TABLE_LIST *find_field_table(TABLE_LIST *table_ref) {
    if (!is_temporary_table(table_ref)) {
      auto *query_block = m_query_block;
      while (query_block) {
        auto *tl = query_block->find_identical_table_with(table_ref);
        if (tl) return tl;
        query_block = query_block->outer_query_block();
      }
      assert(false);
      return nullptr;
    }
    // Some fields could be found in semi-join materialization tables.
    for (auto &ttc : m_query_block->join->temp_tables) {
      auto *tl = ttc.table->pos_in_table_list;
      if (table_ref->m_id == tl->m_id) return tl;
    }

    assert(false);
    return nullptr;
  }

  std::function<user_var_entry *(const std::string &)> m_find_user_var_entry;
  CachedSubselects *m_cached_subselects;
};

bool PartialExecutor::Init(comm::RowChannel *sender_channel,
                           comm::RowExchange *sender_exchange) {
  m_sender_channel = sender_channel;
  m_sender_exchange = sender_exchange;
  m_row_exchange_writer.SetExchange(m_sender_exchange);

  return false;
}

// Walk all query block tree to call @param func, get from inner query
// expressions if @param from_inner_subselects is valid otherwise get them from
// @param from.
template <class Func>
bool walk_query_block(THD *thd, Query_block *query_block, Query_block *from,
                      Func &&func) {
  if (func(thd, query_block, from)) return true;
  return for_each_inner_expressions(
      query_block, from,
      [thd, func](Query_expression *inner_unit,
                  Query_expression *from_inner_unit) {
        for (auto *inner = inner_unit->first_query_block(),
                  *from_inner = from_inner_unit->first_query_block();
             inner && from_inner; inner = inner->next_query_block(),
                  from_inner = from_inner->next_query_block()) {
          if (walk_query_block(thd, inner, from_inner, func)) return true;
        }
        return false;
      });
}

static bool new_query_expression(LEX *lex, Query_block *parent,
                                 Query_expression *from) {
  auto *from_query_block = from->first_query_block();
  auto *query_block = lex->new_query(parent);
  if (!query_block) return true;
  query_block->master_query_expression()->m_id =
      from_query_block->master_query_expression()->m_id;

  for (from_query_block = from_query_block->next_query_block();
       from_query_block;
       from_query_block = from_query_block->next_query_block()) {
    if (!lex->new_union_query(query_block,
                              from_query_block == from->union_distinct))
      return true;
  }

  return false;
}

static bool is_clone_excluded(const Query_expression *query_expression,
                              QueryExpressions *excludes) {
  if (!excludes) return false;
  for (auto &unit : *excludes) {
    if (query_expression == &unit) return true;
  }
  return false;
}

static bool init_query_block_tree_from(THD *cur_thd, Query_block *query_block,
                                       Query_block *from_query_block,
                                       QueryExpressions *excludes) {
  return walk_query_block(
      cur_thd, query_block, from_query_block,
      [excludes](THD *thd, Query_block *cur, Query_block *from) {
        if (add_tables_to_query_block(thd, cur, from->leaf_tables)) return true;
        LEX *lex = thd->lex;
        auto *from_inner_units = cur->m_inner_query_expressions_clone_from;
        if (from_inner_units) {
          for (auto &inner_unit : *from_inner_units) {
            if (new_query_expression(lex, cur, &inner_unit)) return true;
          }
        } else {
          for (auto *inner_unit = from->first_inner_query_expression();
               inner_unit; inner_unit = inner_unit->next_query_expression()) {
            if (is_clone_excluded(inner_unit, excludes)) continue;
            if (new_query_expression(lex, cur, inner_unit)) return true;
          }
        }
        return false;
      });
}

/// Other SQLEngine nodes maybe change table definition e.g. drop index. It may
/// cause problems when cloning plan.
static bool CheckTableDefChange(TABLE *table, TABLE *from) {
  if (table->schema_version == from->schema_version &&
      table->tindex_id == from->tindex_id)
    return false;

  my_error(ER_TABLE_IS_OUT_OF_DATE, MYF(0), from->s->db.str,
           from->s->table_name.str, from->schema_version, table->schema_version,
           table->tindex_id);
  return true;
}


bool clone_leaf_tables(THD *thd, Query_block *query_block, Query_block *from) {
  // Item_field clone depends on this
  if (query_block->setup_tables(thd, query_block->get_table_list(), false))
    return true;

  // Clone table attributes from @from tables
  for (TABLE_LIST *tl = query_block->leaf_tables, *ftl = from->leaf_tables;
       tl && ftl; tl = tl->next_leaf, ftl = ftl->next_leaf) {
    tl->m_id = ftl->m_id;
    tl->set_tableno(ftl->tableno());
    tl->set_map(ftl->map());
    TABLE *table = tl->table;
    TABLE *ftable = ftl->table;
    assert(table);

    if (CheckTableDefChange(table, ftable)) return true;

    // record of const table has be filled during query optimization.
    if (ftable->const_table) {
      table->const_table = true;
      memcpy(table->record[0], ftable->record[0], ftable->s->reclength);
    }
    if (ftable->is_nullable()) table->set_nullable();
    if (ftable->has_null_row()) table->set_null_row();

    // XXX Tranditional explain needs these
    table->covering_keys = ftable->covering_keys;
    table->reginfo.not_exists_optimize = ftable->reginfo.not_exists_optimize;
    if (ftable->part_info) {
      bitmap_clear_all(&table->part_info->read_partitions);
      bitmap_copy(&table->part_info->read_partitions,
                  &ftable->part_info->read_partitions);
    }
    // XXX The materialized derived table clone
  }

  return false;
}

bool PartialExecutor::PrepareQueryPlan(PartialExecutorContext *context) {
  THD *thd = m_thd;
  LEX *lex = thd->lex;

  if (lex_start(thd)) return true;

  lex->is_partial_plan = true;

  // Inherit some properties of leader
  //
  // The sql_command to sync up locking behavior etc. The storage like innodb
  // depends on this.
  lex->sql_command = context->sql_command;
  lex->is_explain_analyze = context->is_explain_analyze;
  if (lex->is_explain_analyze &&
      !(lex->explain_format = new (thd->mem_root) Explain_format_tree))
    return true;

  auto *from_query_block = m_query_plan->QueryBlock();
  auto *from_join = from_query_block->join;
  auto *query_result = new (thd->mem_root) Query_result_to_collector(
      &m_row_exchange_writer, &from_join->tmp_table_param);

  DBUG_EXECUTE_IF("pq_simulate_worker_prepare_query_plan_error_1", {
    my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 2);
    query_result = nullptr;
  });

  if (!query_result) return true;

  lex->result = query_result;
  auto *query_block = lex->query_block;

  auto *unit = lex->unit, *from_unit = m_query_plan->QueryExpression();

  query_block->m_inner_query_expressions_clone_from =
      &m_query_plan->PushdownInnerQueryExpressions();
  QueryExpressions exclude_inners;
  for (auto &cached_subselect : m_query_plan->CachedSubqueries()) {
    if (exclude_inners.push_back(cached_subselect.subselect()->unit))
      return true;
  }
  if (init_query_block_tree_from(thd, query_block, from_query_block,
                                 &exclude_inners))
    return true;

  if (open_tables_for_query(thd, lex->query_tables, 0) ||
      lock_tables(thd, lex->query_tables, lex->table_count, 0))
    return true;

  DBUG_EXECUTE_IF("pq_simulate_worker_prepare_query_plan_error_2", {
    my_error(ER_DA_UNKNOWN_ERROR_NUMBER, MYF(0), 3);
    return true;
  });

  if (walk_query_block(thd, query_block, from_query_block, clone_leaf_tables))
    return true;

  ItemRefCloneResolver ref_clone_resolver(thd->mem_root);
  PartialItemCloneContext clone_context(thd, query_block, &ref_clone_resolver,
                                        context->find_user_var_entry,
                                        &m_query_plan->CachedSubqueries());

  if (unit->clone_from(thd, from_unit, &clone_context, true)) return true;

  if (clone_context.final_resolve_refs()) return true;

  if (query_block->change_query_result(thd, query_result, nullptr)) return true;

  unit->set_query_result(query_result);

  thd->lex->set_current_query_block(query_block);
  thd->query_plan.set_query_plan(lex->sql_command, lex, false);

  // Now attach table parallel scan if it's available
  if (m_query_plan->HasParallelScan() && AttachTablesParallelScan())
    return true;

  return false;
}

bool PartialExecutor::AttachTablesParallelScan() {
  auto &psinfo = m_query_plan->GetParallelScanInfo();
  THD *thd = m_thd;
  auto *query_block = thd->lex->query_block;
  auto *leaf_tables = query_block->leaf_tables;

  assert(leaf_tables);

  TABLE *table = nullptr;
  for (TABLE_LIST *tl = leaf_tables; tl; tl = tl->next_leaf) {
    if (tl->is_identical(psinfo.table->pos_in_table_list)) {
      table = tl->table;
      break;
    }
  }
  assert(table);

  table->parallel_scan_handle = psinfo.table->parallel_scan_handle;
  int res;
  if ((res = table->file->attach_parallel_scan(table->parallel_scan_handle)) !=
      0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

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
    assert(!m_query_plan_timing_data.get());
    m_query_plan_timing_data.reset(new std::string);
    // Here the size of one node is 32 bytes. this prevents it save data in
    // inline buffer inside of std::string
    m_query_plan_timing_data->reserve(128);
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

   m_sender_channel->Close();

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
  //  LAST_INSERT_ID() push down need this, see class
  //  Item_func_last_insert_id;  
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

void CreateBroadcastResult(THD *thd, comm::RowExchangeWriter *writer,
                           Temp_table_param *tmp_table_param) {
  thd->lex->result =
      new (thd->mem_root) Query_result_to_collector(writer, tmp_table_param);
}

TABLE *GetBroadcastResultTable(THD *thd) {
  auto *result = (Query_result_to_collector *)thd->lex->result;
  return result->table();
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

void JOIN::do_each_qep_exec_state(
    std::function<void(pq::QEP_execution_state &)> func) {
  if (!qep_execution_state) return;
  for (auto &qes : *qep_execution_state) func(qes);
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
