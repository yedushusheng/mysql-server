#include "sql/parallel_query/executor.h"

#include <chrono>
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/filesort.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/worker.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"
#include "sql/table.h"

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
  tmp_table_param->sum_func_count =
      join->tmp_table_param.sum_func_count;

  m_table = create_tmp_table(
      thd, tmp_table_param, block->fields, nullptr, false,
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

bool Collector::CreateRowExchange(MEM_ROOT *mem_root) {
  if (!(m_row_exchange = new (mem_root) RowExchange(m_workers.size())))
    return true;

  auto worker_exit_handler = [this](uint worker) {
    return HandleWorkerExited(worker);
  };
  m_row_exchange_reader =
      m_merge_sort ? new (mem_root) RowExchangeMergeSortReader(
                         m_row_exchange, m_merge_sort, worker_exit_handler)
                   : new (mem_root)
                         RowExchangeReader(m_row_exchange, worker_exit_handler);

  if (!m_row_exchange_reader) return true;

  return false;
}

bool Collector::InitParallelScan() {
  auto &psinfo = m_partial_plan->TablesParallelScan();
  auto *table = psinfo.table;
  ulong nranges = 100 * NumWorkers();
  int res;

  if ((res = table->file->init_parallel_scan(
           &table->parallel_scan_handle, &nranges, &psinfo.scan_desc)) != 0) {
    table->file->print_error(res, MYF(0));
    return true;
  }

  return false;
}

bool Collector::Init(THD *thd) {
  if (CreateRowExchange(thd->mem_root)) return true;

  // Here reserved 0 as leader's id. If you use Worker::m_id as a 0-based index,
  // you should use m_id - 1
  uint wid = 1;
  for (auto *&worker : m_workers) {
    worker = new (thd->mem_root) Worker(
        thd, wid++, partial_plan(), &m_worker_state_lock, &m_worker_state_cond);
    if (!worker || worker->Init(m_row_exchange_reader->Event())) return true;
  }

  if (m_row_exchange->Init(thd->mem_root, [this](uint index) {
        return m_workers[index]->message_queue();
      }))
    return true;

  // Do initialize parallel scan for tables before workers are started
  if (InitParallelScan()) return true;

  DEBUG_SYNC(thd, "before_launch_pqworkers");

  bool has_failed_worker;
  if (LaunchWorkers(has_failed_worker)) return true;

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
  bool res = m_row_exchange_reader->Init(
      thd->mem_root, has_failed_worker ? &closed_queues : nullptr, thd,
      [this](uint index) { return m_workers[index]->message_queue_event(); });

  if (has_failed_worker) bitmap_free(&closed_queues);
  return res;
}

void Collector::Reset() {
  // Reset could be called multiple times without Collector::Init() calling, see
  // subselect_hash_sj_engine::exec()
  if (!m_row_exchange) return;
  destroy(m_row_exchange_reader);
  m_row_exchange_reader = nullptr;
  destroy(m_row_exchange);
  m_row_exchange = nullptr;

  for (auto *&worker : m_workers) destroy(worker);
}

bool Collector::LaunchWorkers(bool &has_failed_worker) {
  bool all_start_error = true;
  int error = 0;
  has_failed_worker = false;

  for (auto *worker : m_workers) {
#if !defined(NDEBUG)
    worker->dbug_cs_stack_clone = &dbug_cs_stack_clone;
#endif
    int res = worker->Start();
    if (!all_start_error) continue;

    if (res == 0)
      all_start_error = false;
    else
      error = res;
  }
  has_failed_worker = (error != 0);

  if (all_start_error) {
    char errbuf[MYSQL_ERRMSG_SIZE];
      my_error(ER_STARTING_PARALLEL_QUERY_THREAD, MYF(0), error,
               my_strerror(errbuf, MYSQL_ERRMSG_SIZE, error));
  }

  return all_start_error;
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

bool Collector::HandleWorkerExited(uint windex) {
  assert(windex < m_workers.size());
  auto *worker = m_workers[windex];

  return worker->is_error();
}

int Collector::Read(THD *thd, uchar *buf, ulong reclength) {
  uchar *dataptr;
  auto res = m_row_exchange_reader->Read(thd, &dataptr);
  switch (res) {
    case RowExchange::Result::SUCCESS:
      memcpy(buf, dataptr, reclength);
      ++thd->status_var.pq_rows_exchanged;
      return 0;
    case RowExchange::Result::OOM:
      return HA_ERR_OUT_OF_MEM;
    case RowExchange::Result::KILLED:
      return HA_ERR_QUERY_INTERRUPTED;
    case RowExchange::Result::END:
      return HA_ERR_END_OF_FILE;
    case RowExchange::Result::ERROR:
#ifndef NDEBUG
    {
      bool found = false;
      for (auto *worker : m_workers) {
        if (worker->is_error()) {
          found = true;
        }
      }
      assert(found);
    }
#endif
      return -1;
  }

  return 0;
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
    Diagnostics_area *da = worker->stmt_da(&cur_found_rows, &cur_examined_rows);

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
    if (!worker) continue;
    auto *worker_thd = worker->thd();
    add_to_status(&thd->status_var, &worker_thd->status_var);
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
  da->set_error_status(combined_da->mysql_errno(),
                       combined_da->message_text(),
                       combined_da->returned_sqlstate());
  da->set_overwrite_status(old_ow_status);

  // reset XA state error according to new THD error.
  XID_STATE *xid_state = thd->get_transaction()->xid_state();
  if (xid_state) xid_state->set_error(thd);
}

JOIN *Collector::PartialJoin() const {
  return partial_plan()->Join();
}

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

RowIterator *NewFakeTimingIterator(THD *thd, Collector *collector) {
  return new (thd->mem_root) FakeTimingIterator(thd, collector);
}
}  // namespace pq

CollectorIterator::CollectorIterator(THD *thd, pq::Collector *collector,
                                     ha_rows *examined_rows)
    : TableRowIterator(thd, collector->CollectorTable()),
      m_collector(collector),
      m_record(table()->record[0]),
      m_examined_rows(examined_rows) {}

CollectorIterator::~CollectorIterator() {}

bool CollectorIterator::Init() {
  empty_record(table());
  return m_collector->Init(thd());
}

int CollectorIterator::Read() {
  int error;
  /* Read data from workers */
  if ((error = m_collector->Read(thd(), m_record, table()->s->reclength))) {
    // Worker exit with error, it has been processed in Collector::Read()
    if (error < 0) return 1;
    return HandleError(error);
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
