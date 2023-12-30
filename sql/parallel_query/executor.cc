#include "sql/parallel_query/executor.h"
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
      : m_partial_plan(partial_plan),
        m_workers(num_workers) {
  mysql_mutex_init(PSI_INSTRUMENT_ME, &m_worker_state_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_INSTRUMENT_ME, &m_worker_state_cond);
}

Collector::~Collector() {
  if (m_table) {
    close_tmp_table(m_table);
    free_tmp_table(m_table);
  }
  for (auto *worker : m_workers) destroy(worker);
  // tmp_buffer in Sort_param needs to destroy because it may contain alloced
  // memory.
  destroy(m_merge_sort);

  destroy(m_merge_sort);
  destroy(m_row_exchange_reader);
  destroy(m_row_exchange);

  mysql_mutex_destroy(&m_worker_state_lock);
  mysql_cond_destroy(&m_worker_state_cond);
}

bool Collector::CreateCollectorTable() {
  Query_block *block = m_partial_plan->QueryBlock();
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
  if (!(m_row_exchange = new (mem_root)
            RowExchange(m_workers.size(), RowExchange::Type::RECEIVER)))
    return true;

  m_row_exchange_reader =
      m_merge_sort
          ? new (mem_root)
                RowExchangeMergeSortReader(m_row_exchange, m_merge_sort)
          : new (mem_root) RowExchangeReader(m_row_exchange);

  if (!m_row_exchange_reader) return true;

  return false;
}

bool Collector::Init(THD *thd) {
  uint i = 0;
  for (auto *&worker : m_workers) {
    worker = new (thd->mem_root) Worker(
        thd, i++, m_partial_plan, &m_worker_state_lock, &m_worker_state_cond);
    if (!worker || worker->Init()) return true;
  }

  if (CreateRowExchange(thd->mem_root)) return true;

  if (m_row_exchange->Init(thd->mem_root, [this](uint index) {
        return m_workers[index]->MessageQueue();
      }))
    return true;

  bool has_failed_worker;
  if (LaunchWorkers(has_failed_worker)) return true;

  MY_BITMAP closed_queues;
  if (has_failed_worker) {
    if (bitmap_init(&closed_queues, nullptr, NumWorkers())) return true;
    // Close the queues which workers are launched failed.
    i = 0;
    for (auto *worker : m_workers) {
      if (worker->IsStartFailed()) bitmap_set_bit(&closed_queues, i++);
    }
  }
  // Initialize row exchange reader after workers are started. The reader with
  // merge sort do a block read in Init().
  bool res = m_row_exchange_reader->Init(
      thd, has_failed_worker ? &closed_queues : nullptr);

  if (has_failed_worker) bitmap_free(&closed_queues);
  return res;
}

bool Collector::LaunchWorkers(bool &has_failed_worker) {
  bool all_start_error = true;
  int error = 0;
  has_failed_worker = false;
  for (auto *worker : m_workers) {
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

void Collector::TerminateWorkers(THD *) {
  for (auto *worker : m_workers) worker->Terminate();
  mysql_mutex_lock(&m_worker_state_lock);

  // Wait all workers to exit.
  while (true) {
    uint left_workers = m_workers.size();
    for (auto *worker : m_workers) {
      if (!worker->IsRunning()) --left_workers;
    }
    if (left_workers == 0) break;
    mysql_cond_wait(&m_worker_state_cond, &m_worker_state_lock);
  }

  mysql_mutex_unlock(&m_worker_state_lock);
}

int Collector::Read(THD *thd, uchar *buf, ulong reclength) {
  uchar *dataptr;
  RowExchangeResult res = m_row_exchange_reader->Read(thd, &dataptr);
  switch (res) {
    case RowExchangeResult::SUCCESS:
      memcpy(buf, dataptr, reclength);
      return 0;
    case RowExchangeResult::OOM:
      return HA_ERR_OUT_OF_MEM;
    case RowExchangeResult::KILLED:
      return HA_ERR_QUERY_INTERRUPTED;
    case RowExchangeResult::END:
      return HA_ERR_END_OF_FILE;
    case RowExchangeResult::NONE:
    case RowExchangeResult::DETACHED:
    case RowExchangeResult::WOULD_BLOCK:
    case RowExchangeResult::ERROR:
      assert(0);
  }

  return 0;
}

Diagnostics_area *Collector::combine_workers_stmt_da(THD *thd,
                                                     ha_rows *found_rows) {
  Diagnostics_area *cond_da = nullptr;
  bool is_error = false;
  for (auto *worker : m_workers) {
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

void Collector::End(THD *thd, ha_rows *found_rows) {
  TerminateWorkers(thd);
  Diagnostics_area *combined_da = combine_workers_stmt_da(thd, found_rows);
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
  return m_partial_plan->Join();
}

AccessPath *Collector::PartialRootAccessPath() const {
  return m_partial_plan->Join()->root_access_path();
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
