#include "sql/parallel_query/distribution.h"

#include "sql/parallel_query/local_worker.h"
#include "sql/parallel_query/planner.h"
#include "sql/parse_tree_hints.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "sql/tdsql/parallel/parallel_defs.h"

#include <iostream>
#include <unordered_map>
#include "storage/rocksdb/tdsql/parallel_query.h"
#include "sql/parallel_query/brpc_stream_worker.h"
#include "sql/tdsql/parallel/parallel_defs.h"
#include "sql/tdsql/rpc_cntl/td_client.h"
#include "sql/opt_trace.h"

/// TDStore distribution implementation

namespace pq {
namespace dist {
/// TDStore table data nodes
class TDStoreNode : public Node {
 public:
  TDStoreNode() = default;
  TDStoreNode(const std::string &name, const std::string &addr,
              std::size_t regions)
      : m_tdstore_name(name), m_tdstore_addr(addr), m_num_region(regions) {}

  std::string name() override { return m_tdstore_name; }
  bool IsLocal() override { return m_tdstore_addr.empty(); }
  void SetLocal() { m_tdstore_addr.clear(); }
  ulong weight() const override { return NumOfRegion(); }
  bool SameWith(const Node *other) override {
    return m_tdstore_addr ==
           down_cast<const TDStoreNode *>(other)->m_tdstore_addr;
  }
  std::size_t NumOfRegion() const { return m_num_region; }
  std::string &addr() { return m_tdstore_addr; }
  void IncRegions(size_t regions) { m_num_region += regions; }

  void SetNumOfRegion(std::size_t num) { m_num_region = num; }
  void IncNumOfRegion() { m_num_region++; }

  std::string &Address() { return m_tdstore_addr; }
  void SetAddress(std::string addr) { m_tdstore_addr = addr; }

  BrpcGroupDesc *GetBrpcGroupDesc() { return &m_brpc_desc; }

 private:
  std::string m_tdstore_name;
  std::string m_tdstore_addr;
  std::size_t m_num_region{0};

  // desc all jobs relate to current node
  class BrpcGroupDesc m_brpc_desc;
};

class TDStoreTableDist : public TableDist {
 public:
  bool Init(MEM_ROOT *mem_root) override {
    m_store_nodes.init(mem_root);
    return false;
  }

  bool PushStoreNode(MEM_ROOT *mem_root, const std::string &node_name,
                     const std::string &node_addr, std::size_t regions) {
    // For partition tables, it can get same node from different partition.
    for (auto *nod : m_store_nodes) {
      auto *tnod = down_cast<TDStoreNode *>(nod);
      if (tnod->addr() == node_addr) {
        tnod->IncRegions(regions);
        return false;
      }
    }

    auto node = new (mem_root) TDStoreNode(node_name, node_addr, regions);
    return !node || m_store_nodes.push_back(node);
  }

  virtual NodeArray *GetStoreNodes() override { return &m_store_nodes; }

 private:
  NodeArray m_store_nodes;
};

class TDStorePartialDistPlan : public PartialDistPlan {
 public:
  void SplitWorker(uint num_workers) {
    uint total_jobs = 0;
    uint worker_left = num_workers;
    for (auto n : *m_exec_nodes) {
      TDStoreNode *node = (TDStoreNode *)n;
      uint job = node->NumOfRegion();
      total_jobs += job;
      // at least one worker for exec node which has jobs
      if (job) {
        BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
        group_desc->worker_num_ = 1;
        worker_left -= 1;
      }
    }

    float worker_per_job = worker_left / total_jobs;

    for (auto n : *m_exec_nodes) {
      TDStoreNode *node = (TDStoreNode *)n;
      // do not assign worker if exec node has no job
      uint job = node->NumOfRegion();
      if (!job) continue;

      uint assign_num = std::min((uint)(job * worker_per_job), worker_left);
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      group_desc->worker_num_ += assign_num;
      worker_left -= assign_num;
      if (!worker_left) break;
    }

    // assign left worker due to fractional arithmetic
    uint index = 0;
    uint node_size = m_exec_nodes->size();
    while (worker_left) {
      TDStoreNode *node = (TDStoreNode *)(m_exec_nodes->at(index % node_size));
      if (node->NumOfRegion()) {
        BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
        group_desc->worker_num_ += 1;
        worker_left--;
      }
      index++;
    }
  }

  // setup scan_range_assign_fn
  void SerializePrev(PartialPlan *partial_plan, uint num_workers) {
    auto &psinfo = partial_plan->GetParallelScanInfo();

    THD *thd = partial_plan->thd();
    bool can_serialize =
        tdsql::TestSerializePlan(thd, partial_plan->QueryBlock());

    // see MyRocksParallelScan::Init,
    // do not support split region when is_ref_or_null
    // TODO:support is_ref_or_null
    if (!can_serialize || psinfo.scan_desc.is_ref_or_null) {
      // just use local worker
      psinfo.scan_desc.scan_range_assign_fn = nullptr;

      // we use m_exec_nodes to start worker,so just left one local exec node
      m_exec_nodes->resize(1);
      TDStoreNode *node = (TDStoreNode *)m_exec_nodes->at(0);
      node->SetAddress(tdsql_rpc_addr);
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      group_desc->worker_num_ = num_workers;
      return;
    }

    uint index = 0;
    for (auto n : *m_exec_nodes) {
      TDStoreNode *node = (TDStoreNode *)n;

      // REMOTE_SCHED(DEBUG ONLY) --> use brpc worker even for local region
      if ((thd->variables.tdsql_parallel_worker_scheduling != REMOTE_SCHED) &&
          tdsql::IsLocalTDStoreAddr(node->Address())) {
        has_local_node_ = true;
        local_node_index_ = index;
      }
      address_index_map_[((TDStoreNode *)n)->Address()] = index++;

      // scan_range_assign_fn will recalulate exact regions
      node->SetNumOfRegion(0);
    }

    psinfo.scan_desc.scan_range_assign_fn =
        [this](parallel_scan_range_info arg) -> bool {
      tdsql::tdstore_pscan_range_info *range_info =
          (tdsql::tdstore_pscan_range_info *)arg;
      const std::string *addr = range_info->node_addr;
      auto it = address_index_map_.find(*addr);
      bool exist = (it != address_index_map_.end());

      if (has_local_node_) {
        // use local node to deal local regions and all regions not in
        // m_exec_nodes
        if (tdsql::IsLocalTDStoreAddr(*addr) || (!exist)) {
          TDStoreNode *node =
              (TDStoreNode *)(m_exec_nodes->at(local_node_index_));
          node->IncNumOfRegion();
          return false;
        }
      }

      // use first exec node to deal with all regions not in m_exec_nodes
      uint i = 0;
      if (exist) i = it->second;
      TDStoreNode *node = (TDStoreNode *)(m_exec_nodes->at(i));
      node->IncNumOfRegion();

      // add job
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      group_desc->has_remote_jobs_ = true;
      push_down::ScanJob *scan_job = group_desc->scan_info_.add_scan_job();
      scan_job->set_lower_bound(
          range_info->key_range->start_key().to_string(false /*hex*/));
      scan_job->set_upper_bound(
          range_info->key_range->end_key().to_string(false /*hex*/));
#ifndef NDEBUG
      scan_job->set_is_null(false);
#endif
      return true;
    };
  }

  // split num_workers based on REAL jobs
  // serialize plan if need
  bool SerializePost(PartialPlan *partial_plan, uint num_workers) {
    // fast path
    if (m_exec_nodes->size() == 1) {
      TDStoreNode *node = (TDStoreNode *)m_exec_nodes->at(0);
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      group_desc->worker_num_ = num_workers;

      // no remote job  --> no need to serialize
      if (!group_desc->has_remote_jobs_) {
        LogDebug("only one local exec,no need to serialize");
        return false;
      }
    } else
      // m_exec_nodes may have exec_node without jobs
      SplitWorker(num_workers);

    LogDebug("exist remote jobs,serialize plan");

    THD *thd = partial_plan->thd();
    if (tdsql::SerializePlan(thd, partial_plan->QueryBlock(), &plan_data_)) {
      LogError("SerializePlan fail");
      return true;
    }

    auto &psinfo = partial_plan->GetParallelScanInfo();
    auto &scan_desc = psinfo.scan_desc;

    // connection_id + plan_id(address) --> unique id in cluster
    // BrpcStreamPartialExecutor is destructed after SendStat,so add query_id
    //
    //  sql1
    //  sql2, may reuse plan_id(address),BrpcStreamPartialExecutor with sql1 may
    //  not destructed already
    uint64 query_connection_id =
        ((uint64)GetConnId() << 32) | (thd->query_id & 0xffffffff);

    uint i = 0;
    for (auto n : *m_exec_nodes) {
      TDStoreNode *node = (TDStoreNode *)n;
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      group_desc->data_ = &plan_data_;
      group_desc->tdstore_addr_ = node->Address();

      group_desc->query_connection_id_ = query_connection_id;
      group_desc->plan_id_ = reinterpret_cast<ulong>(partial_plan);

      group_desc->scan_info_.set_keynr(scan_desc.keynr);
      group_desc->scan_info_.set_is_ref_or_null(scan_desc.is_ref_or_null);
      group_desc->scan_info_.set_key_used(scan_desc.key_used);
      group_desc->scan_info_.set_is_asc(scan_desc.is_asc);
      group_desc->scan_info_.set_tableno(psinfo.table->pos_in_table_list->tableno());

      LogDebug("m_exec_nodes:%u,address:%s,jobs:%lu,remote jobs:%d,worker:%u",
               i++, node->Address().c_str(), node->NumOfRegion(),
               group_desc->scan_info_.scan_job().size(),
               group_desc->worker_num_);
    }

    return false;
  }

  void PrintJobTrace(uint num_workers, TABLE_LIST *table) {
    THD *thd = current_thd;
    Opt_trace_context *ctx = &thd->opt_trace;
    if (!ctx->is_started()) return;
    Opt_trace_object trace_wrapper(ctx);
    Opt_trace_object trace_jobs(ctx, "jobs_info");
    trace_jobs.add_utf8_table(table);

    trace_jobs.add("exec_nodes", m_exec_nodes->size());
    trace_jobs.add("all workers", num_workers);

    Opt_trace_array trace_nodes(ctx, "nodes");
    for (auto n : *m_exec_nodes) {
      Opt_trace_object trace_node(ctx);
      TDStoreNode *node = (TDStoreNode *)n;
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();
      if (!group_desc->has_remote_jobs_) {
        trace_node.add("local", true);
        trace_node.add("local_jobs", node->NumOfRegion());
      } else {
        trace_node.add("local", false);
        trace_node.add("remote_jobs", group_desc->scan_info_.scan_job().size());
      }

#ifndef NDEBUG
      trace_node.add_utf8("node_addr", node->Address().c_str(),
                          node->Address().size());
#endif
      trace_node.add("worker_num", group_desc->worker_num_);
    }
  }

  using PartialDistPlan::PartialDistPlan;
  bool InitExecution(PartialPlan *partial_plan, uint num_workers) override {
    SerializePrev(partial_plan,num_workers);

    auto &psinfo = partial_plan->GetParallelScanInfo();
    auto *table = psinfo.table;
    int res;

    ulong suggested_ranges_backup = psinfo.suggested_ranges;

    if ((res = table->file->init_parallel_scan(&table->parallel_scan_handle,
                                               &psinfo.suggested_ranges,
                                               &psinfo.scan_desc)) != 0) {
      table->file->print_error(res, MYF(0));
      return true;
    }

    // just for mtr test,keep suggested_ranges stable
    if (tdsql::MtrMode()) {
      psinfo.suggested_ranges = suggested_ranges_backup;
    }

    if (SerializePost(partial_plan, num_workers)) return true;

    PrintJobTrace(num_workers, table->pos_in_table_list);

    return false;
  }

  // used to speed up search m_exec_nodes 
  std::unordered_map<std::string, uint> address_index_map_;
  std::vector<myrocks::MyRocksParallelScanJob> all_remote_jobs_;
  bool has_local_node_{false};
  uint local_node_index_{0};

  // used when need send plan to remote node
  push_down::SerializeData plan_data_;
};

class TDStoreAdapter : public Adapter {
 public:
  TableDist *GetTableDist(THD *thd, TABLE *table, uint keynr,
                          key_range *min_key, key_range *max_key,
                          bool is_ref_or_null) override {
    auto *table_desc = new (thd->mem_root) TDStoreTableDist;
    if (table_desc->Init(thd->mem_root)) return nullptr;

    // The table could be temporary table, e.g. semi join materialized table.
    if (table->file->ht->db_type != DB_TYPE_ROCKSDB) return table_desc;

    bool error = false;
    int res = table->file->get_distribution_info_by_scan_range(
        keynr, min_key, max_key, is_ref_or_null,
        [table_desc, thd, &error](handler::Dist_unit_info dui) {
          auto *tnsi = static_cast<tdsql::table_node_storage_info *>(dui);
          if (table_desc->PushStoreNode(thd->mem_root, tnsi->node_id,
                                        tnsi->node_addr, tnsi->num_region))
            error = true;
        });

    if (res != 0) {
      table->file->print_error(res, MYF(0));
      error = true;
    }

    if (error) {
      destroy(table_desc);
      return nullptr;
    }

    return table_desc;
  }

  bool NeedParallelScan() const override { return true; }

  void GetThresholdsForParallelScan(
      THD *thd, double *parallel_plan_cost_threshold,
      ulonglong *parallel_scan_records_threshold,
      ulong *parallel_scan_ranges_threshold) override {
    *parallel_plan_cost_threshold = thd->variables.parallel_plan_cost_threshold;
    *parallel_scan_records_threshold =
        thd->variables.parallel_scan_records_threshold;
    *parallel_scan_ranges_threshold =
        thd->variables.parallel_scan_ranges_threshold;
  }

  uint32 GetTableParallelDegree(THD *thd, TABLE_LIST *table,
                                bool *specified_by_hint) override {
    static_assert(parallel_disabled == PT_hint_parallel::parallel_disabled,
                  "parallel_disable must be same");
    static_assert(degree_unspecified == PT_hint_parallel::degree_unspecified,
                  "degree_unspecified must be same");

    uint32 degree = degree_unspecified;
    auto *lex = thd->lex;

    *specified_by_hint = true;
    if (table->opt_hints_table && table->opt_hints_table->parallel_scan) {
      auto *table_hint = table->opt_hints_table->parallel_scan;
      degree = table_hint->parallel_degree();
      if (degree != PT_hint_parallel::degree_unspecified) return degree;
    }

    if (table->opt_hints_qb && table->opt_hints_qb->get_parallel_hint()) {
      auto *qb_hint = table->opt_hints_qb->get_parallel_hint();
      degree = qb_hint->parallel_degree();
      if (degree != PT_hint_parallel::degree_unspecified) return degree;
    }

    if (lex->opt_hints_global && lex->opt_hints_global->parallel_hint) {
      auto *global_hint = lex->opt_hints_global->parallel_hint;
      degree = global_hint->parallel_degree();
      if (degree != PT_hint_parallel::degree_unspecified) return degree;
    }

    *specified_by_hint = false;
    return thd->variables.max_parallel_degree;
  }

  const char *TableRefuseParallel(TABLE *table) override {
    if (is_system_table(table->s->db.str, table->s->table_name.str))
      return "include_system_tables";
    return nullptr;
  }

  PartialDistPlan *DoMakePartialDistPlan(PartialPlan *partial_plan,
                                         dist::NodeArray *exec_nodes
                                         [[maybe_unused]]) const override {
    THD *thd = partial_plan->thd();

    return new (thd->mem_root) TDStorePartialDistPlan(exec_nodes);
  }

  // worker_id is from 1,see Collector::Init
  // node1: 10 worker, node2: 0 worker, node3: 5 worker, node4: 3 worker
  // [1,10] --> node1 , [11,15] --> node3, [16,18] --> node4
  TDStoreNode *GetNodesForWorkerId(NodeArray *exec_nodes, uint worker_id) {
    for (auto n : *exec_nodes) {
      TDStoreNode *node = (TDStoreNode *)n;
      BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();

      if (worker_id <= group_desc->worker_num_) return node;

      worker_id -= group_desc->worker_num_;
    }
    assert(0);
    return nullptr;
  }

  Worker *CreateParallelWorker(uint worker_id, comm::Event *state_event,
                               PartialPlan *plan,
                               TABLE *collector_table [[maybe_unused]],
                               NodeArray *exec_nodes) override {
    TDStoreNode *node = GetNodesForWorkerId(exec_nodes, worker_id);
    BrpcGroupDesc *group_desc = node->GetBrpcGroupDesc();

    LogDebug("create %s worker for worker_id:%u",
             group_desc->has_remote_jobs_ ? "brpc stream" : "local", worker_id);

    if (!group_desc->has_remote_jobs_)
      return CreateLocalWorker(worker_id, state_event, plan);
    else
      return CreateBrpcStreamWorker(worker_id, state_event, plan,
                                    node->GetBrpcGroupDesc());
  }
};

Adapter *CreateTDStoreAdapter(MEM_ROOT *mem_root) {
  return new (mem_root) dist::TDStoreAdapter;
}

}  // namespace dist
}  // namespace pq
