#include "sql/parallel_query/distribution.h"

#include "sql/parallel_query/local_worker.h"
#include "sql/parallel_query/planner.h"
#include "sql/parse_tree_hints.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "sql/tdsql/parallel/parallel_defs.h"

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
  ulong weight() const override { return NumOfRegion(); }
  bool SameWith(const Node *other) override {
    return m_tdstore_addr ==
           down_cast<const TDStoreNode *>(other)->m_tdstore_addr;
  }
  std::size_t NumOfRegion() const { return m_num_region; }
  std::string &addr() { return m_tdstore_addr; }
  void IncRegions(size_t regions) { m_num_region += regions; }

 private:
  std::string m_tdstore_name;
  std::string m_tdstore_addr;
  std::size_t m_num_region{0};
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
  bool InitExecution(PartialPlan *partial_plan,
                     uint num_workers [[maybe_unused]],
                     NodeArray *exec_nodes [[maybe_unused]]) override {
    auto &psinfo = partial_plan->GetParallelScanInfo();
    auto *table = psinfo.table;
    int res;

    psinfo.scan_desc.scan_range_assign_fn = nullptr;
    if ((res = table->file->init_parallel_scan(&table->parallel_scan_handle,
                                               &psinfo.suggested_ranges,
                                               &psinfo.scan_desc)) != 0) {
      table->file->print_error(res, MYF(0));
      return true;
    }
    return false;
  }
};

class TDStoreAdapter : public Adapter {
 public:
  TableDist *GetTableDist(THD *thd, TABLE *table, uint keynr,
                          key_range *min_key, key_range *max_key,
                          bool is_ref_or_null) override {
    auto *table_desc = new (thd->mem_root) TDStoreTableDist;
    if (table_desc->Init(thd->mem_root)) return nullptr;

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

    return new (thd->mem_root) TDStorePartialDistPlan;
  }

  Worker *CreateParallelWorker(uint worker_id, comm::Event *state_event,
                               PartialPlan *plan,
                               TABLE *collector_table [[maybe_unused]],
                               NodeArray *exec_nodes
                               [[maybe_unused]]) override {
    assert(exec_nodes->size() == 0 ||
           (exec_nodes->size() == 1 && exec_nodes->at(0)->IsLocal()));
    return CreateLocalWorker(worker_id, state_event, plan);
  }
};

Adapter *CreateTDStoreAdapter(MEM_ROOT *mem_root) {
  return new (mem_root) dist::TDStoreAdapter;
}

}  // namespace dist
}  // namespace pq
