#ifndef PARALLEL_QUERY_DISTRIBUTION_H
#define PARALLEL_QUERY_DISTRIBUTION_H

#include <string>
#include "my_alloc.h"
#include "sql/mem_root_array.h"

class THD;
class Item;
class TABLE;
class TABLE_LIST;
struct key_range;

namespace pq {
class PartialPlan;
class Worker;
namespace comm {
class Event;
}

constexpr static uint32 parallel_disabled{0};
constexpr static uint32 degree_unspecified{UINT32_MAX};

namespace dist {

/// Table store node description
class Node {
 public:
  virtual ~Node() {}

  /// Display name by EXPLAIN
  virtual std::string name() = 0;
  /// Used for TDSQL 3, local thread use separable class.
  virtual bool IsLocal() = 0;
  /// Used for TDSQL 3, choose suitable execution nodes
  virtual ulong weight() const { return 0; }
  /// Same physical node with the other that from another table.
  virtual bool SameWith(const Node *other) = 0;
};
using NodeArray = Mem_root_array<Node *>;

/// Table distribution descriptor.
class TableDist {
 public:
  virtual ~TableDist() {}
  virtual bool Init(MEM_ROOT *mem_root) = 0;
  /// equality JOIN on dist key could be pushed down if two tables are
  /// compatible
  virtual bool IsCompatibleWith(TableDist *other [[maybe_unused]]) {
    return false;
  }

  virtual NodeArray *GetStoreNodes() = 0;
};
using TableDistArray = Mem_root_array<TableDist *>;

/// Distribution specific plan info for TDSQL 2.0 or 3.0
class PartialDistPlan {
 public:
  PartialDistPlan(NodeArray *exec_nodes) : m_exec_nodes(exec_nodes) {}
  virtual ~PartialDistPlan() {}
  /// This is called just before workers are started to initialize distribution
  /// specific plan.
  virtual bool InitExecution(PartialPlan *partial_plan, uint num_workers) = 0;
  /// Show custom messages in explain tree, set @param *hide_path_tree to
  /// true if dist system does not want to show partial plan tree.
  virtual bool ExplainPlan(std::vector<std::string> *description
                           [[maybe_unused]],
                           bool *hide_plan_tree [[maybe_unused]]) {
    return false;
  }

 protected:
  NodeArray *m_exec_nodes;
};

/// Adapter for distribution, describe capacities of specific distribution.
class Adapter {
 public:
  /// Provide a table distribution descriptor for a table
  virtual TableDist *GetTableDist(THD *thd, TABLE *table, uint keynr,
                                  key_range *min_key, key_range *max_key,
                                  bool is_ref_or_null) = 0;
  /// need parallel scan to do a distribution, for TDSQL 3.0
  virtual bool NeedParallelScan() const = 0;
  /// Get thresholds of variables that are used by plan choosing and the
  /// parallel has parallel scan table. The parallel plan without parallel scan
  /// table has no soft limits.
  virtual void GetThresholdsForParallelScan(
      THD *thd [[maybe_unused]],
      double *parallel_plan_cost_threshold [[maybe_unused]],
      ulonglong *parallel_scan_records_threshold [[maybe_unused]],
      ulong *parallel_scan_ranges_threshold [[maybe_unused]]) {}

  virtual uint32 GetTableParallelDegree(THD *thd [[maybe_unused]],
                                TABLE_LIST *table [[maybe_unused]],
                                bool *specified_by_hint [[maybe_unused]]) {
    return degree_unspecified;
  }
  virtual const char *TableRefuseParallel(TABLE *table [[maybe_unused]]) {
    return nullptr;
  }

  PartialDistPlan *MakePartialDistPlan(PartialPlan *partial_plan,
                                       dist::NodeArray *exec_nodes);
  /// Create new parallel worker, different workers for 2.5 and 3.0
  virtual Worker *CreateParallelWorker(uint worker_id, comm::Event *state_event,
                                       PartialPlan *plan,
                                       TABLE *collector_table,
                                       NodeArray *exec_nodes) = 0;

 private:
  /// Helper API for MakePartialDistPlan()
  virtual PartialDistPlan *DoMakePartialDistPlan(
      PartialPlan *partial_plan, dist::NodeArray *exec_nodes) const = 0;
};

Adapter *CreateSpiderAdapter(MEM_ROOT *mem_root);
}  // namespace dist
}  // namespace pq
#endif
