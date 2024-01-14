#include "sql/parallel_query/distribution.h"

#include <algorithm>
#include "sql/parallel_query/plan_deparser.h"
#include "sql/parallel_query/planner.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

///spider distribution implementation

class st_spider_conn;
typedef st_spider_conn SPIDER_CONN;
namespace pq {
SPIDER_CONN *GetSpiderConn(THD *thd, TABLE *table, uint shardid);
std::string GetSpiderNodeKey(TABLE *table, uint shardid);
Worker *CreateMySQLClientWorker(uint id, comm::Event *state_event, THD *thd,
                                TABLE *collector_table, PlanDeparser *deparser,
                                SPIDER_CONN *conn);
std::string PrintMySQLClientWorkerTiming(Worker *worker,
                                         const std::string &worker_desc);
namespace dist {
/// spider table data nodes
class SpiderNode : public Node {
 public:
  SpiderNode(TABLE *table, uint shardid) : m_table(table), m_shardid(shardid) {
    m_conn_key = GetSpiderNodeKey(m_table, m_shardid);
  }
  std::string name() override { return m_conn_key; }
  bool IsLocal() override { return false; }
  ulong weight() const override { return 0; }
  bool SameWith(const Node *other) override {
    auto *spider_other = down_cast<const SpiderNode *>(other);
    return m_conn_key == spider_other->m_conn_key;
  }
  SPIDER_CONN *GetConn(THD *thd) {
    return GetSpiderConn(thd, m_table, m_shardid);
  }
  std::string GetSpiderKey() { return m_conn_key; }

  // Used for EXPLAIN ANALYZE only;
  Worker *m_exec_worker{nullptr};

 private:
  TABLE *m_table;
  uint m_shardid;
  std::string m_conn_key;
};

class SpiderTableDist : public TableDist {
 public:
  bool Init(MEM_ROOT *mem_root) override {
    m_store_nodes.init(mem_root);
    return false;
  }
  virtual NodeArray *GetStoreNodes() override { return &m_store_nodes; }

  bool PushStoreNode(MEM_ROOT *mem_root, TABLE *table, uint shardid) {
    SpiderNode *node = new (mem_root) SpiderNode(table, shardid);
    return !node || m_store_nodes.push_back(node);
  }

  bool IsCompatibleWith(TableDist *other) override {
    auto *spider_other = down_cast<const SpiderTableDist *>(other);
    if (m_store_nodes.size() != 1) return false;
    return std::equal(m_store_nodes.begin(), m_store_nodes.end(),
                      spider_other->m_store_nodes.begin(),
                      spider_other->m_store_nodes.end(),
                      [](auto &a, auto &b) { return a->SameWith(b); });
  }

 private:
  NodeArray m_store_nodes;
};

class SpiderPartialDistPlan : public PartialDistPlan {
 public:
  SpiderPartialDistPlan(NodeArray *exec_nodes, PlanDeparser *plan_deparser)
      : PartialDistPlan(exec_nodes), m_plan_deparser(plan_deparser) {}

  bool InitExecution(PartialPlan *, uint workers [[maybe_unused]]) override {
    // Number of worker must equal to execution nodes.
    assert(workers == m_exec_nodes->size());
    if (!m_plan_deparser->IsDeparsed() && m_plan_deparser->deparse())
      return true;

    return false;
  }

  bool ExplainPlan(std::vector<std::string> *description,
                   bool *hide_plan_tree) override {
    *hide_plan_tree = true;
    if (!m_plan_deparser->IsDeparsed() && m_plan_deparser->deparse())
      return true;

    /*
      The statement will be printed after (cost=... rows=...) for the parallel
      query. It will look like bebow:
      -> Gather (slice: 1, workers: X) (cost=N.NNN rows=NNN)
      Statement: select `t1`.`id` AS `id`,`t1`.`a` AS `a` from `t1`
      Execution datasets: s1, s2
    */
    std::string str = "Statement: ";
    str += m_plan_deparser->statement()->c_ptr();
    description->push_back(std::move(str));

    THD *thd = m_plan_deparser->thd();
    if (!thd->lex->is_explain_analyze) {
      std::string nodes_str = "Execution datasets: ";
      for (auto *node : *m_exec_nodes) {
        auto *exec_node = down_cast<SpiderNode *>(node);
        nodes_str += exec_node->GetSpiderKey();
        nodes_str += ", ";
      }
      nodes_str.resize(nodes_str.size() - 2);
      description->push_back(std::move(nodes_str));
      return false;
    }
    /*
      The statement and worker info will be printed after
      (cost=... rows=...) for the parallel query. It will look like bebow:
      -> Gather (slice: 1, workers: 2) (cost=N.NNN rows=NNN)
      Statement: select `t1`.`id` AS `id`,`t1`.`a` AS `a` from `t1`
      Worker(1) (dataset = s1) (actual time=0.094..32.49 rows=48 loops=1)
      Worker(2) (dataset = s2) (actual time=0.009..34.57 rows=52 loops=1)
    */
    for (auto *node : *m_exec_nodes) {
      auto *exec_node = down_cast<SpiderNode *>(node);
      std::string timing = PrintMySQLClientWorkerTiming(
          exec_node->m_exec_worker, exec_node->GetSpiderKey());
      description->push_back(std::move(timing));
    }

    return false;
  }

  PlanDeparser *Deparser() const { return m_plan_deparser; }

 private:
  PlanDeparser *m_plan_deparser;
};

static constexpr uint spider_non_shardid = UINT_MAX;

class SpiderAdapter : public Adapter {
 public:
  TableDist *GetTableDist(THD *thd, TABLE *table, uint, key_range *,
                          key_range *, bool) override {
    SpiderTableDist *table_desc;
    if (!(table_desc = new (thd->mem_root) SpiderTableDist)) return nullptr;

    if (table_desc->Init(thd->mem_root)) return nullptr;
    auto shard_type = table->s->tdsql_table_type;
    if (shard_type == tdsql::ddl::TD_NOSHARD_TABLE ||
        shard_type == tdsql::ddl::TD_ALLSET_TABLE) {
      if (table_desc->PushStoreNode(thd->mem_root, table, spider_non_shardid))
        return nullptr;

      return table_desc;
    }

    auto *part_info = table->part_info;
    for (uint part_id = part_info->get_first_used_partition();
         part_id < MY_BIT_NONE;
         part_id = part_info->get_next_used_partition(part_id)) {
      if (table_desc->PushStoreNode(thd->mem_root, table, part_id))
        return nullptr;
    }
    return table_desc;
  }

  bool NeedParallelScan() const override { return false; }

  const char *TableRefuseParallel(TABLE *table) override {
    // Here reuse parallel this flag to identify SPIDER table
    if (!(table->file->ha_table_flags() & HA_CAN_PARALLEL_SCAN))
      return "include_non_distribution_table";

    return nullptr;
  }

  PartialDistPlan *DoMakePartialDistPlan(PartialPlan *partial_plan,
                                         dist::NodeArray *exec_nodes
                                         [[maybe_unused]]) const override {
    THD *thd = partial_plan->thd();
    auto *plan_deparser =
        new (thd->mem_root) PlanDeparser(partial_plan->QueryBlock());
    if (!plan_deparser) return nullptr;

    assert(exec_nodes->size() > 0);
    return new (thd->mem_root) SpiderPartialDistPlan(exec_nodes, plan_deparser);
  }

  Worker *CreateParallelWorker(uint worker_id, comm::Event *state_event,
                               PartialPlan *plan, TABLE *collector_table,
                               NodeArray *exec_nodes) override {
    THD *thd = plan->thd();
    SpiderNode *node = down_cast<SpiderNode *>(exec_nodes->at(worker_id - 1));
    auto *dist_plan = down_cast<SpiderPartialDistPlan *>(plan->DistPlan());
    auto *worker =
        CreateMySQLClientWorker(worker_id, state_event, thd, collector_table,
                                dist_plan->Deparser(), node->GetConn(thd));
    if (unlikely(thd->lex->is_explain_analyze)) node->m_exec_worker = worker;
    return worker;
  }
};

Adapter *CreateSpiderAdapter(MEM_ROOT *mem_root) {
  return new (mem_root) dist::SpiderAdapter;
}

}  // namespace dist
}  // namespace pq
