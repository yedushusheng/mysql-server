#ifndef PARALLEL_QUERY_BRPC_STREAM_EXECUTOR_H
#define PARALLEL_QUERY_BRPC_STREAM_EXECUTOR_H
#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>
#include "sql/parallel_query/brpc_stream_comm.h"
#include "sql/parallel_query/brpc_stream_connection.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/planner.h"
#include "sql/proto/push_down.pb.h"
#include "sql/proto/sqlengine.pb.h"
#include "sql/sql_class.h"
namespace pq {

class BrpcStreamPartialExecutorGroup;
// response to one brpc_stream_worker
class BrpcStreamPartialExecutor {
 public:
  BrpcStreamPartialExecutor(BrpcStreamPartialExecutorGroup *group,
                            THD *leader_thd, PartialPlan *plan);

  ~BrpcStreamPartialExecutor();

  bool InitChannel(comm::BrpcStreamConnectionPtr stream_conn);

  void SetKillFlag();

  void SendStat();

  // we need this in Init,before we deserialize our own thd
  // so use info from group thd
  bool IsExplainAnalyze();

  THD *get_thd() {
    return &thd_;
  }

  // table used to get job from,init in BrpcStreamPartialExecutorGroup
  TABLE *GroupTable();
  push_down::SerializeData *Data();

  PartialExecutor *Executor() { return &executor_; }

  // used by SQL_PLAN
  comm::RowExchange *SenderExchange() { return &sender_exchange_; }
  comm::RowExchangeWriter *row_exchange_writer_{nullptr};
  Temp_table_param *tmp_table_param_{nullptr};

 private:
  // pass to PartialExecutor, like LocalWorker
  // &thd_ = m_thd
  THD thd_;
  uint128 group_id_;
  // group we belong to where we can get serialize data
  BrpcStreamPartialExecutorGroup *owner_{nullptr};
  bool has_send_stat_{false};

  // Communication facilities with RemoteBrpcWorker
  // m_sender_channel in PartialExecutor point to below
  comm::RowChannel *sender_channel_{nullptr};
  comm::RowExchange sender_exchange_;
  PartialExecutor executor_;
};

// responsible for all BrpcStreamPartialExecutor relate to the same group_id
// use deserialize to init thd,PartialPlan...
// all BrpcStreamPartialExecutor clone from here
class BrpcStreamPartialExecutorGroup {
 public:
  // PARTIAL_PLAN: used by parallel
  // SQL_PLAN: used by broadcast
  enum Type { PARTIAL_PLAN, SQL_PLAN };

  BrpcStreamPartialExecutorGroup(tdsql::StartExecutorRequest *req);

  ~BrpcStreamPartialExecutorGroup();

  bool Init();

  TABLE *table() { return table_; }

  MDL_context *mdl_group() { return &leader_thd_.mdl_context; }

  THD *thd() { return &leader_thd_; }
  PartialPlan *plan() { return &partial_plan_; }

  void DecExecutor();
  void AddExecutor();

  bool NeedClean(struct timespec &tp_now);

  bool IsExplainAnalyze() { return is_explain_analyze_; }

  uint128 group_id_;

  Type type() { return type_; }
  push_down::SerializeData *Data() { return &data_; }

 private:

  // number of remote_executor get from first rpc
  // normally we will receive another total_num_ rpc to start remote_executor
  std::atomic<int> total_num_;
  // current active remote_executor,need to wait to zero before manually clean
  std::atomic<int> active_num_;

  // to calu how long this Group exists
  // manually clean current group if exists too long
  timespec tp_create_;

  push_down::SerializeData data_;

  // the table used to scan data
  TABLE *table_{nullptr};

  THD leader_thd_;

  // The query plan template for BrpcStreamPartialExecutor
  PartialPlan partial_plan_;
  bool is_explain_analyze_{false};

  Type type_{PARTIAL_PLAN};
};

// manager all BrpcStreamPartialExecutorGroup in our node
class BrpcStreamPartialExecutorManager {
 public:
  bool AddGroup(tdsql::StartExecutorRequest *req, uint128 group_id);
  void EraseGroup(uint128 group_id);

  BrpcStreamPartialExecutorGroup *AddToGroup(uint128 group_id);

  bool Exist(uint128 group_id);

  void CleanGroup(struct timespec &tp_now);

  BrpcStreamPartialExecutorManager();

  ~BrpcStreamPartialExecutorManager();

 private:
  mysql_mutex_t map_mtx_;

  std::unordered_map<uint128, std::unique_ptr<BrpcStreamPartialExecutorGroup>,
                     uint128_hash>
      map_;
};

bool StartExecutor(comm::BrpcStreamConnectionPtr stream_conn, uint128 group_id);
bool CreateBrpcStreamPartialExecutorGroup(tdsql::StartExecutorRequest *req,
                                          uint128 group_id);

bool ExistExecutorGroup(uint128 group_id);

void CleanExecutorGroup();
}  // namespace pq
#endif
