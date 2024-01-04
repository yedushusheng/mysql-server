#ifndef PARALLEL_QUERY_BRPC_STREAM_WORKER_H
#define PARALLEL_QUERY_BRPC_STREAM_WORKER_H
#include "my_base.h"
#include "sql/sql_error.h"

#include "sql/parallel_query/brpc_stream_comm.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/serialize_wrapper.h"
#include "sql/parallel_query/worker.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/proto/sqlengine.pb.h"
#include "sql/tdsql/global_vars.h"

namespace pq {

// inside TDStoreNode, save all information to start BrpcStreamWorker
// a sql may choose multi nodes to execute the partial plan
class BrpcGroupDesc {
 public:
  // all jobs relate to current group
  push_down::ScanInfo scan_info_;
  bool has_remote_jobs_{false};
  // point to serialize data
  push_down::SerializeData *data_{nullptr};

  // query_connection_id + plan_id --> unique id in cluster
  // use this to bind BrpcStreamWorker and BrpcStreamPartialExecutor
  uint64 query_connection_id_;
  uint64 plan_id_;

  std::string tdstore_addr_;
  // how many worker need to start in current group
  uint worker_num_{0};

  bool init_{false};

  // how many worker actually start and done
  // used for debug
  uint start_worker_num_{0};
  uint done_worker_num_{0};
};

// channel_type == comm::RowChannel::Type::BRPC_STREAM
// start brpc stream executor in dest node
// get all data from brpc stream executor
class BrpcStreamWorker : public Worker {
 public:
  BrpcStreamWorker(uint id, comm::Event *state_event, THD *thd,
                   PartialPlan *plan, BrpcGroupDesc *group_desc);

  bool Init(comm::Event *comm_event) override;

  ~BrpcStreamWorker();

  Diagnostics_area *stmt_da(bool, ha_rows *, ha_rows *) override;

  bool Start() override;
  void Terminate() override;

  bool IsRunning() override;

  bool SetStat();

  std::string QueryPlanTimingData() override;
  void CollectStatusVars(THD *) override;

 private:
  PartialPlan *plan_{nullptr};
  // which group we belong to
  BrpcGroupDesc* group_desc_{nullptr};

  // set to true if is the first worker of group 
  // used for debug only
#ifndef NDEBUG
  bool first_worker_{false};
#endif

  // save info from remote executor
  bool has_set_stat_{false};
  Diagnostics_area main_da_{false};
  std::string timing_data_;
  ha_rows found_rows_{0};
  ha_rows examined_rows_{0};

  THD *m_leader_thd{nullptr};
};

BrpcStreamWorker *CreateBrpcStreamWorker(uint id, comm::Event *state_event,
                                         PartialPlan *plan,
                                         BrpcGroupDesc *group_desc);
}  // namespace pq
#endif
