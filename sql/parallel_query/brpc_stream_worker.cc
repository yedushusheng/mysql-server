#include "sql/parallel_query/brpc_stream_worker.h"
#include "sql/parallel_query/executor.h"
#include "sql/parallel_query/row_channel.h"

#include "sql/parallel_query/row_channel_brpc.h"
namespace pq {

// collector has got all data it needed from m_receiver_channel
// set stat
void OnChanelDataDone(BrpcStreamWorker *worker) {
  LogDebug("OnChanelDataDone, set stat for worker:%p", worker);
  worker->SetStat();
}

BrpcStreamWorker::BrpcStreamWorker(uint id, comm::Event *state_event, THD *thd,
                                   PartialPlan *plan, BrpcGroupDesc* group_desc)
    : Worker(id, state_event),
      plan_(plan),
      group_desc_(group_desc),
      m_leader_thd(thd) {
#ifndef NDEBUG
  if (!group_desc_->init_) {
    uint64 query_connection_id =
        ((uint64)GetConnId() << 32) | (m_leader_thd->query_id & 0xffffffff);
    CheckUnique::getInstance().Check(query_connection_id, reinterpret_cast<ulong>(plan_));
    first_worker_ = true;
  }
#endif
}

BrpcStreamWorker::~BrpcStreamWorker() {
#ifndef NDEBUG
  if (first_worker_) {
    uint64 query_connection_id =
        ((uint64)GetConnId() << 32) | (m_leader_thd->query_id & 0xffffffff);
    CheckUnique::getInstance().Erase(query_connection_id, reinterpret_cast<ulong>(plan_));
  }
#endif
}

// m_receiver_channel relate to brpc stream connection
// so put INIT+START together
bool BrpcStreamWorker::Init(comm::Event *comm_event) {
  assert(comm_event);

  tdsql::StartExecutorRequest request;

  assert(group_desc_);
  request.mutable_group_id()->set_query_connection_id(group_desc_->query_connection_id_);
  request.mutable_group_id()->set_plan_id(group_desc_->plan_id_);

  DBUG_EXECUTE_IF("pq_simulate_remote_worker_init_fail", {
    if (group_desc_->init_) {
      LogError("init BrpcStreamWorker fail:%p", this);
      my_error(ER_PARALLEL_REMOTE_WORK_ERROR, MYF(0),
               "init BrpcStreamWorker fail");
      return true;
    }
  });

  // send serialize data for the first brpc stream executor
  if (!group_desc_->init_) {
    group_desc_->init_ = true;
    push_down::SerializeData *data = request.mutable_serialize_data();

    // all nodes use the same serialize data,so need copy
    data->CopyFrom(*(group_desc_->data_));

    // each group_desc use own scan_info,so can Swap
    group_desc_->scan_info_.Swap(data->mutable_scan_info());

    request.set_num(group_desc_->worker_num_);
  }

  LogDebug("worke_id %u,worker:%p,begin create ChannelRecv for %lu,%lu", m_id,
           this, request.group_id().query_connection_id(),
           request.group_id().plan_id());

  if (comm::CreateBrpcStreamRowChannelRecv(m_leader_thd->mem_root,
                                           &m_receiver_channel, &request,
                                           group_desc_->tdstore_addr_)) {
    LogError("init BrpcStreamWorker fail:%p", this);
    my_error(ER_PARALLEL_REMOTE_WORK_ERROR, MYF(0), "create brpc recv channel fail");
    return true;
  }

  m_receiver_channel->Init(m_leader_thd, comm_event, true);

  LogDebug(
      "init BrpcStreamWorker "
      "success,worker:%p,worker_id:%u,m_receiver_channel:%p",
      this, m_id, m_receiver_channel);

  pq::comm::BrpcStreamRowChannel *receive_channel =
      (pq::comm::BrpcStreamRowChannel *)m_receiver_channel;
  google::protobuf::Closure *data_done_func =
      brpc::NewCallback(OnChanelDataDone, this);
  receive_channel->SetDataDoneFunc(data_done_func);

  return false;
}

// we already start brpc stream executor in Init
bool BrpcStreamWorker::Start() {
  group_desc_->start_worker_num_++;
  assert(group_desc_->start_worker_num_ <= group_desc_->worker_num_);
  LogInfo(
      "start worker:%p,node:%s,group :%lu,%lu,start worker:%u,total "
      "worker:%u",
      this, group_desc_->tdstore_addr_.c_str(),
      group_desc_->query_connection_id_, group_desc_->plan_id_,
      group_desc_->start_worker_num_, group_desc_->worker_num_);
  return false;
}

bool BrpcStreamWorker::SetStat() {
  // should only called once
  if (has_set_stat_) {
    LogError("call SetStat more than once for worker:%p,node:%s", this,
             group_desc_->tdstore_addr_.c_str());
    assert(0);
    return true;
  }
  has_set_stat_ = true;

  group_desc_->done_worker_num_++;
  LogInfo(
      "set stat for worker:%p,node:%s,group :%lu,%lu,done worker:%u,total "
      "worker:%u",
      this, group_desc_->tdstore_addr_.c_str(),
      group_desc_->query_connection_id_, group_desc_->plan_id_,
      group_desc_->done_worker_num_, group_desc_->worker_num_);

  pq::comm::BrpcStreamRowChannel *receive_channel =
      (pq::comm::BrpcStreamRowChannel *)m_receiver_channel;

  std::string stat_str;

  bool ret = receive_channel->GetStat(stat_str);

  // abnormally read data
  if (ret) {
    LogError("read data from remote node:%s error,set stat",
             group_desc_->tdstore_addr_.c_str());
    main_da_.set_error_status(ER_PARALLEL_REMOTE_WORK_ERROR,
                              "read data from remote node error", "HY000");
    return false;
  }


  tdsql::ExecutorStat stat;
  if (!stat.ParseFromString(stat_str)) {
    main_da_.set_error_status(ER_PARALLEL_REMOTE_WORK_ERROR, "parse stat fail",
                              "HY000");
    LogError("stat.ParseFromString fail");
    return true;
  }

  LogDebug("receive found:%lu,examined:%lu", stat.found_rows(),
           stat.examined_rows());
  found_rows_ = stat.found_rows();
  examined_rows_ = stat.examined_rows();

  if (stat.mysql_errno()) {
    main_da_.set_error_status(stat.mysql_errno(), stat.error_msg().c_str(),
                              stat.sqlstate().c_str());
    LogDebug("receive thd is error:%u,msg:%s", stat.mysql_errno(),
             stat.error_msg().c_str());
  }

  timing_data_ = stat.timing_data();

  return false;
}

// return after we close the connection
// or on_received_messages/on_closed may access dangling point
void BrpcStreamWorker::Terminate() {
  if (m_receiver_channel) {
    pq::comm::BrpcStreamRowChannel *receive_channel =
        (pq::comm::BrpcStreamRowChannel *)m_receiver_channel;

    // for analyze limit sql,CollectorIterator may not read all rows
    // but analyze stat info is at stream end,
    // so here we read all rows manually to get the stat info
    if (m_leader_thd->lex->is_explain_analyze) {
      if (!has_set_stat_) receive_channel->ReceiveStat();
    }

    receive_channel->Close();
  }
}

// IsRunning only called from Collector::TerminateWorkers
bool BrpcStreamWorker::IsRunning() {
  if (!m_receiver_channel) return false;

  pq::comm::BrpcStreamRowChannel *receive_channel =
      (pq::comm::BrpcStreamRowChannel *)m_receiver_channel;

  // Terminate must have called before us
  assert(receive_channel->IsClosed());

  return !receive_channel->IsClosed();
}

// TODO
void BrpcStreamWorker::CollectStatusVars(THD *) { return; }

Diagnostics_area *BrpcStreamWorker::stmt_da(bool finished_collect,
                                            ha_rows *found_rows,
                                            ha_rows *examined_rows) {
  if (!finished_collect) return &main_da_;

  *found_rows = found_rows_;
  *examined_rows = examined_rows_;

  return &main_da_;
}

std::string BrpcStreamWorker::QueryPlanTimingData() {
  return std::move(timing_data_);
}

BrpcStreamWorker *CreateBrpcStreamWorker(uint id, comm::Event *state_event,
                                         PartialPlan *plan, BrpcGroupDesc *group_desc) {
  THD *thd = plan->thd();

  return new (thd->mem_root) BrpcStreamWorker(id, state_event, thd, plan, group_desc);
}

}  // namespace pq
