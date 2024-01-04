#include "sql/parallel_query/brpc_stream_executor.h"
#include "sql/join_optimizer/explain_access_path.h"
#include "sql/mysqld.h"
#include "sql/opt_explain_traditional.h"
#include "sql/parallel_query/brpc_stream_connection.h"
#include "sql/parallel_query/planner.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/row_channel_brpc.h"
#include "sql/parallel_query/serialize_wrapper.h"
#include "sql/parallel_query/worker.h"
#include "sql/query_result.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_tmp_table.h"
#include "sql/table.h"
#include "sql/transaction.h"
#include "storage/rocksdb/ha_rocksdb.h"
#include "sql/tdsql/buckets.h"
#include "sql/sql_parse.h"

extern bool thread_attach(THD *thd);

// leader tell group id1 need to start N executor  but exit after send M rpc
// (M<N) we need to detect such group and clean it to free resource like TABLE
int32_t tdsql_clean_brpc_group_sec;
#define PartialExecutorManagerNum 64

namespace pq {

Buckets<BrpcStreamPartialExecutorManager, PartialExecutorManagerNum>
    brpc_partial_executor_buckets;

BrpcStreamPartialExecutorManager *GetParialExecutorManager(uint128 group_id) {
  // use plan_id
  return brpc_partial_executor_buckets.GetBucket((group_id & 0xffffffff) %
                                                 PartialExecutorManagerNum);
}

void OnConClose(BrpcStreamPartialExecutor *executor) {
  LogDebug("OnConClose in executor:%p,set kill flag", executor);
  executor->SetKillFlag();
}

BrpcStreamPartialExecutor::BrpcStreamPartialExecutor(
    BrpcStreamPartialExecutorGroup *group, THD *leader_thd, PartialPlan *plan)
    : thd_(true, leader_thd),
      group_id_(group->group_id_),
      owner_(group),
      executor_(&thd_, plan, [this]() { SendStat(); }) {
  LogDebug("new BrpcStreamPartialExecutor:%p,belong to group_id:%lu,%lu", this,
           static_cast<std::uint64_t>(group_id_ >> 64),
           static_cast<std::uint64_t>(group_id_));
}

BrpcStreamPartialExecutor::~BrpcStreamPartialExecutor() {
  if (sender_channel_) destroy(sender_channel_);
  LogDebug(
      "BrpcStreamPartialExecutor:%p,belong to group_id:%lu,%lu done,dec "
      "group "
      "active number",
      this, static_cast<std::uint64_t>(group_id_ >> 64),
      static_cast<std::uint64_t>(group_id_));
  owner_->DecExecutor();
  // DecExecutor may change current_thd
  thread_attach(&thd_);
}

TABLE *BrpcStreamPartialExecutor::GroupTable() { return owner_->table(); }

push_down::SerializeData *BrpcStreamPartialExecutor::Data() {
  return owner_->Data();
}

bool BrpcStreamPartialExecutor::IsExplainAnalyze() {
  return owner_->IsExplainAnalyze();
}

void BrpcStreamPartialExecutor::SetKillFlag() {
  THD *thd = get_thd();
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_QUERY);
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

// for remote executor,need send extra info to worker
// 1)found_rows,examined_row
// 2)stmt_da if thd is error
// 3)timing_data if explain analyze
void BrpcStreamPartialExecutor::SendStat() {
  DBUG_EXECUTE_IF("pq_simulate_remote_executor_donot_send_stat", {
    LogError("pq_simulate_remote_executor_donot_send_stat");
    my_error(ER_PARALLEL_REMOTE_EXECUTOR_ERROR, MYF(0),
             "simulate remote_executor_donot_send_stat");
    has_send_stat_ = true;
  });

  if (has_send_stat_) return;
  has_send_stat_ = true;

  THD *thd = &thd_;
  // can not rely on release_resources,which is called after SendStat
  thd->UpdateExecTime();

  pq::comm::BrpcStreamRowChannel *sender_channel =
      (pq::comm::BrpcStreamRowChannel *)sender_channel_;

  if (sender_channel->IsClosed()) {
    LogError("sender_channel is closed already,can not send stat");
    return;
  }

  thd->update_previous_found_rows();
  tdsql::ExecutorStat stat;
  stat.set_found_rows(thd->previous_found_rows);
  stat.set_examined_rows(thd->get_examined_row_count());
  LogDebug("send found:%lu,examined:%lu", stat.found_rows(),
           stat.examined_rows());

  stat.mutable_cost_time()->set_query_utime(thd->exec_time_.query_utime);
  stat.mutable_cost_time()->set_lock_utime(thd->exec_time_.lock_utime);
  stat.mutable_cost_time()->set_mc_h2_tc(thd->exec_time_.mc_h2_tc);
  stat.mutable_cost_time()->set_mc_raw_tc(thd->exec_time_.mc_raw_tc);
  stat.mutable_cost_time()->set_tdstore_rpc_tc(thd->exec_time_.tdstore_rpc_tc);
  stat.mutable_cost_time()->set_tdstore_wait_lock_tc(
      thd->exec_time_.tdstore_wait_lock_tc);
  stat.mutable_cost_time()->set_rpc_cntl_retry_delay_tc(
      thd->exec_time_.rpc_cntl_retry_delay_tc);

  if (thd->is_error()) {
    stat.set_error_msg(thd->get_stmt_da()->message_text());
    stat.set_mysql_errno(thd->get_stmt_da()->mysql_errno());
    stat.set_sqlstate(thd->get_stmt_da()->returned_sqlstate());
    LogInfo("send thd is error:%u,msg:%s", stat.mysql_errno(),
            stat.error_msg().c_str());
  }

  // m_query_plan_timing_data only init when is_explain_analyze is true
  if (thd->lex->is_explain_analyze)
    stat.set_timing_data(executor_.GetTimingData());

  std::string message_str;
  stat.SerializeToString(&message_str);

  // BrpcStreamRowChannel::Flush rely this
  thd->killed = THD::NOT_KILLED;
  if (sender_channel->SendStat(message_str) != pq::comm::Result::SUCCESS)
    LogError("sender_channel->SendStat fail");
}

bool BrpcStreamPartialExecutor::InitChannel(
    comm::BrpcStreamConnectionPtr stream_conn) {
  THD *thd = get_thd();
  thd->set_new_thread_id();
  thread_attach(thd);

  LEX *lex = thd->lex;
  bool is_explain_analyze = IsExplainAnalyze();
  if (is_explain_analyze)
    lex->explain_format = new (thd->mem_root) Explain_format_tree;

  sender_channel_ =
      comm::CreateBrpcStreamRowChannel(thd->mem_root, std::move(stream_conn));

  pq::comm::BrpcStreamRowChannel *sender_channel =
      (pq::comm::BrpcStreamRowChannel *)sender_channel_;

  google::protobuf::Closure *close_func = brpc::NewCallback(OnConClose, this);
  sender_channel->Conn()->SetCloseFunc(close_func);

  sender_channel_->Init(thd, sender_exchange_.event(), false);

  if (sender_channel->Conn()->HasRunCloseFunc()) {
    LogDebug("connection is closed by remote,no need to execute the plan");
    SetKillFlag();
    return true;
  } else
    LogDebug("BrpcStreamPartialExecutor :%p,init with stream_id:%lu", this,
             sender_channel->Conn()->stream_id());

  thd->status_var.pq_workers_created = 1;

  if (sender_exchange_.Init(thd->mem_root, 1,
                            [this](uint) { return sender_channel_; })) {
    LogError("sender_exchange.Init fail :%p", this);
    my_error(ER_PARALLEL_REMOTE_EXECUTOR_ERROR, MYF(0),
             "sender_exchange init fail");
    return true;
  }

  DBUG_EXECUTE_IF("pq_simulate_remote_executor_exchange_fail", {
    LogError("pq_simulate_remote_executor_exchange_fail");
    my_error(ER_PARALLEL_REMOTE_EXECUTOR_ERROR, MYF(0),
             "simulate sender_exchange init fail");
    return true;
  });

  executor_.Init(sender_channel_, &sender_exchange_);

  return false;
}

BrpcStreamPartialExecutorGroup::BrpcStreamPartialExecutorGroup(
    tdsql::StartExecutorRequest *req)
    : leader_thd_(0) {
  group_id_ = (static_cast<uint128>(req->group_id().query_connection_id()) << 64) |
              req->group_id().plan_id();

  total_num_ = req->num();
  active_num_ = 0;
  data_.Swap(req->mutable_serialize_data());
  clock_gettime(CLOCK_MONOTONIC, &tp_create_);

  LogDebug("new BrpcStreamPartialExecutorGroup :%p for group_id:%lu,%lu", this,
          static_cast<std::uint64_t>(group_id_ >> 64),
          static_cast<std::uint64_t>(group_id_));
}

// return true if group exist too long
// do not care total_num_
// active_num_ should =0
// protected by mutex
bool BrpcStreamPartialExecutorGroup::NeedClean(struct timespec &tp_now) {
  if (active_num_) return false;

  if (tp_now.tv_sec - tp_create_.tv_sec > tdsql_clean_brpc_group_sec) {
    LogError("group:%lu,%lu,exist too long:%ld sec,num:%d,create time:%lu",
             static_cast<std::uint64_t>(group_id_ >> 64),
             static_cast<std::uint64_t>(group_id_),
             tp_now.tv_sec - tp_create_.tv_sec, total_num_.load(),
             tp_create_.tv_sec);
    return true;
  }
  return false;
}

void BrpcStreamPartialExecutorGroup::AddExecutor() { active_num_++; }

void BrpcStreamPartialExecutorGroup::DecExecutor() {
  active_num_--;
  if (--total_num_ == 0) {
    assert(active_num_ == 0);
    LogDebug("all executor relate to group id:%lu,%lu is done",
             static_cast<std::uint64_t>(group_id_ >> 64),
             static_cast<std::uint64_t>(group_id_));
    GetParialExecutorManager(group_id_)->EraseGroup(group_id_);
  } else {
    LogDebug("there still have :%d,%d executor relate to group id:%lu,%lu",
             total_num_.load(), active_num_.load(),
             static_cast<std::uint64_t>(group_id_ >> 64),
             static_cast<std::uint64_t>(group_id_));
  }
}

BrpcStreamPartialExecutorGroup::~BrpcStreamPartialExecutorGroup() {
  thread_attach(&leader_thd_);

  // join_free need this to cleanup_table
  leader_thd_.lex->is_brpc_group = true;
  auto *qb = leader_thd_.lex->query_block;
  if (qb) {
    JOIN *join = qb->join;
    if (join) join->join_free();
  }

  trans_rollback_stmt(&leader_thd_);
  close_thread_tables(&leader_thd_);
  leader_thd_.cleanup_after_query();
}

// set range need to scan,BrpcStreamPartialExecutor will get job from here
bool BrpcStreamPartialExecutorGroup::Init() {
  THD *thd = &leader_thd_;

  thd->set_new_thread_id();
  thread_attach(thd);

  // Broadcast sql
  if (data_.query_expression().empty()) {
    if (DeserializeBroadcast(thd, &data_)) {
      LogError("DeserializeBroadcast in  BrpcStreamPartialExecutorGroup fail");
      return true;
    }
    type_ = SQL_PLAN;
    return false;
  }

  if (DeserializePlan(thd, &partial_plan_, &data_)) {
    LogError("DeserializePlan in  BrpcStreamPartialExecutorGroup fail");
    return true;
  }

  type_ = PARTIAL_PLAN;

  LEX *lex = thd->lex;
  is_explain_analyze_ = thd->lex->is_explain_analyze;
  uint table_no = data_.scan_info().tableno();
  table_ = GetTableListUseNo(lex->query_block,table_no)->table;

  LogDebug("Group use table_no:%u,%s to scan data",table_no,table_->alias);

  partial_plan_.SetQueryBlock(lex->query_block);

  // PartialAccessPathRewriter::rewrite_materialize need this
  lex->query_block->join->partial_plan = &partial_plan_;

  // init all jobs belong to current node
  // use range info
  push_down::ScanInfo *jobs = data_.mutable_scan_info();
  parallel_scan_desc_t scan_desc;
  scan_desc.job_desc = jobs;
  scan_desc.keynr = jobs->keynr();
  scan_desc.key_used = jobs->key_used();
  scan_desc.flags = (parallel_scan_flags)jobs->flags();
  scan_desc.degree = jobs->degree();

  ulong nranges = 0;
  if (table_->file->init_parallel_scan(&table_->parallel_scan_handle, &nranges,
                                       &scan_desc)) {
    LogError("init_parallel_scan fail, group id:%lu,%lu",
             static_cast<std::uint64_t>(group_id_ >> 64),
             static_cast<std::uint64_t>(group_id_));
    return true;
  }

  partial_plan_.SetTableParallelScan(
      table_, data_.scan_info().scan_job().size(), scan_desc);

  // init mdl group like Collector::PrepareExecution
  // after DeserializePlan which call open_table get mdl lock
  thd->mdl_context.init_lock_group();

  thread_attach(nullptr);
  return false;
}

BrpcStreamPartialExecutorManager::BrpcStreamPartialExecutorManager() {
  mysql_mutex_init(PSI_NOT_INSTRUMENTED, &map_mtx_, nullptr);
}

BrpcStreamPartialExecutorManager::~BrpcStreamPartialExecutorManager() {
  mysql_mutex_destroy(&map_mtx_);
}

// return nullptr if no group exist relate to group_id
BrpcStreamPartialExecutorGroup *BrpcStreamPartialExecutorManager::AddToGroup(
    uint128 group_id) {
  BrpcStreamPartialExecutorGroup *group = nullptr;

  mysql_mutex_lock(&map_mtx_);
  auto iter = map_.find(group_id);
  if (iter != map_.end()) {
    group = (iter->second).get();
    group->AddExecutor();
  }
  mysql_mutex_unlock(&map_mtx_);
  return group;
}

void BrpcStreamPartialExecutorManager::CleanGroup(struct timespec &tp_now) {
  mysql_mutex_lock(&map_mtx_);
  for (auto it = map_.begin(); it != map_.end();) {
    if (it->second->NeedClean(tp_now)) {
      it = map_.erase(it);
    } else {
      ++it;
    }
  }
  mysql_mutex_unlock(&map_mtx_);
}

bool BrpcStreamPartialExecutorManager::Exist(uint128 group_id) {
  bool exist = false;

  mysql_mutex_lock(&map_mtx_);
  if (map_.find(group_id) != map_.end()) {
    exist = true;
  }
  mysql_mutex_unlock(&map_mtx_);

  return exist;
}

// return true if fail
bool BrpcStreamPartialExecutorManager::AddGroup(
    tdsql::StartExecutorRequest *req, uint128 group_id) {

  std::unique_ptr<BrpcStreamPartialExecutorGroup> group(
      new BrpcStreamPartialExecutorGroup(req));

  if (group->Init()) return true;

  mysql_mutex_lock(&map_mtx_);
  auto result = map_.insert(std::make_pair(group_id, std::move(group)));
  mysql_mutex_unlock(&map_mtx_);

  if (!result.second) {
    assert(0);
    LogError(
        "BrpcStreamPartialExecutorGroup with group_id:%lu,%lu already exist",
        static_cast<std::uint64_t>(group_id >> 64),
        static_cast<std::uint64_t>(group_id));
    return true;
  } else {
    LogDebug("AddGroup with group_id:%lu,%lu",
             static_cast<std::uint64_t>(group_id >> 64),
             static_cast<std::uint64_t>(group_id));
  }

  return false;
}

void BrpcStreamPartialExecutorManager::EraseGroup(uint128 group_id) {
  mysql_mutex_lock(&map_mtx_);
  auto result = map_.erase(group_id);
  if (!result) {
    assert(0);
    LogError(
        "BrpcStreamPartialExecutorGroup with group_id:%lu,%lu do not exist",
        static_cast<std::uint64_t>(group_id >> 64),
        static_cast<std::uint64_t>(group_id));
  }
  mysql_mutex_unlock(&map_mtx_);

  return;
}
bool CreateBrpcStreamPartialExecutorGroup(tdsql::StartExecutorRequest *req,
                                          uint128 group_id) {
  if (GetParialExecutorManager(group_id)->AddGroup(req, group_id)) return true;
  return false;
}

bool ExistExecutorGroup(uint128 group_id) {
  return GetParialExecutorManager(group_id)->Exist(group_id);
}

// clean groups exist too long due to abnormal situation
void CleanExecutorGroup() {
  static struct timespec tp_last { 0, 0 };
  struct timespec tp_now;
  clock_gettime(CLOCK_MONOTONIC, &tp_now);
  if (tp_now.tv_sec - tp_last.tv_sec > 10) {
    for (size_t i = 0; i < brpc_partial_executor_buckets.size(); ++i) {
      brpc_partial_executor_buckets.GetBucket(i)->CleanGroup(tp_now);
    }
    tp_last = tp_now;
  }
}

bool StartExecutorUseSql(THD *leader_thd, BrpcStreamPartialExecutor *executor) {
  THD *thd = executor->get_thd();
  thread_attach(thd);
  thd->set_query(leader_thd->query());
  thd->set_db(leader_thd->db());
  thd->set_type(KThdForwardStreamSql);
  thd->security_context()->skip_grants();
  thd->security_context()->set_user_ptr(STRING_WITH_LEN("(null)"));
  thd->security_context()->set_host_or_ip_ptr(my_localhost,
                                              strlen(my_localhost));
  comm::RowExchangeWriter row_exchange_writer;
  row_exchange_writer.SetExchange(executor->SenderExchange());

  Temp_table_param tmp_table_param;
  executor->row_exchange_writer_ = &row_exchange_writer;
  executor->tmp_table_param_ = &tmp_table_param;

  // used in ChangeForwardQueryResult
  thd->forward_executor_ = executor;

  Parser_state parser_state;
  LogDebug("StartExecutorUseSql begin sql:%s", thd->query().str);
  if (parser_state.init(thd, thd->query().str, thd->query().length)) {
    LogError("parser_state.init fail");
    my_error(ER_TDSQL_COMMON, MYF(0), "intra_connection",
             ErrorNameAndCodeInLog(tdcomm::EC_SQL_COMMON),
             "parser_state.init fail");
  } else {
    dispatch_sql_command(thd, &parser_state, false);
  }

  executor->SendStat();
  LogDebug("StartExecutorUseSql end sql:%s",thd->query().str);

  return false;
}

bool StartExecutorInternal(THD *leader_thd,
                           BrpcStreamPartialExecutor *executor) {
  THD *lthd = leader_thd;
  PartialExecutorContext context{lthd->tx_isolation,
                                 &lthd->start_time,
                                 lthd->db(),
                                 NULL_CSTR,
                                 lthd->query_id,
                                 lthd->security_context(),
                                 lthd->first_successful_insert_id_in_prev_stmt,
                                 lthd->save_raw_record,
                                 lthd->lex->sql_command,
                                 lthd->lex->is_explain_analyze,
                                 [lthd](const std::string &key) {
                                   mysql_mutex_lock(&lthd->LOCK_thd_data);
                                   user_var_entry *entry =
                                       find_or_nullptr(lthd->user_vars, key);
                                   mysql_mutex_unlock(&lthd->LOCK_thd_data);
                                   return entry;
                                 }};
  mysql_mutex_lock(&lthd->LOCK_thd_query);
  context.query = lthd->query();
  mysql_mutex_unlock(&lthd->LOCK_thd_query);

  executor->Executor()->InitExecThd(&context, lthd);

  // Clone snapshot always return false, currently
  ha_clone_consistent_snapshot(executor->get_thd(), lthd);

  executor->Executor()->ExecuteQuery(&context);

  return false;
}

bool StartExecutor(comm::BrpcStreamConnectionPtr stream_conn,
                   uint128 group_id) {
  auto group = GetParialExecutorManager(group_id)->AddToGroup(group_id);
  if (!group) {
    assert(0);
    LogError("no BrpcStreamPartialExecutorGroup with group_id:%lu,%lu exist",
             static_cast<std::uint64_t>(group_id >> 64),
             static_cast<std::uint64_t>(group_id));
    return true;
  }

  BrpcStreamPartialExecutor remote_executor(group, group->thd(), group->plan());

  int ret = remote_executor.InitChannel(std::move(stream_conn));
  if (ret) {
    LogError("remote_executor.InitChannel fail");
    remote_executor.SendStat();
  } else {
    if (group->type() == BrpcStreamPartialExecutorGroup::PARTIAL_PLAN)
      StartExecutorInternal(group->thd(), &remote_executor);
    else
      StartExecutorUseSql(group->thd(), &remote_executor);
  }

  LogDebug("done StartExecutor:%p for group_id:%lu,%lu", &remote_executor,
           static_cast<std::uint64_t>(group_id >> 64),
           static_cast<std::uint64_t>(group_id));
  return false;
}

}  // namespace pq
