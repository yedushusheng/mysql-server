#ifndef PARALLEL_QUERY_ROW_CHANNEL_BRPC_H
#define PARALLEL_QUERY_ROW_CHANNEL_BRPC_H

#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/brpc_stream_connection.h"
#include "sql/sql_class.h"
namespace pq {
namespace comm {

class BrpcStreamRowChannel : public RowChannel {
 public:
  BrpcStreamRowChannel(BrpcStreamConnectionPtr conn) : conn_(std::move(conn)) {}

  ~BrpcStreamRowChannel();

  using Result = RowTxResult;

  // +---------------------------------+
  // |       Message row format        |
  // +---------------------------------+
  // | frame_type | body_length | body |
  // +---------------------------------+
  // |<-------- header -------->|

  typedef uint32_t Length;

  enum FrameType : char { FRAME_DATA = 'D', FRAME_EOF = 'X', FRAME_STAT = 'S' };

  // thd status,last Frame of the stream
  // 1) FRAME_DATA,FRAME_EOF, FRAME_STAT --> thd is ok
  // 2) ... NO FRAME_EOF, FRAME_STAT --> thd is error

  static constexpr int HEADER_SIZE = sizeof(FrameType) + sizeof(Length);

  static constexpr int SEND_BUFFER_SIZE = 8192;

  bool Init(THD *thd, Event *event, bool) override;

  void SetDataDoneFunc(google::protobuf::Closure *func) {
    data_done_func_ = func;
  }

  void RunDataDoneFunc() {
    if (data_done_func_) {
      // Run will delete data_done_func_,should not call twice
      data_done_func_->Run();
      data_done_func_ = nullptr;
    }
  }

  BrpcStreamConnection *Conn() { return conn_.get(); }

  Result Send(std::size_t nbytes, const void *data, bool nowait) override;

  Result ReceiveStat();

  Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                 MessageBuffer *buf) override;

  Result SendEOF() override;

  Result SendStat(std::string& str);

  bool GetStat(std::string &str);

  // Call this function to send EOF after all rows are sent out.
  void Close() override;

  bool IsClosed() const override;

  BrpcStreamConnectionPtr ReleaseConnection() { return std::move(conn_); }

 private:
  static void OnWritable(brpc::StreamId stream_id, void *param, int);

  Result SendBytes(std::size_t nbytes, const void *data, bool nowait,
                   std::size_t *send_bytes);

  Result Flush(bool nowait);

  Result RecvBytes(bool nowait);

  THD *thd_ = nullptr;
  // Set by Init function, used to notify the caller of
  // BrpcStreamRowChannel when read/write events occur.
  Event *event_ = nullptr;

  BrpcStreamConnectionPtr conn_;

  // Cache the data that has beed received
  butil::IOBuf recv_buf_;
  size_t last_read_bytes_ = 0;

  // Cache the data that will be sent, used to split large packets.
  butil::IOBuf send_buf_;
  size_t send_partial_bytes_ = 0;

  // run when we got all data from the connection 
  // used by RemoteBrpcWorker to set Diagnostics_area
  //
  // 1) FRAME_DATA,FRAME_EOF, FRAME_STAT --> remote thd is ok
  // 2) ... NO FRAME_EOF, FRAME_STAT --> remote thd is error
  // 3) ... connection close --> manually set Diagnostics_area
  google::protobuf::Closure* data_done_func_{nullptr};

  // set to true if we receive FRAME_STAT
  bool has_frame_stat_{false};

};

using Result = BrpcStreamRowChannel::Result;
}  // namespace comm
}  // namespace pq

#endif
