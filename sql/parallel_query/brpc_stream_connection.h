#ifndef TDSQL_BRPC_STREAM_CONNECTION_H
#define TDSQL_BRPC_STREAM_CONNECTION_H

#include "brpc/stream.h"
#include "brpc/controller.h"
#include "brpc/socket.h"
#include "boost/lockfree/spsc_queue.hpp"
#include "sql/parallel_query/comm_types.h"
#include "sql/parallel_query/row_channel.h"

namespace pq {
namespace comm {

class BrpcStreamConnection;
typedef std::unique_ptr<BrpcStreamConnection> BrpcStreamConnectionPtr;

RowChannel *CreateBrpcStreamRowChannel(MEM_ROOT *mem_root,
                                       BrpcStreamConnectionPtr stream_conn);
class BrpcStreamConnectionReceiver : public brpc::StreamInputHandler {
friend class BrpcStreamConnection;
 public:
  BrpcStreamConnectionReceiver(BrpcStreamConnection* conn) : conn_(conn) {};

  int on_received_messages(brpc::StreamId, 
                           butil::IOBuf *const messages[], 
                           size_t size) override;

  void on_idle_timeout(brpc::StreamId) override {
    // Do nothing
  }

  void on_closed(brpc::StreamId) override;

 private:
  BrpcStreamConnection* conn_;
};

struct BrpcStreamConnectionOptions {
  size_t recv_queue_size = 128;
};

BrpcStreamConnectionPtr CreateBrpcStreamConnection(
      const BrpcStreamConnectionOptions& options,
      brpc::Controller* cntl, bool as_accept);

class BrpcStreamConnection {
  friend class BrpcStreamConnectionReceiver;
  friend BrpcStreamConnectionPtr CreateBrpcStreamConnection(
             const BrpcStreamConnectionOptions& options,
             brpc::Controller* cntl, bool as_accept);

 public:
  BrpcStreamConnection(size_t recv_queue_size)
    : receiver_(this), recv_queue_(recv_queue_size) {}
  
  ~BrpcStreamConnection();

  void SetEvent(Event *event);

  void SetCloseFunc(google::protobuf::Closure *close_func);
  void RunCloseFunc();
  bool HasRunCloseFunc() { return on_closed_; }

  void Close();

  bool IsClosed() const;

  // Try pop io_buf from recv_queue_ and append to @param out
  // 
  // @return Operation result
  //  @retval 0       success
  //  @retval -1      stream was closed       
  //  @retval EAGAIN  no data in recv_queue_ 
  int Recv(butil::IOBuf* out);

  int Send(const butil::IOBuf& in);

  void WaitWritable(const timespec *due_time,
                    void (*on_writable)(brpc::StreamId, void*, int), void *arg);

  brpc::StreamId stream_id() { return stream_id_; }

 private:
  void WaitClosed();

  void PushIOBuf(butil::IOBuf* io_buf); 

  butil::IOBuf* PopIOBuf();

  brpc::StreamId stream_id_ = brpc::INVALID_STREAM_ID;

  // If event_ is not nullptr, when read/close events
  // occur, event_->Set() will be called.
  std::atomic<Event*> event_{nullptr};

  BrpcStreamConnectionReceiver receiver_;

  // BrpcStreamConnectionReceiver::on_received_messages will push
  // IOBuf* to recv_queue_ when stream peer send data
  boost::lockfree::spsc_queue<butil::IOBuf*> recv_queue_;
  Event recv_queue_event_;

  // BrpcStreamConnectionReceiver::on_closed will push ON_CLOSE_EVENT to recv_queue_
  // when we or peer stream call StreamClose. Therefore, when we pop ON_CLOSE_EVENT
  // from recv_queue_, it means that either we or the peer have already closed the
  // stream, then on_received_messages and on_closed will not be called anymore
  const static butil::IOBuf *ON_CLOSE_EVENT;

  // peer_closed_ will be set to true when we pop ON_CLOSE_EVENT from recv_queue_
  bool peer_closed_ = false;

  // Used to wait until the execution of stream_adapter_.on_closed ends
  Event close_event_;

  // run when connection closed
  // used by RemotePartialExecutor to set kill flag
  std::atomic<google::protobuf::Closure*> close_func_{nullptr};

  // set to true if RunCloseFunc is called even close_func_=nullptr
  std::atomic<bool> on_closed_{false};
};
} // namespace comm
} // namespace pq

#endif // TDSQL_BRPC_STREAM_CONNECTION_H
