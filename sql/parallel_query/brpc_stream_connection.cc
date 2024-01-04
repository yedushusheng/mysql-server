#include "sql/parallel_query/brpc_stream_connection.h"
#include "bthread/bthread.h"
#include "sql/tdsql/common.h"

namespace pq {
namespace comm {

const butil::IOBuf *BrpcStreamConnection::ON_CLOSE_EVENT = reinterpret_cast<butil::IOBuf*>(-1L);

int BrpcStreamConnectionReceiver::on_received_messages(brpc::StreamId,
                                                       butil::IOBuf *const messages[],
                                                       size_t size) {
  Event* event = conn_->event_.load(std::memory_order_relaxed);

  for (size_t i = 0; i < size; ++i) {
    butil::IOBuf *io_buf = new butil::IOBuf;
    io_buf->swap(*messages[i]);
    conn_->PushIOBuf(io_buf);
    LogDebug("on_received_messages:%p,i:%lu,size:%lu,stream_id:%lu", this, i,
             io_buf->size(), conn_->stream_id());
  }
  if (event) event->Set();

  return 0;
}

void BrpcStreamConnectionReceiver::on_closed(brpc::StreamId) {
  conn_->RunCloseFunc();
  Event* event = conn_->event_.load(std::memory_order_relaxed);

  conn_->PushIOBuf(const_cast<butil::IOBuf*>(BrpcStreamConnection::ON_CLOSE_EVENT));
  LogDebug("on_closed:%p,stream_id:%lu", this, conn_->stream_id());
  if (event) event->Set();
  conn_->close_event_.Set();

}

// should only called once for a connection
void BrpcStreamConnection::SetCloseFunc(google::protobuf::Closure *close_func) {
  assert(!close_func_);
  close_func_.store(close_func);
}

void BrpcStreamConnection::RunCloseFunc() {
  if (on_closed_) {
    LogError("RunCloseFunc run more than once for con:%p,%lu", this,
             stream_id());
    assert(0);
    return;
  }

  on_closed_ = true;
  if (close_func_.load()) {
    close_func_.load()->Run();
    close_func_ = nullptr;
  }
}

BrpcStreamConnection::~BrpcStreamConnection() {
  Close();

  if (close_func_) {
    delete close_func_;
  }
}

void BrpcStreamConnection::PushIOBuf(butil::IOBuf* io_buf) {
  while (!recv_queue_.push(io_buf)) {
    Event* event = event_.load(std::memory_order_relaxed);
    if (event) event->Set();
    recv_queue_event_.Wait(nullptr);
  }
}

butil::IOBuf* BrpcStreamConnection::PopIOBuf() {
  butil::IOBuf* io_buf = nullptr;
  if (!recv_queue_.pop(io_buf)) {
    recv_queue_event_.Set();
    return nullptr;
  }

  if (io_buf == ON_CLOSE_EVENT) { // Last io_buf
    close_event_.Wait(nullptr);
    assert(!peer_closed_);
    peer_closed_ = true;
  } else {
    recv_queue_event_.Set();
  }

  return io_buf;
}

void BrpcStreamConnection::SetEvent(Event* event) {
  event_.store(event, std::memory_order_relaxed);
}

void BrpcStreamConnection::Close() {
  if (IsClosed()) return;

  brpc::StreamClose(stream_id_);

  // We must wait until the call to BrpcStreamConnection::on_closed
  // completes before we can ensure that stream_adapter_ will not invoke
  // BrpcStreamConnection::on_received_messages again to access
  // dangling pointers.
  WaitClosed(); 

  LogDebug("end streamid close:%lu",stream_id_);
  stream_id_ = brpc::INVALID_STREAM_ID;
}

void BrpcStreamConnection::WaitClosed() {
  while (!peer_closed_) {
    butil::IOBuf* io_buf = PopIOBuf();

    if (io_buf == ON_CLOSE_EVENT) break;

    if (unlikely(!io_buf))
      bthread_yield();
    else
      delete io_buf;
  }
  assert(peer_closed_);
}

bool BrpcStreamConnection::IsClosed() const {
  return stream_id_ == brpc::INVALID_STREAM_ID;
}

int BrpcStreamConnection::Recv(butil::IOBuf* out) {
  butil::IOBuf* io_buf = PopIOBuf();
  if (!io_buf) {
    return EAGAIN;
  }

  if (io_buf == ON_CLOSE_EVENT) {
    return -1;
  }

  out->append(*io_buf);
  delete io_buf;

  return 0;
}

int BrpcStreamConnection::Send(const butil::IOBuf& in) {
  return brpc::StreamWrite(stream_id_, in);
}

void BrpcStreamConnection::WaitWritable(const timespec *due_time,
                                        void (*)(brpc::StreamId, void*, int), void *) {
  brpc::StreamWait(stream_id_, due_time);
}

BrpcStreamConnectionPtr CreateBrpcStreamConnection(
      const BrpcStreamConnectionOptions& options, brpc::Controller* cntl, bool as_accept) {
  assert(cntl);
  BrpcStreamConnectionPtr stream_conn(new BrpcStreamConnection(options.recv_queue_size));

  brpc::StreamOptions stream_options;
  stream_options.handler = &stream_conn->receiver_;
  if (as_accept) {
    if (brpc::StreamAccept(&stream_conn->stream_id_, *cntl, &stream_options)) {
      return nullptr;
    }
  } else {
    if (brpc::StreamCreate(&stream_conn->stream_id_, *cntl, &stream_options)) {
      return nullptr;
    }
  }

  LogDebug("new stream_connection, accept:%d,stream_id:%lu", as_accept,
           stream_conn->stream_id_);
  return stream_conn;
}

} // namespace pq
} // namespace comm
