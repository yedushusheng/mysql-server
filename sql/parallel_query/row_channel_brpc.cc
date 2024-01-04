#include "sql/parallel_query/row_channel_brpc.h"
#include "sql/tdsql/rpc_cntl/td_client.h"
#include "butil/raw_pack.h"

extern std::string tdsql_rpc_addr;

namespace pq {
namespace comm {

const butil::IOBuf *BrpcStreamRowChannel::ON_CLOSE_EVENT = reinterpret_cast<butil::IOBuf*>(-1L);

int BrpcStreamRowChannelAdapter::on_received_messages(brpc::StreamId,
                                                      butil::IOBuf *const messages[],
                                                      size_t size) {
  BrpcStreamRowChannel *channel = nullptr;
  while (!(channel = channel_.load(std::memory_order_relaxed)))
    bthread_yield();
 
  for (size_t i = 0; i < size; ++i) {
    butil::IOBuf *io_buf = new butil::IOBuf;
    io_buf->swap(*messages[i]);
    channel->PushIOBuf(io_buf);
  }
  channel->event_->Set();

  return 0;
}

void BrpcStreamRowChannelAdapter::on_closed(brpc::StreamId) {
  BrpcStreamRowChannel* channel = nullptr;
  while (!(channel = channel_.load(std::memory_order_relaxed)))
    bthread_yield();

  channel->PushIOBuf(const_cast<butil::IOBuf*>(BrpcStreamRowChannel::ON_CLOSE_EVENT));
  channel->event_->Set();
  channel->close_event_.Set();
}

using Result = BrpcStreamRowChannel::Result;

BrpcStreamRowChannel::~BrpcStreamRowChannel() {
  Close();
}

bool BrpcStreamRowChannel::Init(THD *thd, Event *event, bool) {
  thd_ = thd;
  event_ = event;
  return false;
}

Result BrpcStreamRowChannel::Receive(std::size_t *nbytesp, void **datap,
                                     bool nowait, MessageBuffer *buf) {
  if (last_read_bytes_ > 0) {
    assert(recv_buf_.size() >= last_read_bytes_);
    recv_buf_.pop_front(last_read_bytes_);
    last_read_bytes_ = 0;
  }

  // Read header
  while (recv_buf_.size() < HEADER_SIZE) {
    Result res = ReadNextIOBuf(nowait);
    if (res != Result::SUCCESS) return res; 
  } 

  char header_buf[HEADER_SIZE];
  recv_buf_.copy_to(header_buf, HEADER_SIZE);
  static_assert(sizeof(FrameType) == sizeof(char));
  FrameType frame_type = header_buf[0];
  uint32_t body_size = 0;
  butil::RawUnpacker(header_buf + sizeof(frame_type)).unpack32(body_size);

  if (frame_type == FRAME_EOF) {
    recv_buf_.pop_front(HEADER_SIZE);
    return Result::DETACHED;
  }

  assert (frame_type == FRAME_DATA);
  if (frame_type != FRAME_DATA) {
    LogError("Receive unknown frame type : %c", frame_type);
    my_error(ER_PARALLEL_ROW_CHANNEL_FRAME_ERROR, MYF(0));
    return Result::ERROR;
  }

  // Read body
  while (recv_buf_.size() < body_size + HEADER_SIZE) {
    Result res = ReadNextIOBuf(nowait);
    if (res != Result::SUCCESS) return res;
  }

  recv_buf_.pop_front(HEADER_SIZE);

  // If the row message belongs to a contiguous segment of memory in
  // recv_buf_, Receive will only return the memory address and record
  // the length of the row message to last_read_bytes_.
  // The next time Receive is called, it will pop last_read_bytes_ bytes
  // from recv_buf_ and continue reading the next row message.
  assert(recv_buf_.backing_block_num() >= 1);
  butil::StringPiece slice = recv_buf_.backing_block(0);
  if (slice.size() >= body_size) {
    *datap = const_cast<char*>(slice.data());
    last_read_bytes_ = body_size;
    *nbytesp = last_read_bytes_;
    return Result::SUCCESS;
  }

  // The row message does not belong to a continuous memory, so
  // we can only copy the data to the parameter buf and return
  if (buf->buflen < body_size) {
    if (buf->reserve(body_size)) return Result::ERROR;
  }

  recv_buf_.cutn(buf->buf, body_size);
  buf->buflen = body_size;

  *datap = buf->buf;
  *nbytesp = body_size;
  return Result::SUCCESS;
}

Result BrpcStreamRowChannel::Send(std::size_t nbytes, const void *data, bool nowait) {
  Result res = Result::SUCCESS; 

  // Send header
  while (send_partial_bytes_ < HEADER_SIZE) {
    char header_buf[HEADER_SIZE];
    header_buf[0] = FRAME_DATA;
    butil::RawPacker(header_buf + sizeof(FRAME_DATA)).pack32(nbytes);
    size_t send_bytes = 0;
    res = SendBytes(HEADER_SIZE - send_partial_bytes_, 
                    header_buf + send_partial_bytes_, nowait, &send_bytes);
    send_partial_bytes_ += send_bytes;
    if (res != Result::SUCCESS) return res;
  } 

  // Send body
  while (send_partial_bytes_ < HEADER_SIZE + nbytes) {
    size_t offset = send_partial_bytes_ - HEADER_SIZE;
    size_t send_bytes = 0;
    res = SendBytes(nbytes-offset, pointer_cast<const uchar *>(data) + offset, nowait, &send_bytes);
    send_partial_bytes_ += send_bytes;
    if (res != Result::SUCCESS) return res;
  }

  send_partial_bytes_ = 0;

  return res;
}

Result BrpcStreamRowChannel::SendBytes(std::size_t nbytes, const void *data,
                                       bool nowait, std::size_t *send_bytes) {
  Result res = Result::SUCCESS;

  *send_bytes = 0;

  while (*send_bytes < nbytes) {
    size_t left_space = SEND_BUFFER_SIZE - send_buf_.size();
    size_t remain_bytes = nbytes - *send_bytes;
    size_t append_bytes = std::min(remain_bytes, left_space);

    send_buf_.append(pointer_cast<const uchar *>(data) + *send_bytes, append_bytes);

    *send_bytes += append_bytes;

    if (send_buf_.size() >= SEND_BUFFER_SIZE) {
      res = FlushSendBuf(nowait);
      if (res != Result::SUCCESS) return res;
    }
  }

  return res;
}

Result BrpcStreamRowChannel::SendEOF() {
  char header_buf[HEADER_SIZE];
  header_buf[0] = FRAME_EOF;
  butil::RawPacker(header_buf + sizeof(FRAME_EOF)).pack32(0);

  size_t send_bytes = 0;
  Result res = SendBytes(HEADER_SIZE, header_buf, false, &send_bytes);
  if (res != Result::SUCCESS) return res;
  return FlushSendBuf(false);
}

void BrpcStreamRowChannel::Close() {
  if (IsClosed()) return;

  FlushSendBuf(false);

  brpc::StreamClose(stream_id_);

  // We must wait until the call to BrpcStreamRowChannelAdapter::on_closed
  // completes before we can ensure that stream_adapter_ will not invoke
  // BrpcStreamRowChannelAdapter::on_received_messages again to access
  // dangling pointers.
  WaitClosed(); 

  stream_id_ = brpc::INVALID_STREAM_ID;
}

bool BrpcStreamRowChannel::IsClosed() const {
  return stream_id_ == brpc::INVALID_STREAM_ID;
}

void BrpcStreamRowChannel::WaitClosed() {
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

void BrpcStreamRowChannel::OnWritable(brpc::StreamId,
                                      void *param, int) {
  Event *event = (Event *)param;
  event->Set();
}

Result BrpcStreamRowChannel::Flush(const butil::IOBuf &io_buf, bool nowait) {
  if (io_buf.empty()) return Result::SUCCESS;

  while (true) {
    if (thd_->killed) return Result::ERROR;

    int res = brpc::StreamWrite(stream_id_, io_buf);
    if (!res) break;

    if (res != EAGAIN) {
      MyOsError(res, ER_PARALLEL_ROW_CHANNEL_TX_ERROR, MYF(0));
      return Result::ERROR;
    }

    brpc::StreamWait(stream_id_, nullptr, OnWritable, (void*)event_);

    if (nowait) return Result::WOULD_BLOCK;

    event_->Wait(thd_);
  }

  return Result::SUCCESS;
}

Result BrpcStreamRowChannel::FlushSendBuf(bool nowait) {
  Result res = Flush(send_buf_, nowait);
  if (res != Result::SUCCESS) return res;
  send_buf_.clear();
  return res;
}

Result BrpcStreamRowChannel::ReadNextIOBuf(bool nowait) {
  butil::IOBuf* io_buf = nullptr;
  while (!(io_buf = PopIOBuf())) {
    if (thd_->killed) return Result::ERROR;

    if (nowait) return Result::WOULD_BLOCK;

    event_->Wait(thd_);
  }

  // TODO: Add a new Result type so that the upper layer
  // can do some special handling for this situation
  if (io_buf == ON_CLOSE_EVENT) return Result::ERROR;

  recv_buf_.append(*io_buf);
  delete io_buf;

  return Result::SUCCESS;
}

void BrpcStreamRowChannel::PushIOBuf(butil::IOBuf* io_buf) {
  while (!recv_queue_.push(io_buf)) {
    event_->Set();
    recv_queue_event_.Wait(nullptr);
  }
}

butil::IOBuf* BrpcStreamRowChannel::PopIOBuf() {
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

RowChannel *CreateBrpcStreamRowChannel(MEM_ROOT *mem_root,
                                       brpc::StreamId stream_id,
                                       BrpcStreamRowChannelAdapterPtr stream_adapter) {
  return new (mem_root) BrpcStreamRowChannel(stream_id, std::move(stream_adapter));
}

bool CreateBrpcStreamRowChannelPair(MEM_ROOT *mem_root, RowChannel **receiver,
                                    RowChannel **sender) {
  brpc::StreamId recv_stream_id;
  comm::BrpcStreamRowChannelAdapterPtr receiver_adapter(new comm::BrpcStreamRowChannelAdapter);

  brpc::Controller cntl;
  brpc::StreamOptions options;
  options.handler = receiver_adapter.get();
  if (brpc::StreamCreate(&recv_stream_id, cntl, &options)) {
    return true;
  }

  std::shared_ptr<tdsql::TDStoreClient> client = tdsql::GetTDStoreClient(tdsql_rpc_addr);

  if (!client) return true;

  brpc::Channel* chan = client->channel();
  if (!chan) return true;

  tdsql::SQLEngineService_Stub stub(static_cast<google::protobuf::RpcChannel*>(chan));

  tdsql::LocalStreamRequest req;
  tdsql::LocalStreamResponse resp;
  stub.LocalStream(&cntl, &req, &resp, nullptr);

  if (cntl.Failed()) {
    LogError("Fail to create local stream because of LocalStreamRequest return error %s",
             cntl.ErrorText().c_str());
    return true;
  }

  if (resp.head().error_code() != tdcomm::EC_OK) {
    LogError("Fail to create local stream because of LocalStreamRequest return error %s",
             resp.head().error_msg().c_str());
    return true;
  }

  brpc::StreamId send_stream_id = resp.accept_stream_id();
  std::unique_ptr<BrpcStreamRowChannelAdapter> sender_adapter(
      reinterpret_cast<BrpcStreamRowChannelAdapter*>(resp.handler_ptr()));

  *receiver = CreateBrpcStreamRowChannel(mem_root, recv_stream_id, std::move(receiver_adapter));
  *sender = CreateBrpcStreamRowChannel(mem_root, send_stream_id, std::move(sender_adapter));

  return !(*receiver && *sender);
}

} // namespace comm
} // namespace pq
