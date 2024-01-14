#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/message_queue.h"

#include "sql/sql_class.h"

namespace pq {
namespace comm {
constexpr uint message_queue_ring_size = 65536;

void Event::Wait(THD *thd, bool auto_reset) {
  mysql_mutex_lock(&m_mutex);
  thd->ENTER_COND(&m_cond, &m_mutex, nullptr, nullptr);

  while (!m_set && likely(!thd->killed)) mysql_cond_wait(&m_cond, &m_mutex);

  if (auto_reset && likely(m_set)) m_set = false;

  mysql_mutex_unlock(&m_mutex);

  thd->EXIT_COND(nullptr);
}

bool MessageBuffer::reserve(std::size_t len) {
  if (buflen >= len) return false;

  assert(len < MaxMessageSize);

  uchar *newbuf;
  if (!(newbuf =
            (uchar *)my_realloc(PSI_NOT_INSTRUMENTED, buf, len, MYF(MY_WME))))
    return true;

  buf = newbuf;
  buflen = len;

  return false;
}

MessageBuffer::~MessageBuffer() { my_free(buf); }

class MemRowChannel : public RowChannel {
 public:
  MemRowChannel() = default;
  MemRowChannel(MemRowChannel *peer)
      : m_message_queue(peer->m_message_queue), m_message_queue_owned(false) {
    assert(m_message_queue);
  }
  ~MemRowChannel() {
    ::destroy(m_message_queue_handle);
    if (m_message_queue_owned) ::destroy(m_message_queue);
  }
  bool Init(THD *thd, Event *event, bool) override {
    if (m_message_queue_owned) {
      assert(!m_message_queue);
      if (!(m_message_queue =
                new (thd->mem_root) MessageQueue(message_queue_ring_size)) ||
          m_message_queue->Init(thd->mem_root))
        return true;
    }

    if (!(m_message_queue_handle = new (thd->mem_root)
              MessageQueueHandle(m_message_queue, thd, event)))
      return true;

    return false;
  }
  Result Send(std::size_t nbytes, const void *data, bool nowait) override {
    return m_message_queue_handle->Send(nbytes, data, nowait);
  }
  Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                 MessageBuffer *buf) override {
    return m_message_queue_handle->Receive(nbytesp, datap, nowait, buf);
  }
  Result SendEOF() override {
    m_message_queue_handle->SetEOF();
    return Result::SUCCESS;
  }
  void Close() override {
    assert(!m_closed);
    m_message_queue_handle->Detach();
    m_closed = true;
  }
  bool IsClosed() const override { return m_closed; }
  void SetPeerEvent(Event *peer_event) {
    assert(m_message_queue_handle);
    m_message_queue_handle->SetPeerEvent(peer_event);
  }

 private:
  MessageQueue *m_message_queue{nullptr};
  MessageQueueHandle *m_message_queue_handle{nullptr};
  bool m_message_queue_owned{true};
  /**
    The flag is set which means sender/receiver does not send/receive messages
    any more.
  */
  bool m_closed{false};
};

/// Create a memory row channel and create an owned message queue if @param
/// peer is not given otherwise connect to peer's message queue.
RowChannel *CreateMemRowChannel(MEM_ROOT *mem_root, RowChannel *peer) {
  MemRowChannel *channel =
      peer ? new (mem_root) MemRowChannel(down_cast<MemRowChannel *>(peer))
           : new (mem_root) MemRowChannel;
  return channel;
}

void SetPeerEventForMemChannel(RowChannel *channel, Event *peer_event) {
  assert(peer_event);
  down_cast<MemRowChannel *>(channel)->SetPeerEvent(peer_event);
}
}  // namespace comm
}  // namespace pq
