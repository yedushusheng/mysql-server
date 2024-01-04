#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#include <atomic>
#include <cstdint>
#include "sql/parallel_query/comm_types.h"

class MEM_ROOT;
class THD;

namespace pq {
namespace comm {
using MessageQueueEvent = Event;
using MessageReassembleBuffer = MessageBuffer;

class MessageQueueEndpointInfo {
 public:
  MessageQueueEndpointInfo() = default;
  inline void NotifyPeer() {
    assert(m_peer_event);
    m_peer_event->Set();
  }
  inline void Wait() {
    assert(m_thd && m_self_event);
    m_self_event->Wait(m_thd);
  }
  inline bool IsKilled() const;
  inline void SetSelfEvent(THD *wait_thd, MessageQueueEvent *self_event) {
    assert(!m_thd && wait_thd);
    assert(!m_peer_event || m_peer_event != self_event);
    m_thd = wait_thd;
    m_self_event = self_event;
  }
  inline void SetPeerEvent(MessageQueueEvent *peer_event) {
    assert(!m_self_event || peer_event != m_self_event);
    m_peer_event = peer_event;
  }

 private:
  /// Event wait thd, call its enter_cond() when waiting
  THD *m_thd{nullptr};
  MessageQueueEvent *m_self_event{nullptr};
  MessageQueueEvent *m_peer_event{nullptr};
};

class MessageQueue {
 public:
  using Result = RowTxResult;
  friend class MessageQueueHandle;
  MessageQueue(std::size_t queue_size);
  MessageQueue(const MessageQueue &) = delete;
  ~MessageQueue() {}

  bool Init(MEM_ROOT *mem_root);

  void Detach(MessageQueueEndpointInfo *endpoint_info) {
    m_detached.store(true, std::memory_order_relaxed);
    atomic_thread_fence(std::memory_order_seq_cst);
    endpoint_info->NotifyPeer();
  }
  /// This function must be called before Detach() so we don't need fence
  void SetEOF() { m_eof.store(true, std::memory_order_relaxed); }

 protected:
  bool IsDetached() const { return m_detached.load(std::memory_order_relaxed); }
  void IncreaseBytesWritten(std::size_t n);
  void IncreaseBytesRead(MessageQueueEndpointInfo *endpoint_info,
                         std::size_t n);
  Result SendBytes(MessageQueueEndpointInfo *endpoint_info, std::size_t nbytes,
                   const void *data, bool nowait, std::size_t *bytes_written);
  Result ReceiveBytes(MessageQueueEndpointInfo *endpoint_info,
                      std::size_t bytes_needed, bool nowait,
                      std::size_t *nbytesp, void **datap,
                      std::size_t *consume_pending);
  std::size_t Size() { return m_ring_size; }

  std::atomic<bool> m_detached{false};
  std::atomic<bool> m_eof{false};
  std::atomic<std::uint64_t> m_bytes_written;
  std::atomic<std::uint64_t> m_bytes_read;
  char *m_buffer{nullptr};
  std::size_t m_ring_size;

};

class MessageQueueHandle {
 public:
  using Result = MessageQueue::Result;
  MessageQueueHandle(MessageQueue *smq) : m_queue(smq) {}
  MessageQueueHandle(const MessageQueueHandle &) = delete;

  Result Send(std::size_t nbytes, const void *data, bool nowait);
  Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                 MessageReassembleBuffer *reassemble_buf);
  /**
    Notify counterparty that we're detaching from shared message queue.
  */
  void Detach() { m_queue->Detach(&m_endpoint_info); }

  /**
    Sender will call this, tell counter-party no messages any more.
  */
  void SetEOF() { m_queue->SetEOF(); }
  void SetPeerEvent(MessageQueueEvent *peer_event) {
    m_endpoint_info.SetPeerEvent(peer_event);
  }
  void SetSelfEvent(THD *thd, MessageQueueEvent *self_event) {
    m_endpoint_info.SetSelfEvent(thd, self_event);
  }

 private:
  /**
    Saves the count of bytes currently produced or consumed, try to notify
    sender or receiver and reset to zero when it exceeds a certain water level.
  */
  std::size_t m_pending_bytes{0};
  std::size_t m_partial_bytes{0};
  std::size_t m_expected_bytes{0};
  bool m_length_word_complete{false};

  MessageQueue *m_queue;
  MessageQueueEndpointInfo m_endpoint_info;
};
}  // namespace comm
}  // namespace pq
#endif
