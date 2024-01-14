#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#include <atomic>
#include <cstdint>
#include "my_base.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"

class MEM_ROOT;
class THD;

namespace pq {
class MessageQueueEvent {
 public:
  MessageQueueEvent() {
    mysql_mutex_init(PSI_INSTRUMENT_ME, &m_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(PSI_INSTRUMENT_ME, &m_cond);
  }
  MessageQueueEvent(const MessageQueueEvent &) = delete;
  ~MessageQueueEvent() {
    mysql_mutex_destroy(&m_mutex);
    mysql_cond_destroy(&m_cond);
  }

  inline void Set() {
    mysql_mutex_lock(&m_mutex);
    if (!m_set) {
      m_set = true;
      mysql_cond_signal(&m_cond);
    }
    mysql_mutex_unlock(&m_mutex);
  }
  inline void Reset() {
    mysql_mutex_lock(&m_mutex);
    if (m_set) m_set = false;
    mysql_mutex_unlock(&m_mutex);
  }

  void Wait(THD *thd, bool auto_reset = true);

 private:
  bool m_set{false};
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
};

class MessageQueueEndpointInfo {
 public:
  MessageQueueEndpointInfo(THD *thd, MessageQueueEvent *self_event,
                           MessageQueueEvent *peer_event)
      : m_thd(thd), m_self_event(self_event), m_peer_event(peer_event) {}

  inline void NotifyPeer() { m_peer_event->Set(); }
  inline void Wait() { m_self_event->Wait(m_thd); }
  inline bool IsKilled() const;
  inline THD *thd() const { return m_thd; }

 private:
  THD *m_thd;
  MessageQueueEvent *m_self_event;
  MessageQueueEvent *m_peer_event;
};

class MessageQueue {
 public:
  enum class Result { SUCCESS, DETACHED, WOULD_BLOCK, OOM, KILLED };
  friend class MessageQueueHandle;
  MessageQueue() = default;
  MessageQueue(const MessageQueue &) = delete;
  virtual ~MessageQueue() {}

  virtual bool Init(MEM_ROOT *mem_root) = 0;

  void Detach(MessageQueueEndpointInfo *endpoint_info) {
    m_detached.store(true, std::memory_order_relaxed);
    HandleDetach();
    endpoint_info->NotifyPeer();
  }

 protected:
  std::atomic<bool> m_detached{false};
  virtual void HandleDetach() {}
  bool IsDetached() const { return m_detached.load(std::memory_order_relaxed); }
};

class MemMessageQueue : public MessageQueue {
 public:
  friend class MemMessageQueueHandle;
  MemMessageQueue(std::size_t queue_size);
  virtual ~MemMessageQueue() {}
  bool Init(MEM_ROOT *mem_root) override;

 protected:
  void HandleDetach() override {
    atomic_thread_fence(std::memory_order_seq_cst);
  }
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

  std::atomic<std::uint64_t> m_bytes_written;
  std::atomic<std::uint64_t> m_bytes_read;
  char *m_buffer{nullptr};
  std::size_t m_ring_size;
};

class MessageQueueHandle {
 public:
  MessageQueueHandle(MessageQueue *smq, THD *thd, MessageQueueEvent *self_event,
                     MessageQueueEvent *peer_event)
      : m_queue(smq), m_endpoint_info(thd, self_event, peer_event) {}
  MessageQueueHandle(const MessageQueueHandle &) = delete;
  virtual ~MessageQueueHandle() {}
  virtual MessageQueue::Result Send(std::size_t nbytes, const void *data,
                                    bool nowait) = 0;
  virtual MessageQueue::Result Receive(std::size_t *nbytesp, void **datap,
                                       bool nowait) = 0;
  /**
    Notify counterparty that we're detaching from shared message queue.
  */
  void Detach() {
    if (m_closed) return;
    m_queue->Detach(&m_endpoint_info);
    SetClosed();
  }
  inline bool IsClosed() { return m_closed; }
  inline void SetClosed() { m_closed = true; }

 protected:
  MessageQueue *m_queue;
  MessageQueueEndpointInfo m_endpoint_info;
  /**
    The flag is set which means sender/receiver does not send/receive messages
    any more.
  */
  bool m_closed{false};
};

class MemMessageQueueHandle : public MessageQueueHandle {
  using MessageQueueHandle::MessageQueueHandle;

 public:
  MessageQueue::Result Send(std::size_t nbytes, const void *data,
                            bool nowait) override;
  MessageQueue::Result Receive(std::size_t *nbytesp, void **datap,
                               bool nowait) override;

 private:
  /**
    For messages that are larger or happen to wrap, we reassemble the message
    locally by copying the chunks into a local buffer. m_buffer is the buffer,
    and m_buflen is the number of bytes allocated for it.
  */
  char *m_buffer{nullptr};
  std::size_t m_buflen{0};
  /**
    Saves the count of bytes currently produced or consumed, try to notify
    sender or receiver and reset to zero when it exceeds a certain water level.
  */
  std::size_t m_pending_bytes{0};
  std::size_t m_partial_bytes{0};
  std::size_t m_expected_bytes{0};
  bool m_length_word_complete{false};
  /// The initial size of m_buffer, could be enlarged by large message size.
  static constexpr std::size_t initial_buffer_size{65536};
};
}  // namespace pq
#endif
