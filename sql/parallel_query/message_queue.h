#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#include <atomic>
#include <cstdint>
#include "my_base.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"

class THD;
namespace pq {
class MessageQueueEvent {
 public:
  MessageQueueEvent() : m_set(false) {
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
  bool m_set;
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
};

enum class MessageQueueResult {
  NONE,
  SUCCESS,
  END,
  WOULD_BLOCK,
  DETACHED,
  OOM,
  KILLED,
  ERROR
};

class MessageQueue {
 public:
  friend class MessageQueueHandle;

  MessageQueue() : m_detached(false) {}
  MessageQueue(const MessageQueue &) = delete;
  virtual ~MessageQueue(){}

  inline void SetSenderEvent(MessageQueueEvent *sender_event) {
    m_sender_event = sender_event;
  }
  inline void SetReceiverEvent(MessageQueueEvent *receiver_event) {
    m_receiver_event = receiver_event;
  }

  virtual bool Init(THD *thd) = 0;
  inline void NotifySender() { m_sender_event->Set(); }
  inline void SenderWait(THD *thd) { m_sender_event->Wait(thd); }
  inline void NotifyReceiver() { m_receiver_event->Set(); }
  inline void ReceiverWait(THD *thd) { m_receiver_event->Wait(thd); }
  void Detach() {
    m_detached = true;

    HandleDetach();

    NotifySender();
    NotifyReceiver();
  }

 protected:
  MessageQueueEvent *m_sender_event;
  MessageQueueEvent *m_receiver_event;

  bool m_detached;
  virtual void HandleDetach() {}
};

class MemMessageQueue : public MessageQueue {
 public:
  friend class MemMessageQueueHandle;
  MemMessageQueue(std::size_t queue_size);
  virtual ~MemMessageQueue(){}
  bool Init(THD *thd) override;

 protected:
  void HandleDetach() override {
    atomic_thread_fence(std::memory_order_seq_cst);
  }
  void IncreaseBytesWritten(std::size_t n);
  void IncreaseBytesRead(std::size_t n);
  MessageQueueResult SendBytes(std::size_t nbytes, const void *data,
                                bool nowait, std::size_t *bytes_written,
                                THD *thd);
  MessageQueueResult ReceiveBytes(std::size_t bytes_needed, bool nowait,
                                   std::size_t *nbytesp, void **datap,
                                   std::size_t *consume_pending, THD *thd);

  std::size_t Size() { return m_ring_size; }

  std::atomic<std::uint64_t> m_bytes_written;
  std::atomic<std::uint64_t> m_bytes_read;
  char *m_buffer;
  std::size_t m_ring_size;
};

class MessageQueueHandle {
 public:
  MessageQueueHandle(MessageQueue *smq, THD *thd_arg);
  MessageQueueHandle(const MessageQueueHandle &) = delete;
  virtual ~MessageQueueHandle(){}
  virtual MessageQueueResult Send(std::size_t nbytes, const void *data,
                                  bool nowait) = 0;
  virtual MessageQueueResult Receive(std::size_t *nbytesp, void **datap,
                                     bool nowait) = 0;
  void Detach();
  ha_rows RowsSent() { return m_rows_sent; }

  inline bool IsClosed() { return m_closed; }
  inline void SetClosed() { m_closed = true; }

 protected:
  MessageQueue *m_queue;
  THD *thd;

  /// The flag is set which means sender/receiver does not send/receive messages
  /// any more.
  bool m_closed;
  ha_rows m_rows_sent;
  void IncreaseRowsSent() { m_rows_sent++; }
};

class MemMessageQueueHandle : public MessageQueueHandle {
 public:
  MemMessageQueueHandle(MessageQueue *smq, THD *thd_arg);
  MessageQueueResult Send(std::size_t nbytes, const void *data,
                          bool nowait) override;
  MessageQueueResult Receive(std::size_t *nbytesp, void **datap,
                             bool nowait) override;

 private:
  /*
    For messages that are larger or happen to wrap, we reassemble the
    message locally by copying the chunks into a local buffer.
    m_buffer is the buffer, and m_buflen is the number of bytes
    allocated for it.
  */
  char *m_buffer;
  std::size_t m_buflen;
  /*
    Saves the count of bytes currently produced or consumed, try to notify
    sender or receiver and reset to zero when it exceeds a certain water level.
  */
  std::size_t m_pending_bytes;
  std::size_t m_partial_bytes;
  std::size_t m_expected_bytes;
  bool m_length_word_complete;
  // The initial size of m_buffer, could be enlarged by large message size.
  static constexpr size_t initial_buffer_size{65536};
};
}  // namespace pq
#endif
