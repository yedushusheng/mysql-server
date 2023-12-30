#ifndef PARALLEL_QUERY_ROW_EXCHANGE_H
#define PARALLEL_QUERY_ROW_EXCHANGE_H

#include <functional>
#include "sql/parallel_query/message_queue.h"
#include "sql/parallel_query/merge_sort.h"

class Filesort;
struct MY_BITMAP;
namespace pq {
  using RowExchangeResult = MessageQueueResult;

class RowExchange {
 public:
  enum class Type {
    SENDER,    // Send records to message queues.
    RECEIVER,  // Recv records from message queues.
  };
  RowExchange(uint num_queues, Type ex_type)
      : m_num_queues(num_queues), m_type(ex_type) {}
  bool Init(MEM_ROOT *mem_root, std::function<MessageQueue *(uint)> get_queue);
  uint NumQueues() const { return m_num_queues; }
  MessageQueue *Queue(uint i) const { return m_message_queues[i]; }
  void Wait(THD *thd) { m_message_queue_event.Wait(thd); }

 private:
  MessageQueue **m_message_queues{nullptr};
  MessageQueueEvent m_message_queue_event;
  uint m_num_queues;
  Type m_type;
};

class RowExchangeContainer {
 public:
  RowExchangeContainer(RowExchange *row_exchange)
      : m_row_exchange(row_exchange) {}
  RowExchangeContainer(const RowExchangeContainer &) = delete;
  virtual ~RowExchangeContainer();

  //bool Init(THD *thd);
  virtual bool Init(THD *thd, MY_BITMAP *closed_queues);

 protected:
  bool IsQueueClosed(uint queue) {
    return m_message_queue_handles[queue]->IsClosed();
  }
  void CloseQueue(uint queue) { m_message_queue_handles[queue]->SetClosed(); }
  RowExchange *m_row_exchange;
  MessageQueueHandle **m_message_queue_handles{nullptr};
};

/// Normal collect rows from multiple workers for normal gather operator.
class RowExchangeReader : public RowExchangeContainer {
 public:
  RowExchangeReader(RowExchange *row_exchange)
      : RowExchangeContainer(row_exchange),
        m_left_queues(row_exchange->NumQueues()) {}

  virtual RowExchangeResult Read(THD *thd, uchar **buf);

 protected:
  uint m_left_queues;
 private:
  void AdvanceQueue() {
    uint queues = m_row_exchange->NumQueues();
    do {
      if (++m_next_queue >= queues) m_next_queue = 0;
    } while (IsQueueClosed(m_next_queue));
  }
  uint m_next_queue{0};
};

class RowExchangeWriter : public RowExchangeContainer {
 public:
  RowExchangeWriter(RowExchange *row_exchange)
      : RowExchangeContainer(row_exchange) {}
  RowExchangeResult Write(uchar *record, size_t nbytes);
  void WriteEOF();

 protected:
  RowExchangeResult WriteToQueue(uint queue, uchar *data, size_t nbytes,
                                 bool nowait);
};

}
#endif