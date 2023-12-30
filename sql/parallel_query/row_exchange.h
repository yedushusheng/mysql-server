#ifndef PARALLEL_QUERY_ROW_EXCHANGE_H
#define PARALLEL_QUERY_ROW_EXCHANGE_H

#include <functional>
#include "sql/parallel_query/merge_sort.h"
#include "sql/parallel_query/message_queue.h"

class Filesort;
struct MY_BITMAP;

namespace pq {
class RowExchange {
 public:
  enum Result { SUCCESS, END, ERROR, OOM, KILLED };
  RowExchange(uint num_queues) : m_num_queues(num_queues) {}
  bool Init(MEM_ROOT *mem_root, std::function<MessageQueue *(uint)> get_queue);
  uint NumQueues() const { return m_num_queues; }
  MessageQueue *Queue(uint i) const { return m_message_queues[i]; }
  void Wait(THD *thd) { m_message_queue_event.Wait(thd); }

 private:
  MessageQueue **m_message_queues{nullptr};
  MessageQueueEvent m_message_queue_event;
  uint m_num_queues;
};

class RowExchangeContainer {
 public:
  using Result = RowExchange::Result;
  RowExchangeContainer(RowExchange *row_exchange)
      : m_row_exchange(row_exchange) {}
  RowExchangeContainer(const RowExchangeContainer &) = delete;
  virtual ~RowExchangeContainer();

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
  RowExchangeReader(RowExchange *row_exchange,
                    std::function<bool(uint)> queue_detach_handler)
      : RowExchangeContainer(row_exchange),
        m_left_queues(row_exchange->NumQueues()),
        m_queue_detach_handler(queue_detach_handler) {}

  virtual Result Read(THD *thd, uchar **buf);

 protected:
  bool HandleQueueDetach(uint index) {
    CloseQueue(index);
    return m_queue_detach_handler(index);
  }

 private:
  void AdvanceQueue() {
    uint queues = m_row_exchange->NumQueues();
    do {
      if (++m_next_queue >= queues) m_next_queue = 0;
    } while (IsQueueClosed(m_next_queue));
  }
  uint m_left_queues;
  std::function<bool(uint)> m_queue_detach_handler;

  uint m_next_queue{0};
};

class RowExchangeWriter : public RowExchangeContainer {
 public:
  RowExchangeWriter(RowExchange *row_exchange)
      : RowExchangeContainer(row_exchange) {}
  Result Write(uchar *record, size_t nbytes);
  void WriteEOF();
};

/// Collect rows for multiple workers for gather operator with merge sort
class RowExchangeMergeSortReader : public RowExchangeReader, MergeSortSource {
 public:
  RowExchangeMergeSortReader(RowExchange *row_exchange, Filesort *filesort,
                             std::function<bool(uint)> queue_detach_handler)
      : RowExchangeReader(row_exchange, queue_detach_handler),
        m_filesort(filesort),
        m_mergesort(this) {}
  bool Init(THD *thd, MY_BITMAP *closed_queues) override;
  Result Read(THD *thd, uchar **buf) override;

  bool IsChannelFinished(uint i) override { return IsQueueClosed(i); }
  void Wait(THD *thd) override { m_row_exchange->Wait(thd); }
  MergeSort::Result ReadFromChannel(uint i, size_t *nbytes, void **data,
                                    bool no_wait) override;

 private:
  Filesort *m_filesort;
  MergeSort m_mergesort;
};
}  // namespace pq
#endif