#ifndef PARALLEL_QUERY_ROW_EXCHANGE_H
#define PARALLEL_QUERY_ROW_EXCHANGE_H

#include <functional>
#include "sql/parallel_query/message_queue.h"

class Filesort;
struct MY_BITMAP;
class TABLE;

namespace pq {
class RowSegmentCodec;
struct RowDataInfo;

class RowExchange {
 public:
  enum Result { SUCCESS, END, ERROR, OOM, KILLED };
  RowExchange(uint num_queues) : m_num_queues(num_queues) {}
  bool Init(MEM_ROOT *mem_root, std::function<MessageQueue *(uint)> get_queue);
  uint NumQueues() const { return m_num_queues; }
  MessageQueue *Queue(uint i) const { return m_message_queues[i]; }

 private:
  MessageQueue **m_message_queues{nullptr};
  uint m_num_queues;
};

class RowExchangeEndpoint {
 public:
  using Result = RowExchange::Result;
  RowExchangeEndpoint(RowExchange *row_exchange)
      : m_row_exchange(row_exchange) {}
  RowExchangeEndpoint(const RowExchangeEndpoint &) = delete;
  virtual ~RowExchangeEndpoint();

  virtual bool Init(MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
                    std::function<MessageQueueEvent *(uint)> get_peer_event);
  MessageQueueEvent *Event() { return &m_message_queue_event; }

 protected:
  bool IsQueueClosed(uint queue) {
    return m_message_queue_handles[queue]->IsClosed();
  }
  void CloseQueue(uint queue) { m_message_queue_handles[queue]->SetClosed(); }
  RowExchange *m_row_exchange;
  MessageQueueEvent m_message_queue_event;
  MessageQueueHandle **m_message_queue_handles{nullptr};
};

class RowExchangeReader : public RowExchangeEndpoint {
 public:
  RowExchangeReader(RowExchange *row_exchange,
                    std::function<bool(uint)> queue_detach_handler)
      : RowExchangeEndpoint(row_exchange),
        m_queue_detach_handler(queue_detach_handler) {}

  virtual Result Read(THD *thd, uchar *dest, std::size_t nbytes) = 0;

 protected:
  bool HandleQueueDetach(uint index) {
    CloseQueue(index);
    return m_queue_detach_handler(index);
  }

 private:
  std::function<bool(uint)> m_queue_detach_handler;
};

class RowExchangeWriter : public RowExchangeEndpoint {
 public:
  RowExchangeWriter(RowExchange *row_exchange)
      : RowExchangeEndpoint(row_exchange) {}
  Result Write(const uchar *record, size_t nbytes);
  void WriteEOF();
  bool CreateRowSegmentCodec(MEM_ROOT *mem_root, TABLE *table);

 private:
  RowSegmentCodec *m_row_segment_codec{nullptr};
};

class RowSegmentCodec {
 public:
  RowSegmentCodec(TABLE *table) : m_table(table) {}

  bool SegmentHasData(uint segno, const uchar *recbuf) const;
  void SetSegment(uint segno, const uchar *recbuf, const uchar *data,
                  uint32 len);
  void GetSegment(uint segno, const uchar *recbuf, const uchar **data,
                  uint32 *len) const;
  uint NumSegments() const;

 private:
  TABLE *m_table;
};

RowDataInfo *AllocRowDataInfoArray(MEM_ROOT *mem_root, uint array_size,
                                   uint segments);
void DestroyRowDataInfoArray(RowDataInfo *arr, uint array_size);
RowDataInfo *RowDataInfoAt(RowDataInfo *arr, uint index);

RowExchangeReader *CreateRowExchangeReader(
    MEM_ROOT *mem_root, RowExchange *row_exchange, TABLE *table,
    Filesort *merge_sort, std::function<bool(uint)> queue_detach_handler);
}  // namespace pq

#endif
