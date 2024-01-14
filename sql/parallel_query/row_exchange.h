#ifndef PARALLEL_QUERY_ROW_EXCHANGE_H
#define PARALLEL_QUERY_ROW_EXCHANGE_H

#include <functional>
#include "sql/parallel_query/comm_types.h"
#include "sql/sql_array.h"

class Filesort;
struct MY_BITMAP;
class TABLE;

namespace pq {
namespace comm {
class RowSegmentCodec;
struct RowDataInfo;
class RowChannel;

class RowExchange {
 public:
  enum Result { SUCCESS, END, ERROR};
  RowExchange() = default;
  bool Init(MEM_ROOT *mem_root, uint num_channels,
            std::function<RowChannel *(uint)> get_channel,
            MY_BITMAP *closed_queues = nullptr);
  void Reset() {
    m_channel_array.reset();
    m_event.Reset();
  }
  uint NumChannels() const { return m_channel_array.size(); }
  RowChannel *Channel(uint i) const { return m_channel_array[i]; }
  Event *event() { return &m_event; }
  void Wait(THD *thd) { m_event.Wait(thd); }
  void CloseChannel(uint chn);
  bool IsChannelClosed(uint chn) const;

 private:
  /// Channel array, Exchange doesn't own this channels.
  Bounds_checked_array<RowChannel *> m_channel_array;
  Event m_event;
};

class RowExchangeReader {
 public:
  using Result = RowExchange::Result;
  RowExchangeReader(RowExchange *row_exchange) : m_row_exchange(row_exchange) {}
  virtual bool Init(THD *thd) = 0;
  virtual Result Read(THD *thd, uchar *dest, std::size_t nbytes) = 0;

 protected:
  RowExchange *m_row_exchange;
  RowSegmentCodec *m_row_segment_codec{nullptr};
};

class RowExchangeWriter {
 public:
  using Result = RowExchange::Result;
  void SetExchange(RowExchange *row_exchange) { m_row_exchange = row_exchange; }
  Result Write(const uchar *record, size_t nbytes);
  void WriteEOF();
  bool CreateRowSegmentCodec(MEM_ROOT *mem_root, TABLE *table);

 private:
  RowExchange *m_row_exchange{nullptr};
  RowSegmentCodec *m_row_segment_codec{nullptr};
};

RowExchangeReader *CreateRowExchangeReader(MEM_ROOT *mem_root,
                                           RowExchange *row_exchange,
                                           TABLE *table, Filesort *merge_sort);
}  // namespace comm
}  // namespace pq

#endif
