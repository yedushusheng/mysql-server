#include "sql/parallel_query/row_exchange.h"

#include "sql/parallel_query/merge_sort.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/parallel_query/row_segment.h"
#include "sql/sql_class.h"

namespace pq {
namespace comm {

void RowExchange::CloseChannel(uint chn) { m_channel_array[chn]->Close(); }
bool RowExchange::IsChannelClosed(uint chn) const {
  return m_channel_array[chn]->IsClosed();
}
bool RowExchange::Init(MEM_ROOT *mem_root, uint num_channels,
                       std::function<RowChannel *(uint)> get_channel) {
  RowChannel **channels = new (mem_root) RowChannel *[num_channels];
  if (!channels) return true;

  m_channel_array.reset(channels, num_channels);
  for (uint i = 0; i < num_channels; i++) {
    auto *channel = get_channel(i);
    m_channel_array[i] = channel;
  }

  return false;
}

static RowChannel::Result ReadOneRow(RowChannel *channel, uchar *dest,
                                     std::size_t nbytes [[maybe_unused]],
                                     bool nowait, bool buffer_all,
                                     RowDataInfo *rowdata,
                                     RowSegmentCodec *segcodec) {
  auto segments = rowdata->num_segments();
  auto *curseg = rowdata->segment(0);
  auto result = channel->Receive(&curseg->length, (void **)&curseg->data,
                                 nowait, curseg->buffer());

  if (unlikely(result != RowChannel::Result::SUCCESS)) return result;
  assert(nbytes == curseg->length);
  // Copy it to destination that is caller specified so that we can decode
  // blob fields correctly.
  if (dest != curseg->data) memcpy(dest, curseg->data, curseg->length);
  // Receive extra segments in waiting mode but we should copy it to buffer
  // to avoid be overwritten by subsequent messages.
  for (uint seg = 1; seg < segments; seg++) {
    if (!segcodec->SegmentHasData(seg, dest)) continue;

    curseg = rowdata->segment(seg);

    if ((result = channel->Receive(&curseg->length, (void **)&curseg->data,
                                   false, curseg->buffer())) !=
        RowChannel::Result::SUCCESS)
      break;

    if (buffer_all || seg != segments - 1)
      if (curseg->buffer_data()) return RowChannel::Result::ERROR;
    segcodec->SetSegment(seg, dest, curseg->data, curseg->length);
  }

  return result;
}

RowExchange::Result RowExchangeWriter::Write(const uchar *record,
                                             size_t nbytes) {
  assert(m_row_exchange->NumChannels() == 1);
  RowChannel::Result result;
  uint chn = 0;
  if (m_row_exchange->IsChannelClosed(chn)) return Result::SUCCESS;

  if ((result = m_row_exchange->Channel(chn)->Send(nbytes, record, false)) ==
      RowChannel::Result::SUCCESS) {
    for (uint i = 1; i < m_row_segment_codec->NumSegments(); i++) {
      if (!m_row_segment_codec->SegmentHasData(i, record)) continue;
      const uchar *seg;
      uint32 seglen;
      m_row_segment_codec->GetSegment(i, record, &seg, &seglen);
      if ((result = m_row_exchange->Channel(chn)->Send(seglen, seg, false)) !=
          RowChannel::Result::SUCCESS)
        break;
    }
  }
  switch (result) {
    case RowChannel::Result::SUCCESS:
      return Result::SUCCESS;
    case RowChannel::Result::DETACHED:
      m_row_exchange->CloseChannel(chn);
      return Result::SUCCESS;
    case RowChannel::Result::ERROR:
      return Result::ERROR;
    case RowChannel::Result::WOULD_BLOCK:
      assert(0);
  }

  return Result::SUCCESS;
}

void RowExchangeWriter::WriteEOF() {
  constexpr uint chn = 0;
  assert(m_row_exchange->NumChannels() == 1);
  assert(!m_row_exchange->Channel(chn)->IsClosed());
  // TODO: Handle ERROR here
  m_row_exchange->Channel(chn)->SendEOF();
}

bool RowExchangeWriter::CreateRowSegmentCodec(MEM_ROOT *mem_root,
                                              TABLE *table) {
  if (!(m_row_segment_codec = new (mem_root) RowSegmentCodec(table)))
    return true;
  return false;
}

/*
  DO NOT export this row exchange implementation outside.
*/

/// Normal collect rows from multiple workers for normal gather operator.
class RowExchangeFIFOReader : public RowExchangeReader{
 public:
  RowExchangeFIFOReader(RowExchange *row_exchange)
      : RowExchangeReader(row_exchange),
        m_left_channels(row_exchange->NumChannels()) {
    assert(m_left_channels > 0);
  }
  ~RowExchangeFIFOReader() {
    DestroyRowDataInfoArray(m_row_data, m_row_exchange->NumChannels());
  }

  bool Init(THD *thd) override {
    // Recalculate left channels because channels of started failed workers
    // are closed by collector.
    for (uint chn = 0; chn < m_row_exchange->NumChannels(); ++chn) {
      if (m_row_exchange->IsChannelClosed(chn)) --m_left_channels;
    }

    if (!(m_row_data = AllocRowDataInfoArray(
              thd->mem_root, m_row_exchange->NumChannels(),
              m_row_segment_codec->NumSegments())))
      return true;

    return false;
  }

  bool CreateRowSegmentCodec(MEM_ROOT *mem_root, TABLE *table) {
    if (!(m_row_segment_codec = new (mem_root) RowSegmentCodec(table)))
      return true;
    return false;
  }

  Result Read(THD *thd, uchar *dest, std::size_t nbytes) override {
    assert(m_left_channels > 0);
    uint visited = 0;
    for (;;) {
      if (thd->killed) return Result::ERROR;
      auto result = ReadOneRow(m_row_exchange->Channel(m_next_channel), dest,
                               nbytes, true, false, &m_row_data[m_next_channel],
                               m_row_segment_codec);
      if (result == RowChannel::Result::SUCCESS) return Result::SUCCESS;

      if (result == RowChannel::Result::DETACHED) {
        m_row_exchange->CloseChannel(m_next_channel);
        m_left_channels--;

        if (unlikely(m_left_channels == 0)) break;

        AdvanceChannel();
        continue;
      }

      // Could be ERROR
      if (result == RowChannel::Result::ERROR) return Result::ERROR;
      assert(result == RowChannel::Result::WOULD_BLOCK);

      /*
        Advance nextreader pointer in round-robin fashion. Note that we only
        reach this code if we weren't able to get a tuple from the current
        worker.
       */
      AdvanceChannel();

      visited++;
      if (visited >= m_left_channels) {
        /* Nothing to do except wait for developments. */
        m_row_exchange->Wait(thd);
        visited = 0;
      }
    }

    return Result::END;
  }

 private:
  void AdvanceChannel() {
    uint chns = m_row_exchange->NumChannels();
    do {
      if (++m_next_channel >= chns) m_next_channel = 0;
    } while (m_row_exchange->IsChannelClosed(m_next_channel));
  }

  uint m_left_channels;
  uint m_next_channel{0};
  RowDataInfo *m_row_data{nullptr};
};

/// Collect rows for multiple workers for gather operator with merge sort
class RowExchangeMergeSortReader : public RowExchangeReader, MergeSortSource {
 public:
  RowExchangeMergeSortReader(RowExchange *row_exchange, Filesort *filesort)
      : RowExchangeReader(row_exchange),
        m_filesort(filesort),
        m_mergesort(this) {}

  bool Init(THD *thd) override {
    if (!(m_row_segment_codec =
              CreateRowSegmentCodec(thd->mem_root, m_filesort)))
      return true;
    if (m_mergesort.Init(thd, m_filesort, m_row_exchange->NumChannels()) ||
        m_mergesort.Populate(thd))
      return true;

    return false;
  }

  Result Read(THD *, uchar *buf, std::size_t nbytes) override {
    uchar *data;
    auto result = m_mergesort.Read(&data);

    switch (result) {
      case MergeSort::Result::SUCCESS:
        memcpy(buf, data, nbytes);
        return Result::SUCCESS;
      case MergeSort::Result::END:
        return Result::END;
      case MergeSort::Result::ERROR:
        return Result::ERROR;
      case MergeSort::Result::NODATA:
        assert(false);
    }
    return Result::SUCCESS;
  }

  bool IsChannelFinished(uint i) override {
    return m_row_exchange->IsChannelClosed(i);
  }
  void Wait(THD *thd) override { m_row_exchange->Wait(thd); }

  MergeSort::Result ReadFromChannel(uint index, uchar *dest, size_t nbytes,
                                    bool nowait,
                                    RowDataInfo *rowdata) override {
    auto result = ReadOneRow(m_row_exchange->Channel(index), dest, nbytes,
                             nowait, true, rowdata, m_row_segment_codec);
    switch (result) {
      case RowChannel::Result::SUCCESS:
        break;
      case RowChannel::Result::DETACHED:
        m_row_exchange->CloseChannel(index);
        return MergeSort::Result::NODATA;
      case RowChannel::Result::WOULD_BLOCK:
        // no wait , return now
        return MergeSort::Result::NODATA;
      case RowChannel::Result::ERROR:
        return MergeSort::Result::ERROR;
    }

    assert(result == RowChannel::Result::SUCCESS);

    return MergeSort::Result::SUCCESS;
  }

 private:
  Filesort *m_filesort;
  MergeSort m_mergesort;
};

RowExchangeReader *CreateRowExchangeReader(MEM_ROOT *mem_root,
                                           RowExchange *row_exchange,
                                           TABLE *table, Filesort *merge_sort) {
  if (merge_sort)
    return new (mem_root) RowExchangeMergeSortReader(row_exchange, merge_sort);
  auto *reader = new (mem_root) RowExchangeFIFOReader(row_exchange);
  if (!reader || reader->CreateRowSegmentCodec(mem_root, table)) return nullptr;
  return reader;
}

}  // namespace comm
}  // namespace pq
