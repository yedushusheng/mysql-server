#include "sql/parallel_query/row_exchange.h"

#include "my_bitmap.h"
#include "sql/field.h"
#include "sql/filesort.h"
#include "sql/parallel_query/merge_sort.h"
#include "sql/sql_class.h"
#include "sql/table.h"

namespace pq {
/**
  One table row includes main record (TABLE::record[0]) and several BLOB
  field values. The class RowSegment represents main record or one BLOB
  field value.
*/
struct RowSegment {
  uchar *data{nullptr};
  std::size_t length{0};
};

/**
  One table row description num_segments equal to BLOB fields + 1, segment 0
  and rbuffers 0 are always for main record.
*/
struct RowDataInfo {
  RowSegment *m_row_segments;
  MessageReassembleBuffer *m_rbuffers{nullptr};
  uint m_num_segments;
  RowDataInfo(uint segs) : m_num_segments(segs) {}
  ~RowDataInfo();

  bool init(MEM_ROOT *mem_root);
  uint num_segments() const { return m_num_segments; }
  RowSegment *segment(uint seg) const;
  MessageReassembleBuffer *rbuffer(uint seg) const { return &m_rbuffers[seg]; }
};

/*
  DO NOT export this row exchange implementation outside.
*/
/// Normal collect rows from multiple workers for normal gather operator.
class RowExchangeFIFOReader : public RowExchangeReader, RowSegmentCodec {
 public:
  RowExchangeFIFOReader(RowExchange *row_exchange, TABLE *table,
                        std::function<bool(uint)> queue_detach_handler)
      : RowExchangeReader(row_exchange, queue_detach_handler),
        RowSegmentCodec(table),
        m_left_queues(row_exchange->NumQueues()) {}
  ~RowExchangeFIFOReader() {
    DestroyRowDataInfoArray(m_row_data, m_row_exchange->NumQueues());
  }

  bool Init(MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
            std::function<MessageQueueEvent *(uint)> get_peer_event) override;

  Result Read(THD *thd, uchar *dest, std::size_t nbytes) override;

 private:
  void AdvanceQueue() {
    uint queues = m_row_exchange->NumQueues();
    do {
      if (++m_next_queue >= queues) m_next_queue = 0;
    } while (IsQueueClosed(m_next_queue));
  }

  uint m_left_queues;
  uint m_next_queue{0};
  RowDataInfo *m_row_data{nullptr};
};

/// Collect rows for multiple workers for gather operator with merge sort
class RowExchangeMergeSortReader : public RowExchangeReader,
                                   MergeSortSource,
                                   RowSegmentCodec {
 public:
  RowExchangeMergeSortReader(RowExchange *row_exchange, Filesort *filesort,
                             std::function<bool(uint)> queue_detach_handler)
      : RowExchangeReader(row_exchange, queue_detach_handler),
        RowSegmentCodec(filesort->tables[0]),
        m_filesort(filesort),
        m_mergesort(this) {}
  bool Init(MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
            std::function<MessageQueueEvent *(uint)> get_peer_event) override;
  Result Read(THD *thd, uchar *buf, std::size_t nbytes) override;

  bool IsChannelFinished(uint i) override { return IsQueueClosed(i); }
  void Wait(THD *thd) override { m_message_queue_event.Wait(thd); }
  MergeSort::Result ReadFromChannel(uint index, uchar *dest, size_t nbytes,
                                    bool no_wait,
                                    RowDataInfo *rowdata) override;

 private:
  Filesort *m_filesort;
  MergeSort m_mergesort;
};

bool RowExchange::Init(MEM_ROOT *mem_root,
                       std::function<MessageQueue *(uint)> get_queue) {
  if (!(m_message_queues = new (mem_root) MessageQueue *[m_num_queues]))
    return true;

  for (uint i = 0; i < m_num_queues; i++) {
    m_message_queues[i] = get_queue(i);
    assert(m_message_queues[i]);
  }

  return false;
}

RowExchangeEndpoint::~RowExchangeEndpoint() {
  if (!m_message_queue_handles) return;
  for (uint i = 0; i < m_row_exchange->NumQueues(); i++)
    destroy(m_message_queue_handles[i]);
}

bool RowExchangeEndpoint::Init(
    MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
    std::function<MessageQueueEvent *(uint)> get_peer_event) {
  uint queues = m_row_exchange->NumQueues();

  if (!(m_message_queue_handles = new (mem_root) MessageQueueHandle *[queues]))
    return true;

  for (uint i = 0; i < queues; i++) {
    if (!(m_message_queue_handles[i] = new (mem_root)
              MemMessageQueueHandle(m_row_exchange->Queue(i), user_thd,
                                    &m_message_queue_event, get_peer_event(i))))
      return true;
    if (closed_queues && bitmap_is_set(closed_queues, i)) CloseQueue(i);
  }

  return false;
}

RowDataInfo::~RowDataInfo() { destroy_array(m_rbuffers, m_num_segments); }

bool RowDataInfo::init(MEM_ROOT *mem_root) {
  if (!(m_rbuffers =
            mem_root->ArrayAlloc<MessageReassembleBuffer>(m_num_segments)) ||
      !(m_row_segments = mem_root->ArrayAlloc<RowSegment>(m_num_segments)))
    return true;

  return false;
}

RowSegment *RowDataInfo::segment(uint seg) const {
  return &m_row_segments[seg];
}

RowDataInfo *AllocRowDataInfoArray(MEM_ROOT *mem_root, uint array_size,
                                   uint segments) {
  auto *rowdata = mem_root->ArrayAlloc<RowDataInfo>(array_size, segments);
  if (!rowdata) return rowdata;

  for (uint i = 0; i < array_size; i++) {
    if (rowdata[i].init(mem_root)) {
      DestroyRowDataInfoArray(rowdata, array_size);
      return nullptr;
    }
  }

  return rowdata;
}

void DestroyRowDataInfoArray(RowDataInfo *arr, uint array_size) {
  destroy_array(arr, array_size);
}
RowDataInfo *RowDataInfoAt(RowDataInfo *arr, uint index) { return &arr[index]; }
uint RowSegmentCodec::NumSegments() const {
  return m_table->s->blob_fields + 1;
}

bool RowSegmentCodec::SegmentHasData(uint segno, const uchar *recbuf) const {
  assert(segno > 0);
  assert(m_table->s->blob_fields > 0 and segno <= m_table->s->blob_fields);
  auto *field = down_cast<Field_blob *>(
      m_table->field[m_table->s->blob_field[segno - 1]]);

  return field->get_length(recbuf - m_table->record[0]) > 0;
}

void RowSegmentCodec::SetSegment(uint segno, const uchar *recbuf,
                                 const uchar *data, uint32 len) {
  assert(segno > 0 && len > 0);
  assert(m_table->s->blob_fields > 0 and segno <= m_table->s->blob_fields);
  auto *field = down_cast<Field_blob *>(
      m_table->field[m_table->s->blob_field[segno - 1]]);

  field->set_ptr_offset(recbuf - m_table->record[0], len, data);
}

void RowSegmentCodec::GetSegment(uint segno, const uchar *recbuf,
                                 const uchar **data, uint32 *len) const {
  assert(segno > 0);
  assert(m_table->s->blob_fields > 0 and segno <= m_table->s->blob_fields);
  auto *field = down_cast<Field_blob *>(
      m_table->field[m_table->s->blob_field[segno - 1]]);
  *len = field->get_length();
  *data = field->get_blob_data(recbuf - m_table->record[0]);
  assert(*len > 0);
}

MessageQueue::Result ReadOneRow(MessageQueueHandle *handle, uchar *dest,
                                std::size_t nbytes [[maybe_unused]],
                                bool nowait, bool buffer_all,
                                RowDataInfo *rowdata,
                                RowSegmentCodec *segcodec) {
  auto segments = rowdata->num_segments();
  auto *curseg = rowdata->segment(0);
  auto result = handle->Receive(&curseg->length, (void **)&curseg->data, nowait,
                                &rowdata->m_rbuffers[0]);

  if (unlikely(result != MessageQueue::Result::SUCCESS)) return result;
  assert(nbytes == curseg->length);
  memcpy(dest, curseg->data, curseg->length);
  // Receive extra segments in waiting mode but we should copy it to buffer
  // to avoid be overwritten by subsequent messages.
  for (uint seg = 1; seg < segments; seg++) {
    if (!segcodec->SegmentHasData(seg, dest)) continue;

    curseg = rowdata->segment(seg);
    auto *currbuf = rowdata->rbuffer(seg);

    if ((result = handle->Receive(&curseg->length, (void **)&curseg->data,
                                  false, currbuf)) !=
        MessageQueue::Result::SUCCESS)
      break;

    if ((char *)curseg->data != currbuf->buf &&
        (buffer_all || seg != segments - 1)) {
      if (currbuf->reserve(curseg->length)) return MessageQueue::Result::OOM;
      memcpy(currbuf->buf, curseg->data, curseg->length);
      curseg->data = (uchar *)currbuf->buf;
    }
    segcodec->SetSegment(seg, dest, curseg->data, curseg->length);
  }

  return result;
}

bool RowExchangeFIFOReader::Init(
    MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
    std::function<MessageQueueEvent *(uint)> get_peer_event) {
  if (RowExchangeReader::Init(mem_root, closed_queues, user_thd,
                              get_peer_event)) return true;

  if (!(m_row_data = AllocRowDataInfoArray(
            mem_root, m_row_exchange->NumQueues(), NumSegments())))
    return true;

  return false;
}

RowExchange::Result RowExchangeFIFOReader::Read(THD *thd, uchar *dest,
                                                std::size_t nbytes) {
  assert(m_left_queues > 0);
  uint visited = 0;
  for (;;) {
    if (thd->killed) return Result::KILLED;
    auto result =
        ReadOneRow(m_message_queue_handles[m_next_queue], dest, nbytes, true,
                   false, &m_row_data[m_next_queue], this);
    if (result == MessageQueue::Result::SUCCESS) return Result::SUCCESS;

    if (result == MessageQueue::Result::DETACHED) {
      if (HandleQueueDetach(m_next_queue)) return Result::ERROR;
      m_left_queues--;

      if (unlikely(m_left_queues == 0)) break;

      AdvanceQueue();
      continue;
    }

    // Could be OOM or END
    if (result == MessageQueue::Result::OOM) return Result::OOM;
    if (result == MessageQueue::Result::KILLED) return Result::KILLED;

    assert(result == MessageQueue::Result::WOULD_BLOCK);

    /*
      Advance nextreader pointer in round-robin fashion. Note that we only
      reach this code if we weren't able to get a tuple from the current
      worker.
     */
    AdvanceQueue();

    visited++;
    if (visited >= m_left_queues) {
      /* Nothing to do except wait for developments. */
      m_message_queue_event.Wait(thd);
      visited = 0;
    }
  }

  return Result::END;
}

RowExchange::Result RowExchangeWriter::Write(const uchar *record,
                                             size_t nbytes) {
  assert(m_row_exchange->NumQueues() == 1);
  MessageQueue::Result result;
  uint queue = 0;
  if (IsQueueClosed(queue)) return Result::SUCCESS;

  if ((result = m_message_queue_handles[queue]->Send(nbytes, record, false)) ==
      MessageQueue::Result::SUCCESS) {
    for (uint i = 1; i < m_row_segment_codec->NumSegments(); i++) {
      if (!m_row_segment_codec->SegmentHasData(i, record)) continue;
      const uchar *seg;
      uint32 seglen;
      m_row_segment_codec->GetSegment(i, record, &seg, &seglen);
      if ((result = m_message_queue_handles[queue]->Send(seglen, seg, false)) !=
          MessageQueue::Result::SUCCESS)
        break;
    }
  }
  switch (result) {
    case MessageQueue::Result::SUCCESS:
      return Result::SUCCESS;
    case MessageQueue::Result::OOM:
      return Result::OOM;
    case MessageQueue::Result::KILLED:
      return Result::KILLED;
    case MessageQueue::Result::DETACHED:
      CloseQueue(0);
      return Result::SUCCESS;
    case MessageQueue::Result::WOULD_BLOCK:
      assert(0);
  }

  return Result::SUCCESS;
}

void RowExchangeWriter::WriteEOF() {
  assert(m_row_exchange->NumQueues() == 1);
  m_message_queue_handles[0]->Detach();
}

bool RowExchangeWriter::CreateRowSegmentCodec(MEM_ROOT *mem_root,
                                              TABLE *table) {
  if (!(m_row_segment_codec = new (mem_root) RowSegmentCodec(table)))
    return true;
  return false;
}

bool RowExchangeMergeSortReader::Init(
    MEM_ROOT *mem_root, MY_BITMAP *closed_queues, THD *user_thd,
    std::function<MessageQueueEvent *(uint)> get_peer_event) {
  if (RowExchangeEndpoint::Init(mem_root, closed_queues, user_thd,
                                get_peer_event))
    return true;

  if (m_mergesort.Init(user_thd, m_filesort, m_row_exchange->NumQueues()) ||
      m_mergesort.Populate(user_thd))
    return true;

  return false;
}

MergeSort::Result RowExchangeMergeSortReader::ReadFromChannel(
    uint index, uchar *dest, std::size_t nbytes, bool nowait,
    RowDataInfo *rowdata) {
  auto result = ReadOneRow(m_message_queue_handles[index], dest, nbytes, nowait,
                           true, rowdata, this);
  switch (result) {
    case MessageQueue::Result::SUCCESS:
      break;
    case MessageQueue::Result::OOM:
      return MergeSort::Result::OOM;
    case MessageQueue::Result::KILLED:
      return MergeSort::Result::KILLED;
    case MessageQueue::Result::DETACHED:
      if (unlikely(HandleQueueDetach(index))) return MergeSort::Result::ERROR;
      return MergeSort::Result::NODATA;
    case MessageQueue::Result::WOULD_BLOCK:
      // no wait , return now
      return MergeSort::Result::NODATA;
  }

  assert(result == MessageQueue::Result::SUCCESS);

  return MergeSort::Result::SUCCESS;
}

RowExchange::Result RowExchangeMergeSortReader::Read(THD *, uchar *buf,
                                                     std::size_t nbytes) {
  uchar *data;
  RowDataInfo *rowdata;
  auto result = m_mergesort.Read(&data, rowdata);

  switch (result) {
    case MergeSort::Result::SUCCESS:
      memcpy(buf, data, nbytes);
      return Result::SUCCESS;
    case MergeSort::Result::END:
      return Result::END;
    case MergeSort::Result::KILLED:
      return Result::KILLED;
    case MergeSort::Result::OOM:
      return Result::OOM;
    case MergeSort::Result::ERROR:
      return Result::ERROR;
    case MergeSort::Result::NODATA:
      assert(false);
  }
  return Result::SUCCESS;
}

RowExchangeReader *CreateRowExchangeReader(
    MEM_ROOT *mem_root, RowExchange *row_exchange, TABLE *table,
    Filesort *merge_sort, std::function<bool(uint)> queue_detach_handler) {
  if (!merge_sort)
    return new (mem_root)
        RowExchangeFIFOReader(row_exchange, table, queue_detach_handler);
  assert(table == merge_sort->tables[0]);
  return new (mem_root) RowExchangeMergeSortReader(row_exchange, merge_sort,
                                                   queue_detach_handler);
}
}  // namespace pq
