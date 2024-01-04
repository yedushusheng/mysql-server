#ifndef PARALLEL_QUERY_ROW_SEGMENT_H
#define PARALLEL_QUERY_ROW_SEGMENT_H
#include "sql/parallel_query/comm_types.h"
#include "my_alloc.h"

class TABLE;
class Filesort;

namespace pq {
namespace comm {
/**
  One table row includes main record (TABLE::record[0]) and several BLOB
  field values. The class RowSegment represents main record or one BLOB
  field value.
*/
struct RowSegment {
  MessageBuffer *buffer() { return &m_buffer; }
  bool is_data_buffered() const { return data == m_buffer.buf; }
  bool buffer_data() {
    if (is_data_buffered()) return false;
    if (m_buffer.reserve(length)) return true;
    memcpy(m_buffer.buf, data, length);
    data = m_buffer.buf;
    return false;
  }
  uchar *data{nullptr};
  std::size_t length{0};
  /// Segment data buffer, underlying channel may use this buffer to
  /// resemble segment message.
  MessageBuffer m_buffer;
};

/**
  One table row description num_segments equal to BLOB fields + 1, segment 0
  and rbuffers 0 are always for main record.
*/
struct RowDataInfo {
  RowSegment *m_segments{nullptr};
  uint m_num_segments;
  RowDataInfo(uint segs) : m_num_segments(segs) {}
  ~RowDataInfo() { destroy_array(m_segments, m_num_segments); }

  bool init(MEM_ROOT *mem_root) {
    if (!(m_segments = mem_root->ArrayAlloc<RowSegment>(m_num_segments)))
      return true;

    return false;
  }
  uint num_segments() const { return m_num_segments; }
  RowSegment *segment(uint seg) const { return &m_segments[seg]; }
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
RowSegmentCodec *CreateRowSegmentCodec(MEM_ROOT *mem_root, TABLE *table);
RowSegmentCodec *CreateRowSegmentCodec(MEM_ROOT *mem_root, Filesort *table);

}  // namespace comm
}  // namespace pq
#endif
