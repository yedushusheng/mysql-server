#include "sql/parallel_query/row_segment.h"

#include "sql/field.h"
#include "sql/filesort.h"
#include "sql/table.h"

namespace pq {
namespace comm {
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

RowSegmentCodec *CreateRowSegmentCodec(MEM_ROOT *mem_root, TABLE *table) {
  return new (mem_root) RowSegmentCodec(table);
}

RowSegmentCodec *CreateRowSegmentCodec(MEM_ROOT *mem_root, Filesort *filesort) {
  return CreateRowSegmentCodec(mem_root, filesort->tables[0]);
}
}  // namespace comm
}  // namespace pq
