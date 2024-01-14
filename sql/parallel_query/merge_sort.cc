#include "sql/parallel_query/merge_sort.h"

#include "sql/filesort.h"
#include "sql/parallel_query/row_exchange.h"
#include "sql/sql_class.h"
#include "sql/table.h"

namespace pq {

constexpr uint wider_sortkey_threshold{255};
constexpr uint max_records_buffered{10};

bool Mem_compare_queue_key::operator()(const MergeSortElement *e1,
                                       const MergeSortElement *e2) const {
  if (m_param->using_varlen_keys())
    return cmp_varlen_keys(m_param->local_sortorder, m_param->use_hash,
                           e2->m_key, e1->m_key);
  else
    // memcmp(s1, s2, 0) is guaranteed to return zero.
    return memcmp(e2->m_key, e1->m_key, e2->m_key_length) < 0;
}

MergeSortElement::~MergeSortElement() {
  my_free(m_key);
  DestroyRowDataInfoArray(m_row_data, m_allocated_records);
}

bool MergeSortElement::Init(size_t index, THD *thd, size_t key_size,
                            uint row_segments) {
  m_chn_index = index;
  // Building key from variable length field will do alignment to event number
  // of bytes, See make_sortkey_from_field() in filesort.cc. So, we need to
  // make one more byte larger.
  if (!(m_key = (uchar *)my_malloc(PSI_NOT_INSTRUMENTED, key_size + 1,
                                   MYF(MY_WME))))
    return true;
  m_key_length = key_size;

  if (!(m_record_buffer = static_cast<uchar *>(
            thd->mem_root->Alloc(m_record_length * m_allocated_records))))
    return true;
  if (!(m_row_data = AllocRowDataInfoArray(thd->mem_root, m_allocated_records,
                                           row_segments)))
    return true;

  ResetRecordRead();

  return false;
}

/**
  Helper function to allocate buffer and build sort key, see same name function
  in filesort.cc.
*/
bool MergeSortElement::alloc_and_make_sortkey(Sort_param *param, TABLE *table) {
  for (;;) {
    size_t real_key_length =
        param->make_sortkey(m_key, m_key_length + 1, {table});
    // Either fit into or not
    assert(real_key_length == UINT_MAX ||
           real_key_length <= m_key_length + 1);

    // See if the sort key can fit into current buffer, if yes, stop now,
    // otherwise, try an even larger buffer
    if (real_key_length <= m_key_length) break;

    assert(param->using_varlen_keys());

    auto inc_len = m_key_length / 2;
    if (unlikely(inc_len == 0)) inc_len = 1;
    m_key_length = m_key_length + inc_len;

    if (!(m_key = (uchar *)my_realloc(PSI_NOT_INSTRUMENTED, m_key,
                                    m_key_length + 1, MYF(0))))
      return true;
  }

  return false;
}

bool MergeSort::Init(THD *thd, Filesort *filesort, uint nelements) {
  assert(filesort->tables.size() == 1);
  m_table = filesort->tables[0];

  if (!(m_elements = thd->mem_root->ArrayAlloc<MergeSortElement>(
            nelements, m_table->s->reclength, max_records_buffered)))
    return true;
  m_num_elements = nelements;

  uint sortlen = filesort->sort_order_length();

  m_sort_param.m_addon_fields_status = Addon_fields_status::using_by_merge_sort;
  m_sort_param.init_for_filesort(filesort,
                                 make_array(filesort->sortorder, sortlen),
                                 sortlength(thd, filesort->sortorder, sortlen),
                                 {m_table}, filesort->limit, false);
  // No need to carry addon_fields for merge sort because we buffered records
  assert(m_sort_param.addon_fields == nullptr);

  uint max_sortkey_length = m_sort_param.max_compare_length();

  if (!(m_priority_queue = new (thd->mem_root) PriorityQueue(
            Mem_compare_queue_key(max_sortkey_length, &m_sort_param),
            Malloc_allocator<MergeSortElement *>(PSI_NOT_INSTRUMENTED))))
    return true;

  if (m_priority_queue->reserve(nelements)) return true;

  // We need to avoid allocating too large buffers for ordering by variable
  // length columns, including lob. Instead, we start from a rational beginning
  // like tiny lob. Hopefully 255 bytes can cover majority cases. If not, the
  // buffer will be extended as needed.
  size_t start_size = max_sortkey_length;
  if (m_sort_param.using_varlen_keys() &&
      max_sortkey_length > wider_sortkey_threshold)
    start_size = wider_sortkey_threshold;

  for (size_t i = 0; i < nelements; i++) {
    if (m_elements[i].Init(i, thd, start_size, m_table->s->blob_fields + 1))
      return true;
  }

  return false;
}

uchar *MergeSortElement::CurrentRecord(RowDataInfo **rowdata) const {
  if (rowdata) *rowdata = RowDataInfoAt(m_row_data, m_cur_record);

  return m_read_cursor;
}

MergeSort::Result MergeSortElement::PushRecord(MergeSortSource *source,
                                               bool nowait) {
  uchar *offset = m_record_buffer + m_num_records * m_record_length;
  auto result =
      source->ReadFromChannel(m_chn_index, offset, m_record_length, nowait,
                              RowDataInfoAt(m_row_data, m_num_records));

  if (result != MergeSort::Result::SUCCESS) return result;

  m_num_records++;

  return result;
}

/// Define as a template function to do a little optimization.
template <bool Push>
inline void PushPriorityQueue(MergeSort::PriorityQueue *, MergeSortElement *);

template <>
inline void PushPriorityQueue<true>(MergeSort::PriorityQueue *pq,
                                    MergeSortElement *elem) {
  pq->push(elem);
}

template <>
inline void PushPriorityQueue<false>(MergeSort::PriorityQueue *pq,
                                     MergeSortElement *elem) {
  pq->decrease(0, elem);
}

/// Dequeue the header record from @elem buffer, and push the record into the
/// PQ. If the buffer becomes empty, try fill up it from the m_source in a
/// batch.
template <bool Push>
bool FillToPriorityQueue(MergeSort *merge_sort, MergeSortElement *elem) {
  TABLE *table = merge_sort->m_table;
  Sort_param *sort_param = &merge_sort->m_sort_param;
  uchar *rec = elem->CurrentRecord(nullptr);

  // repoint tmp table's fields to refer to current record in element buffer
  // TODO: Could we find actual used fields in make_sortkey(), then just
  // repoint fields that are real used. The used fields should be exactly
  // *sort_param->sortorder? Are they just item fields of collector table?
  repoint_field_to_record(table, table->record[0], rec);

  if (elem->alloc_and_make_sortkey(sort_param, table)) return true;

  PushPriorityQueue<Push>(merge_sort->m_priority_queue, elem);

  repoint_field_to_record(table, rec, table->record[0]);
  return false;
}

bool MergeSort::Populate(THD *thd) {
  uint nleft = m_num_elements;
  while (nleft > 0) {
    for (uint i = 0; i < m_num_elements; i++) {
      MergeSortElement *elem = &m_elements[i];
      // skip full elements or finished elements
      if (elem->NumRecords() > 0 || m_source->IsChannelFinished(i)) continue;

      if (FillElementBuffer(elem, false) != Result::SUCCESS) return true;

      if (elem->NumRecords() == 0) {
        if (m_source->IsChannelFinished(i)) nleft--;
        continue;
      }
      if (FillToPriorityQueue<true>(this, elem)) return true;

      nleft--;
    }
    if (nleft > 0) m_source->Wait(thd);

    if (thd->killed) return true;
  }

  return false;
}

/**
  Fill up buffer of element from specified channel If succeed, the element is
  fully filled
*/
MergeSort::Result MergeSort::FillElementBuffer(MergeSortElement *elem,
                                               bool block_for_first) {
  bool no_wait = !block_for_first;

  assert(elem->NumRecords() == 0);

  // Issue waiting read for the first record if @param block_for_first is set,
  // and nowait read for latter ones. Fill as many as max_records_buffered - 1
  // rows, "logically" proceding the header record, which is to be returned.
  while (elem->NumRecords() < max_records_buffered) {
    auto result = elem->PushRecord(m_source, no_wait);
    // return RowExchangeResult::SUCCESS RowExchangeResult::WOULDBLOCK case.
    if (result == Result::NODATA) return Result::SUCCESS;

    if (result != Result::SUCCESS) return result;

    no_wait = true;
  }

  return Result::SUCCESS;
}

MergeSort::Result MergeSort::Read(uchar **buf, RowDataInfo *&rowdata) {
  // Return end if nothing is pushed in Populate().
  if (m_priority_queue->size() == 0) return Result::END;

  // Fetch top record from PQ
  MergeSortElement *elem = m_priority_queue->top();
  uint index = elem->ChannelIndex();

  if (elem->NumRecords() == 0) {
    // No record left, let's fill up this element from source channel
    if (!m_source->IsChannelFinished(index)) {
      Result result;
      if ((result = FillElementBuffer(elem, true)) != Result::SUCCESS)
        return result;
    }
    if (elem->NumRecords() > 0) {
      if (FillToPriorityQueue<false>(this, elem)) return Result::OOM;
    } else {
      // FillElementBuffer() filled one record at least, current channel must
      // be finished if no record is filled.
      assert(m_source->IsChannelFinished(index));
      m_priority_queue->pop();
      // PQ is empty, it's the end
      if (m_priority_queue->size() == 0) return Result::END;
    }

    // After Priority_queue re-heapify, get new top element. It has one record
    // at least.
    elem = m_priority_queue->top();
    assert(elem->NumRecords() > 0);
  }

  *buf = elem->CurrentRecord(&rowdata);

  // One record is returned, advance read cursor and decrease num_record, fill
  // PQ for next read.
  if (elem->PopRecord() && FillToPriorityQueue<false>(this, elem))
    return Result::OOM;

  return Result::SUCCESS;
}

MergeSort::~MergeSort() {
  destroy(m_priority_queue);
  destroy_array(m_elements, m_num_elements);
}
}  // namespace pq
