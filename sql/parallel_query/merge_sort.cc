#include "sql/parallel_query/merge_sort.h"

#include "sql/cmp_varlen_keys.h"
#include "sql/filesort.h"
#include "sql/item.h"
#include "sql/parallel_query/row_segment.h"
#include "sql/sql_class.h"
#include "sql/table.h"

namespace pq {

constexpr uint wider_sortkey_threshold{255};
constexpr uint max_records_buffered{10};

static bool key_is_greater_than(Sort_param *param, uchar *key1, uchar *key2,
                                size_t len) {
  if (param->using_varlen_keys())
    return cmp_varlen_keys(param->local_sortorder, param->use_hash, key2, key1);
  else
    // memcmp(s1, s2, 0) is guaranteed to return zero.
    return memcmp(key1, key2, len) > 0;
}

bool MergeSortElementGreater::operator()(const MergeSortElement *e1,
                                         const MergeSortElement *e2) const {
  return key_is_greater_than(m_param, e1->m_key, e2->m_key, m_compare_length);
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

  if (!(m_records_buffer = static_cast<uchar *>(
            thd->mem_root->Alloc(m_record_length * m_allocated_records))))
    return true;
  if (!(m_row_data = comm::AllocRowDataInfoArray(
            thd->mem_root, m_allocated_records, row_segments)))
    return true;

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
                                      m_key_length + 1, MYF(MY_WME))))
      return true;
  }

  return false;
}

bool MergeSort::Init(THD *thd, Filesort *filesort, uint nelements) {
  assert(filesort->tables.size() == 1);
  m_sort_param = &filesort->m_sort_param;
  m_table = filesort->tables[0];

  if (!(m_elements = thd->mem_root->ArrayAlloc<MergeSortElement>(
            nelements, m_table->s->reclength, max_records_buffered)))
    return true;
  m_num_elements = nelements;

  uint sortlen = filesort->sort_order_length();

  m_sort_param->set_addon_fields_using_merge_sort();
  m_sort_param->init_for_filesort(
      filesort, make_array(filesort->sortorder, sortlen),
      sortlength(thd, filesort->sortorder, sortlen), {m_table}, filesort->limit,
      filesort->m_remove_duplicates);
  // No need to carry addon_fields for merge sort because we buffered records
  assert(m_sort_param->addon_fields == nullptr);

  uint max_sortkey_length = m_sort_param->max_compare_length();

  if (!(m_priority_queue = new (thd->mem_root) PriorityQueue(
            MergeSortElementGreater(max_sortkey_length, m_sort_param),
            Malloc_allocator<MergeSortElement *>(PSI_NOT_INSTRUMENTED))))
    return true;

  if (m_priority_queue->reserve(nelements)) return true;

  // We need to avoid allocating too large buffers for ordering by variable
  // length columns, including lob. Instead, we start from a rational beginning
  // like tiny lob. Hopefully 255 bytes can cover majority cases. If not, the
  // buffer will be extended as needed.
  size_t start_size = max_sortkey_length;
  if (m_sort_param->using_varlen_keys() &&
      max_sortkey_length > wider_sortkey_threshold)
    start_size = wider_sortkey_threshold;

  for (size_t i = 0; i < nelements; i++) {
    if (m_elements[i].Init(i, thd, start_size, m_table->s->blob_fields + 1))
      return true;
  }

  return false;
}

uchar *MergeSortElement::CurrentRecord() const {
  assert(m_cur_read < m_cur_write &&
         m_cur_write <= m_cur_read + (std::uint64_t)m_allocated_records);

  auto cur_rec = m_cur_read % (std::uint64_t)m_allocated_records;
  return m_records_buffer + cur_rec * m_record_length;
}

MergeSort::Result MergeSortElement::PushRecord(MergeSortSource *source,
                                               bool nowait) {
  assert(m_cur_read <= m_cur_write &&
         m_cur_write < m_cur_read + (std::uint64_t)m_allocated_records);

  auto cur_rec = m_cur_write % (std::uint64_t)m_allocated_records;
  uchar *offset = m_records_buffer + cur_rec * m_record_length;
  auto result =
      source->ReadFromChannel(m_chn_index, offset, m_record_length, nowait,
                              RowDataInfoAt(m_row_data, cur_rec));

  if (result != MergeSort::Result::SUCCESS) return result;

  ++m_cur_write;

  return result;
}

/// Each MergeSortElement buffers several records, in order to make
/// PriorityQueue use the buffer directly, This class repoints the ptr of
/// all sort fields. See parallel planner the sort fields of merge sort must
/// be item field.
class RepointSortFields {
 public:
  RepointSortFields(Sort_param *sort_param, ptrdiff_t offset)
      : m_sort_param(sort_param), m_offset(offset) {
    repoint_fields(m_offset);
  }
  ~RepointSortFields() { repoint_fields(-m_offset); }

 private:
  inline void repoint_fields(ptrdiff_t offset) {
    for (const auto &sf : m_sort_param->local_sortorder) {
      auto *field = down_cast<Item_field *>(sf.item)->field;
      field->move_field_offset(offset);
    }
  }
  Sort_param *m_sort_param;
  ptrdiff_t m_offset;
};

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
  Sort_param *sort_param = merge_sort->m_sort_param;
  uchar *rec = elem->CurrentRecord();

  // repoint tmp table's fields to refer to current record in element buffer
  RepointSortFields repoint_sortfields(sort_param, rec - table->record[0]);
  if (elem->alloc_and_make_sortkey(sort_param, table)) return true;
  PushPriorityQueue<Push>(merge_sort->m_priority_queue, elem);

  return false;
}

bool MergeSort::Populate(THD *thd) {
  uint nleft = m_num_elements;
  bool has_row_filled = false;
  while (nleft > 0) {
    for (uint i = 0; i < m_num_elements; i++) {
      MergeSortElement *elem = &m_elements[i];
      // skip non-empty elements or finished elements
      if (!elem->IsEmpty() || m_source->IsChannelFinished(i)) continue;

      if (FillElementBuffer(elem, false, &has_row_filled) != Result::SUCCESS)
        return true;

      if (elem->IsEmpty()) {
        if (m_source->IsChannelFinished(i)) nleft--;
        continue;
      }
      if (FillToPriorityQueue<true>(this, elem)) return true;

      nleft--;
    }
    // Since all channels share same Event for reading and we read blob data
    // with block mode, one channel reading could overwrites other channels'
    // event set. Here we just make sure no data read from any channels
    // before waiting Event. see also ReadOneRow() in row_exchange.cc.
    if (nleft > 0 && !has_row_filled) m_source->Wait(thd);

    if (thd->killed) return true;
  }

  return false;
}

MergeSort::Result MergeSort::FillElementBuffer(MergeSortElement *elem,
                                               bool block_for_first,
                                               bool *has_row_filled) {
  bool no_wait = !block_for_first;

  assert(elem->IsEmpty());

  // Issue waiting read for the first record if @param block_for_first is set,
  // and nowait read for latter ones. Fill as many as max_records_buffered - 1
  // rows, "logically" proceding the header record, which is to be returned.
  while (!elem->IsFull()) {
    auto result = elem->PushRecord(m_source, no_wait);
    // return RowExchangeResult::SUCCESS RowExchangeResult::WOULDBLOCK case.
    if (result == Result::NODATA) return Result::SUCCESS;

    if (result != Result::SUCCESS) return result;

    if (has_row_filled && !*has_row_filled) *has_row_filled = true;
    if (!no_wait) no_wait = true;
  }

  return Result::SUCCESS;
}

void MergeSort::FreeLastKeySeen() { my_free(m_last_key_seen); }

bool MergeSort::SaveLastKeySeen(MergeSortElement *element) {
  uint row_length, key_length, payload_length;
  m_sort_param->get_rec_and_res_len(element->CurrentKey(), &row_length,
                                    &payload_length);
  key_length = row_length - payload_length;

  if (key_length > m_last_key_seen_length) {
    uchar *key;
    if (!(key = (uchar *)my_realloc(PSI_NOT_INSTRUMENTED, m_last_key_seen,
                                    key_length, MYF(MY_WME))))
      return true;
    m_last_key_seen_length = key_length;
    m_last_key_seen = key;
  }

  memcpy(m_last_key_seen, element->CurrentKey(), key_length);
  m_last_key_seen_element = element;
  return false;
}

MergeSort::Result MergeSort::Read(uchar **buf) {
  MergeSort::Result res;
  bool duplicated_with_last;

  do {
    duplicated_with_last = false;
    res =
        ReadOneRow(buf, m_sort_param->m_remove_duplicates ? &duplicated_with_last
                                                         : nullptr);
  } while (res == Result::SUCCESS && duplicated_with_last);

  return res;
}

MergeSort::Result MergeSort::ReadOneRow(uchar **buf, bool *duplicated_with_last) {
  // Return end if nothing is pushed in Populate().
  if (m_priority_queue->size() == 0) return Result::END;

  // Fetch top record from PQ
  MergeSortElement *elem = m_priority_queue->top();
  uint index = elem->ChannelIndex();

  if (elem->IsEmpty()) {
    // No record left, let's fill up this element from source channel
    if (!m_source->IsChannelFinished(index)) {
      Result result;
      if ((result = FillElementBuffer(elem, true, nullptr)) != Result::SUCCESS)
        return result;
    }
    if (!elem->IsEmpty()) {
      if (FillToPriorityQueue<false>(this, elem)) return Result::ERROR;
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
    assert(!elem->IsEmpty());
  }

  *buf = elem->CurrentRecord();

  if (duplicated_with_last) {
    // Copy length of duplicates removal, please see merge_buffers() in
    // filesort.cc. Cache every key except one key that equals to cached key
    // and comes from a different channel.
    if (m_last_key_seen_element != elem && m_last_key_seen &&
        !key_is_greater_than(m_sort_param, elem->CurrentKey(), m_last_key_seen,
                             m_last_key_seen_length))
      *duplicated_with_last = true;
    else if (SaveLastKeySeen(elem))
      return Result::ERROR;
  }

  // One record is returned, advance read cursor and decrease num_record, fill
  // PQ for next read.
  if (elem->PopRecord() && FillToPriorityQueue<false>(this, elem))
    return Result::ERROR;

  return Result::SUCCESS;
}

MergeSort::~MergeSort() {
  destroy(m_priority_queue);
  destroy_array(m_elements, m_num_elements);
  FreeLastKeySeen();
}
}  // namespace pq
