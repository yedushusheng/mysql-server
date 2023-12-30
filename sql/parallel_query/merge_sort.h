#ifndef PARALLEL_QUERY_MERGE_SORT_H
#define PARALLEL_QUERY_MERGE_SORT_H

#include "priority_queue.h"
#include "sql/cmp_varlen_keys.h"
#include "sql/sort_param.h"

class THD;
namespace pq {
class MergeSortElement {
 public:
  MergeSortElement() = default;
  ~MergeSortElement() { my_free(m_key); }
  bool Init(size_t index, THD *thd, size_t buf_size, size_t key_size);
  uint ChannelIndex() const { return m_chn_index; }

  /**
     Helper function to allocate buffer and build sort key
   */
  bool alloc_and_make_sortkey(Sort_param *param, TABLE *table);

  uint NumRecords() const { return m_num_records; }
  uchar *CurrentRecord() const { return m_curr_record; }
  /// Pop a record for next read. The top record is ready to read. @return false
  /// if no record left.
  bool PopRecord() {
    if (--m_num_records == 0) {
      m_curr_record = m_record_buffer;
      return false;
    }
    m_curr_record += m_record_length;
    return true;
  }

  /// Push a new record and this is just called when the buffer is emptied.
  void PushRecord(void *rec) {
    uchar *offset = m_record_buffer + m_num_records * m_record_length;
    memcpy(offset, rec, m_record_length);
    m_num_records++;
  }

 private:
  uint m_chn_index;  // index of source channel

  uchar *m_key;         // pointer to sort key
  size_t m_key_length;  // The length of key ever stored in this element
  friend struct Mem_compare_queue_key;

  // The record buffer related properties
  /// Records buffer
  uchar *m_record_buffer;
  uint m_num_records;  // The number of records in buffer
  uchar *m_curr_record; // Current read record
   /// The the length of record, it equals to source table's table->s->reclength
  ulong m_record_length;
};

struct Mem_compare_queue_key {
  Mem_compare_queue_key(size_t compare_length, Sort_param *sort_param)
      : m_compare_length(compare_length), m_param(sort_param) {}

  bool operator()(const MergeSortElement *e1,
                  const MergeSortElement *e2) const {
    if (m_param->using_varlen_keys())
      return cmp_varlen_keys(m_param->local_sortorder, m_param->use_hash,
                             e2->m_key, e1->m_key);
    else
      // memcmp(s1, s2, 0) is guaranteed to return zero.
      return memcmp(e2->m_key, e1->m_key, e2->m_key_length) < 0;
  }

  size_t m_compare_length;
  Sort_param *m_param;
};

class MergeSortSource;
/// Using priority queue do a merge sort which reads data from multiple channels
/// of data source (described as a MergeSortSource). We saved maximum
/// MAX_RECORDS_BUFFERED records in MergeSortElement buffer.
class MergeSort {
 public:
  using PriorityQueue = Priority_queue<
      MergeSortElement *,
      std::vector<MergeSortElement *, Malloc_allocator<MergeSortElement *>>,
      Mem_compare_queue_key>;

  enum class Result { SUCCESS, OOM, KILLED, NODATA, END, ERROR };
  MergeSort(MergeSortSource *source) : m_source(source) {}
  ~MergeSort();
  bool Init(THD *thd, Filesort *filesort, uint nelements);
  /// For first time fetch, populate PQ with one record from each channel.
  bool Populate(THD *thd);
  /// Read one row of sorted data
  Result Read(uchar **buf);

 private:
  /**
    Fill element buffer with records from corresponding channel. This function
    should be called when the element buffer is emptied
  */
  Result FillElementBuffer(MergeSortElement *elem, bool block_for_first);

  MergeSortSource *m_source;
  TABLE *m_table;
  Sort_param m_sort_param;
  MergeSortElement *m_elements;
  uint m_num_elements{0};

  PriorityQueue *m_priority_queue{nullptr};

  template <bool Push>
  friend bool FillToPriorityQueue(MergeSort *, MergeSortElement *);
};

/// An abstract class of merge sort data source
class MergeSortSource {
 public:
  /// Return true if the channel #index has been drained out.
  virtual bool IsChannelFinished(uint index) = 0;
  /// We looped over all channels, call this to wait if there is no data
  /// temporarily.
  virtual void Wait(THD *thd) = 0;
  /// Read data from channel @param index, returned @param data and size into
  /// @param bytes, can do a block read if @param no_wait is true.
  virtual MergeSort::Result ReadFromChannel(uint index, size_t *nbytes, void **data,
                                            bool no_wait) = 0;
};

}  // namespace pq
#endif  // PARALLEL_QUERY_MERGE_SORT_H
