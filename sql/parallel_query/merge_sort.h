#ifndef PARALLEL_QUERY_MERGE_SORT_H
#define PARALLEL_QUERY_MERGE_SORT_H

#include "priority_queue.h"
#include "sql/cmp_varlen_keys.h"
#include "sql/sort_param.h"

class THD;
namespace pq {
namespace comm {
struct RowDataInfo;
}
class MergeSortSource;
class MergeSortElement;

struct Mem_compare_queue_key {
  Mem_compare_queue_key(size_t compare_length, Sort_param *sort_param)
      : m_compare_length(compare_length), m_param(sort_param) {}

  bool operator()(const MergeSortElement *e1,
                  const MergeSortElement *e2) const;

  size_t m_compare_length;
  Sort_param *m_param;
};

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
  Result Read(uchar **buf, comm::RowDataInfo *&rowdata);

 private:
  /**
    Fill element buffer with records from corresponding channel. This function
    should be called when the element buffer is emptied
  */
  Result FillElementBuffer(MergeSortElement *elem,
                           bool block_for_first);

  MergeSortSource *m_source;
  TABLE *m_table;
  Sort_param m_sort_param;
  MergeSortElement *m_elements;
  uint m_num_elements{0};
  PriorityQueue *m_priority_queue{nullptr};

  template <bool Push>
  friend bool FillToPriorityQueue(MergeSort *, MergeSortElement *);
};

class MergeSortElement {
 public:
  MergeSortElement(ulong record_length, uint allocated_records)
      : m_record_length(record_length),
        m_allocated_records(allocated_records) {}
  ~MergeSortElement();
  bool Init(size_t index, THD *thd, size_t key_size, uint row_segments);
  uint ChannelIndex() const { return m_chn_index; }

  /**
     Helper function to allocate buffer and build sort key
   */
  bool alloc_and_make_sortkey(Sort_param *param, TABLE *table);

  bool IsEmpty() const {
    assert(m_cur_read <= m_cur_write &&
           m_cur_write <= m_cur_read + (std::uint64_t)m_allocated_records);

    return m_cur_read == m_cur_write;
  }
  bool IsFull() const {
    assert(m_cur_read <= m_cur_write);

    auto num_recs = m_cur_write - m_cur_read;
    assert(num_recs <= (std::uint64_t)m_allocated_records);

    return num_recs == (std::uint64_t)m_allocated_records;
  }
  uchar *CurrentRecord(comm::RowDataInfo **rowdata) const;

  /// Pop a record for next read. The top record is ready to read. @return false
  /// if no record left.
  bool PopRecord() {
    assert(m_cur_read < m_cur_write &&
           m_cur_write <= m_cur_read + (std::uint64_t)m_allocated_records);

    return ++m_cur_read != m_cur_write;
  }

  /// Push a new record and this is just called when the buffer is emptied.
  MergeSort::Result PushRecord(MergeSortSource *source, bool nowait);

 private:
  uint m_chn_index;  // index of source channel

  uchar *m_key{nullptr};  // pointer to sort key
  size_t m_key_length;    // The length of key ever stored in this element
  friend struct Mem_compare_queue_key;

  // The record buffer related properties
  /// Records buffer
  uchar *m_records_buffer{nullptr};
  comm::RowDataInfo *m_row_data{nullptr};
  std::uint64_t m_cur_read{0};  // Next read record index
  // Next write record index, the buffer empty if it equals to cur_read.
  std::uint64_t m_cur_write{0};
  /// The the length of record, it equals to source table's table->s->reclength
  ulong m_record_length;
  uint m_allocated_records{0};
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
  virtual MergeSort::Result ReadFromChannel(uint index, uchar *dest,
                                            size_t nbytes, bool no_wait,
                                            comm::RowDataInfo *rowdata) = 0;
};

}  // namespace pq
#endif  // PARALLEL_QUERY_MERGE_SORT_H
