
#include "sql/parallel_query/row_exchange.h"

#include "sql/filesort.h"
#include "sql/sql_class.h"
#include "sql/table.h"

namespace pq {
bool RowExchange::Init(MEM_ROOT *mem_root,
                       std::function<MessageQueue *(uint)> get_queue) {
  if (!(m_message_queues = new (mem_root) MessageQueue *[m_num_queues]))
    return true;

  for (uint i = 0; i < m_num_queues; i++) {
    MessageQueue *queue = get_queue(i);
    assert(queue);
    switch (m_type) {
      case Type::SENDER:
        queue->SetSenderEvent(&m_message_queue_event);
        break;
      case Type::RECEIVER:
        queue->SetReceiverEvent(&m_message_queue_event);
        break;
      default:
        assert(0);
    }
    m_message_queues[i] = queue;
  }

  return false;
}

RowExchangeContainer::~RowExchangeContainer() {
  if (!m_message_queue_handles) return;
  for (uint i = 0; i < m_row_exchange->NumQueues(); i++)
    destroy(m_message_queue_handles[i]);
}

bool RowExchangeContainer::Init(THD *thd, MY_BITMAP *closed_queues) {
  uint queues = m_row_exchange->NumQueues();

  if (!(m_message_queue_handles =
            new (thd->mem_root) MessageQueueHandle *[queues]))
    return true;

  for (uint i = 0; i < queues; i++) {
    if (!(m_message_queue_handles[i] = new (thd->mem_root)
              MemMessageQueueHandle(m_row_exchange->Queue(i), thd)))
      return true;
    if (closed_queues && bitmap_is_set(closed_queues, i)) CloseQueue(i);
  }
  return false;
}


RowExchangeResult RowExchangeReader::Read(THD *thd, uchar **buf) {
  assert(m_left_queues > 0);
  uint visited = 0;
  for (;;) {
    if (thd->killed) return RowExchangeResult::KILLED;
    size_t nbytes;
    void *data;
    auto *handler = m_message_queue_handles[m_next_queue];
    auto result = handler->Receive(&nbytes, &data, true);
    if (likely(result == RowExchangeResult::SUCCESS)) {
      *buf = (uchar *)data;
      return RowExchangeResult::SUCCESS;
    }
    if (result == RowExchangeResult::DETACHED) {
      CloseQueue(m_next_queue);
      m_left_queues--;

      if (unlikely(m_left_queues == 0)) break;

      AdvanceQueue();
      continue;
    }
    // Could be OOM or END
    if (result != RowExchangeResult::WOULD_BLOCK) return result;

    /*
      Advance nextreader pointer in round-robin fashion. Note that we only reach
      this code if we weren't able to get a tuple from the current worker.
     */
    AdvanceQueue();

    visited++;
    if (visited >= m_left_queues) {
      /* Nothing to do except wait for developments. */
      m_row_exchange->Wait(thd);
      visited = 0;
    }
  }
  return RowExchangeResult::END;
}

RowExchangeResult RowExchangeWriter::WriteToQueue(uint queue, uchar *data,
                                                   size_t nbytes, bool nowait) {
  // When the target mq is detached by receiver, some receiver may quit early
  // because the data repartition is tilted.
  // Ignore it and return success directly to avoid query hang.
  if (IsQueueClosed(queue)) return MessageQueueResult::SUCCESS;

  return m_message_queue_handles[queue]->Send(nbytes, data, nowait);
}

RowExchangeResult RowExchangeWriter::Write(uchar *record, size_t nbytes) {
  assert(m_row_exchange->NumQueues() == 1);
  RowExchangeResult res = WriteToQueue(0, record, nbytes, false);
  assert(res != RowExchangeResult::WOULD_BLOCK);
  if (unlikely(res == RowExchangeResult::DETACHED)) {
    CloseQueue(0);
    res = RowExchangeResult::SUCCESS;
  }

  return res;
}

void RowExchangeWriter::WriteEOF() {
  assert(m_row_exchange->NumQueues() == 1);
  m_message_queue_handles[0]->Detach();
}

bool RowExchangeMergeSortReader::Init(THD *thd, MY_BITMAP *closed_queues) {
  if (RowExchangeContainer::Init(thd, closed_queues)) return true;

  if (m_mergesort.Init(thd, m_filesort, m_row_exchange->NumQueues()) ||
      m_mergesort.Populate(thd))
    return true;

  return false;
}

MergeSort::Result RowExchangeMergeSortReader::ReadFromChannel(
    uint index, size_t *nbytes, void **data, bool no_wait) {
  auto result = m_message_queue_handles[index]->Receive(nbytes, data, no_wait);
  switch (result) {
    case RowExchangeResult::SUCCESS:
      break;
    case RowExchangeResult::OOM:
      return MergeSort::Result::OOM;
    case RowExchangeResult::KILLED:
      return MergeSort::Result::KILLED;
    case RowExchangeResult::DETACHED:
      CloseQueue(index);
      return MergeSort::Result::NODATA;
    case RowExchangeResult::WOULD_BLOCK:
      // no wait , return now
      return MergeSort::Result::NODATA;
    case RowExchangeResult::END:
    case RowExchangeResult::NONE:
    case RowExchangeResult::ERROR:
      assert(false);
  }

  assert(result == RowExchangeResult::SUCCESS);

  return MergeSort::Result::SUCCESS;
}

RowExchangeResult RowExchangeMergeSortReader::Read(THD *, uchar **buf) {
  auto result = m_mergesort.Read(buf);
  switch (result) {
    case MergeSort::Result::SUCCESS:
      return RowExchangeResult::SUCCESS;
    case MergeSort::Result::END:
      return RowExchangeResult::END;
    case MergeSort::Result::KILLED:
      return RowExchangeResult::KILLED;
    case MergeSort::Result::OOM:
      return RowExchangeResult::OOM;
    case MergeSort::Result::ERROR:
      return RowExchangeResult::ERROR;
    case MergeSort::Result::NODATA:
      assert(false);
  }
  return RowExchangeResult::SUCCESS;
}

}  // namespace pq