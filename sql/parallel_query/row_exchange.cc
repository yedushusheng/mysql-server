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
    queue->SetEvent(&m_message_queue_event);
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

RowExchange::Result RowExchangeReader::Read(THD *thd, uchar **buf) {
  assert(m_left_queues > 0);
  uint visited = 0;
  for (;;) {
    if (thd->killed) return Result::KILLED;
    size_t nbytes;
    void *data;
    auto *handler = m_message_queue_handles[m_next_queue];
    auto result = handler->Receive(&nbytes, &data, true);

    if (likely(result == MessageQueue::Result::SUCCESS)) {
      *buf = (uchar *)data;
      return Result::SUCCESS;
    }

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

  return Result::END;
}

RowExchange::Result RowExchangeWriter::Write(uchar *data, size_t nbytes) {
  assert(m_row_exchange->NumQueues() == 1);
  uint queue = 0;
  if (IsQueueClosed(queue)) return Result::SUCCESS;
  auto result = m_message_queue_handles[queue]->Send(nbytes, data, false);
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

bool RowExchangeMergeSortReader::Init(THD *thd, MY_BITMAP *closed_queues) {
  if (RowExchangeContainer::Init(thd, closed_queues)) return true;

  if (m_mergesort.Init(thd, m_filesort, m_row_exchange->NumQueues()) ||
      m_mergesort.Populate(thd))
    return true;

  return false;
}

MergeSort::Result RowExchangeMergeSortReader::ReadFromChannel(uint index,
                                                              size_t *nbytes,
                                                              void **data,
                                                              bool no_wait) {
  auto result = m_message_queue_handles[index]->Receive(nbytes, data, no_wait);
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

RowExchange::Result RowExchangeMergeSortReader::Read(THD *, uchar **buf) {
  auto result = m_mergesort.Read(buf);
  switch (result) {
    case MergeSort::Result::SUCCESS:
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

}  // namespace pq