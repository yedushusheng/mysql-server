#include "sql/parallel_query/row_exchange.h"
#include "sql/sql_class.h"

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

bool RowExchangeContainer::Init(THD *thd) {
  uint queues = m_row_exchange->NumQueues();

  if (!(m_message_queue_handles =
            new (thd->mem_root) MessageQueueHandle *[queues]))
    return true;

  for (uint i = 0; i < queues; i++) {
    if (!(m_message_queue_handles[i] = new (thd->mem_root)
              MemMessageQueueHandle(m_row_exchange->Queue(i), thd)))
      return true;
  }
  return false;
}

RowExchangeResult RowExchangeReader::Read(THD *thd, uchar **buf, uint &detached) {
  DBUG_TRACE;
  if (m_left_queues == 0) return RowExchangeResult::END;
  uint visited = 0;
  uint queues = m_row_exchange->NumQueues();
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
      detached = m_next_queue;
      CloseQueue(m_next_queue);
      m_left_queues--;

      if (m_left_queues > 0) {
        do {
          if (++m_next_queue >= queues) m_next_queue = 0;
        } while (IsQueueClosed(m_next_queue));
      }

      return RowExchangeResult::DETACHED;
    }
    // Could be OOM or END
    if (result != RowExchangeResult::WOULD_BLOCK) return result;

    /*
     * Advance nextreader pointer in round-robin fashion.  Note that we
     * only reach this code if we weren't able to get a tuple from the
     * current worker.
     */
    do {
      if (++m_next_queue >= queues) m_next_queue = 0;
    } while (IsQueueClosed(m_next_queue));

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

}
