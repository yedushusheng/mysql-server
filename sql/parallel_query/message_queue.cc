#include "message_queue.h"
#include "my_bit.h"
#include "sql/sql_class.h"

namespace pq {
namespace comm {
// For record size is 1k should be a common case.
constexpr std::size_t InitialBufferSize = 1024;

bool MessageQueueEndpointInfo::IsKilled() const { return m_thd->killed; }

MessageQueue::MessageQueue(std::size_t queue_size)
    : m_ring_size(queue_size) {
  m_bytes_written.store(0, std::memory_order_relaxed);
  m_bytes_read.store(0, std::memory_order_relaxed);
  // queue_size must be align size
  assert(ALIGN_SIZE(queue_size) == queue_size);
}

bool MessageQueue::Init(MEM_ROOT *mem_root) {
  if (!(m_buffer = (char *)mem_root->Alloc(m_ring_size))) return true;

  return false;
}

void MessageQueue::IncreaseBytesRead(MessageQueueEndpointInfo *endpoint_info,
                                        std::size_t n) {
  /*
    Separate prior reads of m_buffer from the increment of m_bytes_read which
    follows. This pairs with the full barrier in SendBytes(). We only need a
    read barrier here because the increment of m_bytes_read is actually a read
    followed by a dependent write.
  */
  atomic_thread_fence(std::memory_order_acquire);

  /*
    There's no need to use fetch_add here, because nobody else can be changing
    this value. This method should be cheaper.
  */
  m_bytes_read.store(m_bytes_read.load(std::memory_order_relaxed) + n,
                     std::memory_order_relaxed);
  endpoint_info->NotifyPeer();
}

void MessageQueue::IncreaseBytesWritten(std::size_t n) {
  /*
    Separate prior reads of m_buffer from the write of m_bytes_written which
    we're about to do. Pairs with the read barrier found in receive_bytes.
  */
  atomic_thread_fence(std::memory_order_release);

  /*
    There's no need to use fetch_add here, because nobody else can be changing
    this value. This method avoids taking the bus lock unnecessarily.
  */
  m_bytes_written.store(m_bytes_written.load(std::memory_order_relaxed) + n,
                        std::memory_order_relaxed);
}

/**
  Write bytes into the shared message queue.
*/
MessageQueue::Result MessageQueue::SendBytes(
    MessageQueueEndpointInfo *endpoint_info, std::size_t nbytes,
    const void *data, bool nowait, std::size_t *bytes_written) {
  std::size_t sent = 0;
  std::size_t used;
  std::size_t available;

  while (sent < nbytes) {
    uint64_t rb = m_bytes_read.load(std::memory_order_relaxed);
    uint64_t wb = m_bytes_written.load(std::memory_order_relaxed);
    assert(wb >= rb);

    /* Compute number of ring buffer bytes used and available. */
    used = (std::size_t)(wb - rb);
    assert(used <= m_ring_size);

    available = std::min(m_ring_size - used, nbytes - sent);
    /*
      Bail out if the queue has been detached. Note that we would be in trouble
      if the compiler decided to cache the value of m_detached in a register or
      on the stack across loop iterations. It probably shouldn't do that anyway
      since we'll always return, call an external function that performs a
      system call, or reach a memory barrier at some point later in the loop,
      but just to be sure, insert a compiler barrier here.
    */

    atomic_thread_fence(std::memory_order_consume);
    if (IsDetached()) {
      *bytes_written = sent;
      return Result::DETACHED;
    }

    if (available == 0) {
      endpoint_info->NotifyPeer();

      /* Skip manipulation of our latch if nowait = true. */
      if (nowait) {
        *bytes_written = sent;
        return Result::WOULD_BLOCK;
      }

      endpoint_info->Wait();

      if (unlikely(endpoint_info->IsKilled())) return Result::KILLED;

    } else {
      std::size_t offset = wb % (uint64)m_ring_size;
      std::size_t sendnow = std::min(available, m_ring_size - offset);

      /*
        Write as much data as we can via a single memcpy(). Make sure these
        writes happen after the read of m_bytes_read, above. This barrier pairs
        with the one in inc_bytes_read. (Since we're separating the read of
        m_bytes_read from a subsequent write to m_buffer, we need a full barrier
        here.)
      */
      atomic_thread_fence(std::memory_order_acq_rel);
      memcpy(&m_buffer[offset], static_cast<const char *>(data) + sent,
             sendnow);
      sent += sendnow;

      /*
        Update count of bytes written, with alignment padding. Note that this
        will never actually insert any padding except at the end of a run of
        bytes, because the buffer size is a multiple of ALIGN_SIZE, and each
        read is as well.
      */
      assert(sent == nbytes || sendnow == ALIGN_SIZE(sendnow));
      IncreaseBytesWritten(ALIGN_SIZE(sendnow));

      /*
        For efficiency, we don't notify the reader here. We'll do that only when
        the buffer fills up or after writing an entire message.
      */
    }
  }

  *bytes_written = sent;

  return Result::SUCCESS;
}

/**
  Wait until at least *nbytesp bytes are available to be read from the shared
  message queue, or until the buffer wraps around.
*/
MessageQueue::Result MessageQueue::ReceiveBytes(
    MessageQueueEndpointInfo *endpoint_info, std::size_t bytes_needed,
    bool nowait, std::size_t *nbytesp, void **datap,
    std::size_t *consume_pending) {
  std::size_t used;
  uint64_t written;

  for (;;) {
    std::size_t offset;
    uint64_t read;

    written = m_bytes_written.load(std::memory_order_relaxed);
    read = m_bytes_read.load(std::memory_order_relaxed) + *consume_pending;
    used = (std::size_t)(written - read);

    assert(used <= m_ring_size);
    offset = read % (uint64_t)m_ring_size;

    /* If we have enough data or buffer has wrapped, we're done. */
    if (used >= bytes_needed || offset + used >= m_ring_size) {
      *nbytesp = std::min(used, m_ring_size - offset);
      *datap = &m_buffer[offset];

      /*
        Separate the read of m_bytes_written, above, from caller's attempt to
        read the data itself. Pairs with the barrier in inc_bytes_written.
      */
      atomic_thread_fence(std::memory_order_acquire);
      return Result::SUCCESS;
    }

    /*
      Fall out before waiting if the queue has been detached.

      Note that we don't check for this until *after* considering whether the
      data already available is enough, since the receiver can finish receiving
      a message stored in the buffer even after the sender has detached.
    */
    if (IsDetached()) {
      /*
        If the writer advanced m_bytes_written and then set m_detached, we might
        not have read the final value of m_bytes_written above. Insert a read
        barrier and then check again if m_bytes_written has advanced.
      */
      atomic_thread_fence(std::memory_order_acquire);
      if (written != m_bytes_written.load(std::memory_order_relaxed)) continue;

      return Result::DETACHED;
    }

    /*
      We didn't get enough data to satisfy the request, so mark any data
      previously-consumed as read to make more buffer space.
    */
    if (*consume_pending > 0) {
      IncreaseBytesRead(endpoint_info, *consume_pending);
      *consume_pending = 0;
    }

    /* Skip manipulation of our latch if nowait = true. */
    if (nowait) return Result::WOULD_BLOCK;

    endpoint_info->Wait();

    if (unlikely(endpoint_info->IsKilled())) return Result::KILLED;
  }
}

/**
  Write a message into a shared message queue.
*/
MessageQueue::Result MessageQueueHandle::Send(std::size_t nbytes,
                                                 const void *data,
                                                 bool nowait) {
  MessageQueue::Result res;
  std::size_t bytes_written;

  /*
    Prevent writing messages overwhelming the receiver. Should this return a
    meaningful error message?
  */
  if (nbytes > MaxMessageSize) return Result::OOM;

  while (!m_length_word_complete) {
    assert(m_partial_bytes < sizeof(std::size_t));

    res = m_queue->SendBytes(
        &m_endpoint_info, sizeof(std::size_t) - m_partial_bytes,
        ((char *)&nbytes) + m_partial_bytes, nowait, &bytes_written);

    if (res != Result::SUCCESS && res != Result::WOULD_BLOCK) {
      /* Reset state in case caller tries to send another message. */
      m_partial_bytes = 0;
      m_length_word_complete = false;
      return res;
    }

    m_partial_bytes += bytes_written;

    if (m_partial_bytes >= sizeof(std::size_t)) {
      assert(m_partial_bytes == sizeof(std::size_t));
      m_partial_bytes = 0;
      m_length_word_complete = true;
    }

    if (res != Result::SUCCESS) return res;
  }

  /* Write the actual data bytes into the buffer. */
  assert(m_partial_bytes <= nbytes);
  do {
    res = m_queue->SendBytes(&m_endpoint_info, nbytes - m_partial_bytes,
                           static_cast<const char *>(data) + m_partial_bytes,
                           nowait, &bytes_written);

    if (res != Result::SUCCESS && res != Result::WOULD_BLOCK) {
      /* Reset state in case caller tries to send another message. */
      m_partial_bytes = 0;
      m_length_word_complete = false;
      return res;
    }

    m_partial_bytes += bytes_written;

    if (res != Result::SUCCESS) return res;

  } while (m_partial_bytes < nbytes);

  /*
    After a complete message was sent, accumulate the current sent bytes into
    m_pending_bytes.
  */
  m_pending_bytes += m_partial_bytes + ALIGN_SIZE(sizeof(std::size_t));

  /* Reset for next message. */
  m_partial_bytes = 0;
  m_length_word_complete = false;

  /* If queue has been detached, let caller know. */
  if (m_queue->IsDetached()) return Result::DETACHED;

  /*
    If we've produced an amount of data greater than 1/4th of the ring size,
    notify receiver to wakeup and continue to consumer messages if the receiver
    has been blocked. We try to avoid doing this frequently when only a small
    amount of data has been produced, because NotifyPeer() is fairly expensive
    and we don't want to do it too often.
  */
  if (m_pending_bytes > m_queue->Size() / 4) {
    m_endpoint_info.NotifyPeer();
    m_pending_bytes = 0;
  }

  return Result::SUCCESS;
}

/**
  Receive a message from a shared message queue.
*/
MessageQueue::Result MessageQueueHandle::Receive(
    std::size_t *nbytesp, void **datap, bool nowait,
    MessageReassembleBuffer *reassemble_buf) {
  MessageQueue::Result res;
  void *rawdata;
  std::size_t nbytes;
  std::size_t rb = 0;

  /*
    If we've consumed an amount of data greater than 1/4th of the ring size,
    mark it consumed in shared memory. We try to avoid doing this unnecessarily
    when only a small amount of data has been consumed, because NotifyPeer() is
    fairly expensive and we don't want to do it too often.
  */
  if (m_pending_bytes > m_queue->Size() / 4) {
    m_queue->IncreaseBytesRead(&m_endpoint_info, m_pending_bytes);
    m_pending_bytes = 0;
  }

  /* Try to read, or finish reading, the length word from the buffer. */
  while (!m_length_word_complete) {
    assert(m_partial_bytes < sizeof(std::size_t));
    res = m_queue->ReceiveBytes(&m_endpoint_info,
                              sizeof(std::size_t) - m_partial_bytes, nowait,
                              &rb, &rawdata, &m_pending_bytes);
    if (res != Result::SUCCESS) return res;

    if (m_partial_bytes == 0 && rb >= sizeof(std::size_t)) {
      std::size_t needed;

      memcpy(&nbytes, rawdata, sizeof(std::size_t));

      /* If we've already got the whole message, we're done. */
      needed = ALIGN_SIZE(sizeof(std::size_t)) + ALIGN_SIZE(nbytes);
      if (rb >= needed) {
        m_pending_bytes += needed;
        *nbytesp = nbytes;
        *datap = (char *)rawdata + ALIGN_SIZE(sizeof(std::size_t));

        return Result::SUCCESS;
      }

      /*
        We don't have the whole message, but we at least have the whole length
        word.
      */
      m_expected_bytes = nbytes;
      m_length_word_complete = true;
      m_pending_bytes += ALIGN_SIZE(sizeof(std::size_t));
      rb -= ALIGN_SIZE(sizeof(std::size_t));
    } else {
      std::size_t lengthbytes;

      if (unlikely(reassemble_buf->reserve(InitialBufferSize)))
        return Result::OOM;

      assert(reassemble_buf->buflen >= sizeof(std::size_t));

      /* Copy partial length word; remember to consume it. */
      if (m_partial_bytes + rb > sizeof(std::size_t))
        lengthbytes = sizeof(std::size_t) - m_partial_bytes;
      else
        lengthbytes = rb;
      memcpy(&reassemble_buf->buf[m_partial_bytes], rawdata, lengthbytes);
      m_partial_bytes += lengthbytes;
      m_pending_bytes += ALIGN_SIZE(lengthbytes);
      rb -= lengthbytes;

      if (m_partial_bytes >= sizeof(std::size_t)) {
        assert(m_partial_bytes == sizeof(std::size_t));
        m_expected_bytes = *(std::size_t *)reassemble_buf->buf;
        m_length_word_complete = true;
        m_partial_bytes = 0;
      }
    }
  }
  nbytes = m_expected_bytes;

  /*
   Should be disallowed on the sending side already, but better check and
   error out on the receiver side as well rather than trying to read a
   prohibitively large message. Should this return a meaningful error
   message?
  */
  if (nbytes > MaxMessageSize) return Result::OOM;

  /* m_partial_bytes could be non-zero if nowait is true */
  if (m_partial_bytes == 0) {
    /*
      Try to obtain the whole message in a single chunk. If this works, we need
      not copy the data and can return a pointer directly into shared memory.
    */
    res = m_queue->ReceiveBytes(&m_endpoint_info, nbytes, nowait, &rb, &rawdata,
                              &m_pending_bytes);
    if (res != Result::SUCCESS) return res;

    if (rb >= nbytes) {
      m_length_word_complete = false;
      m_pending_bytes += ALIGN_SIZE(nbytes);
      *nbytesp = nbytes;
      *datap = rawdata;

      return Result::SUCCESS;
    }

    /*
      The message has wrapped the buffer. We'll need to copy it in order to
      return it to the client in one chunk. First, make sure we have a large
      enough buffer available.
    */

    if (reassemble_buf->buflen < nbytes) {
      std::size_t newbuflen = my_round_up_to_next_power(nbytes);
      assert(newbuflen >= nbytes);  // Avoid overflow.
      newbuflen = std::min(newbuflen, MaxMessageSize);
      if (unlikely(reassemble_buf->reserve(newbuflen))) return Result::OOM;
    }
  }

  /* Loop until we've copied the entire message. */
  for (;;) {
    std::size_t still_needed;

    /* Copy as much as we can. */
    assert(m_partial_bytes + rb <= nbytes);
    memcpy(&reassemble_buf->buf[m_partial_bytes], rawdata, rb);
    m_partial_bytes += rb;

    /*
      Update count of bytes that can be consumed, accounting for alignment
      padding. Note that this will never actually insert any padding except at
      the end of a message, because the buffer size is a multiple of ALIGN_SIZE,
      and each read and write is as well.
    */
    m_pending_bytes += ALIGN_SIZE(rb);

    /* If we got all the data, exit the loop. */
    if (m_partial_bytes >= nbytes) break;

    /* Wait for some more data */
    still_needed = nbytes - m_partial_bytes;
    res = m_queue->ReceiveBytes(&m_endpoint_info, still_needed, nowait, &rb,
                              &rawdata, &m_pending_bytes);
    if (res != Result::SUCCESS) return res;
    if (rb > still_needed) rb = still_needed;
  }

  /* Return the complete message, and reset for next message. */
  *nbytesp = nbytes;
  *datap = reassemble_buf->buf;
  m_length_word_complete = false;
  m_partial_bytes = 0;
  return Result::SUCCESS;
}
}  // namespace comm
}  // namespace pq
