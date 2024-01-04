#include <sys/socket.h>
#include <sys/types.h>
#include <cstring>
#include "my_bit.h"
#include "my_inttypes.h"
#include "sql/parallel_query/comm_types.h"
#include "sql/parallel_query/event_service.h"
#include "sql/parallel_query/row_channel.h"
#include "sql/sql_class.h"

namespace pq {
namespace comm {
class RowTxConnection {
  using Result = RowTxResult;

 public:
  explicit RowTxConnection(int sock) : m_socket(sock) {}
  bool Init(THD *thd, Event *event, bool receiver) {
    m_thd = thd;
    m_event = event;
    return EventServiceAddFd(m_socket, event, receiver);
  }
  static constexpr int SendBufferSize{8192};
  static constexpr int RecvBufferSize{8192};

  Result SendBytes(std::size_t nbytes, const void *data, bool nowait,
                   std::size_t *nbytes_written);
  Result RecvBytes(std::size_t bytes_needed, std::size_t *nbytesp, void **datap,
                   bool nowait);
  Result RecvByte(uchar *byte, bool nowait);
  void Close() {
    if (m_socket == -1) return;
    // TODO: handle flush error here
    flush(false);
    // TODO: handle remove error here
    EventServiceRemoveFd(m_socket);
    close(m_socket);
    m_socket = -1;
  }

  bool IsClosed() const { return m_socket == -1; }

 private:
  Result flush(bool nowait);
  Result RecvBuf(bool nowait);

  int m_socket{-1};
  // Message send or receiver

  // Bytes send or receive and internal buffer operations
  int m_send_cursor{0}; /* Next index to store a byte in m_send_buffer */
  int m_send_start{0};  /* Next index to send a byte in m_send_buffer */
  int m_recv_cursor{0}; /* Next index to read a byte from m_recv_buffer */
  int m_recv_length{0}; /* End of data available in m_recv_buffer */

  int m_send_buffer_size{SendBufferSize};
  int m_recv_buffer_size{RecvBufferSize};
  uchar m_send_buffer[SendBufferSize];
  uchar m_recv_buffer[RecvBufferSize];
  Event *m_event{nullptr};
  THD *m_thd{nullptr};
};

class TcpRowChannel : public RowChannel {
 public:
  explicit TcpRowChannel(int sock) : m_connection(sock) {}
  ~TcpRowChannel() { m_connection.Close(); }
  bool Init(THD *thd, Event *event, bool receiver) override {
    if (m_connection.Init(thd, event, receiver)) return true;
    return false;
  }

  Result Send(std::size_t nbytes, const void *data, bool nowait) override {
    return PutMessage('D', nbytes, data, nowait);
  }

  Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                 MessageBuffer *buf) override {
    uchar msgtype;
    Result res;

    if ((res = GetMessage(&msgtype, nbytesp, datap, nowait, buf)) !=
        Result::SUCCESS)
      return res;

    assert(msgtype == 'X' || msgtype == 'D');

    if (msgtype == 'X') return Result::DETACHED;

    return res;
  }

  Result SendEOF() override {
    assert(!IsClosed());
    return PutMessage('X', 0, nullptr, false);
  }

  void Close() override { m_connection.Close(); }

  bool IsClosed() const override { return m_connection.IsClosed(); }

 private:
  RowTxResult PutMessage(uchar msgtype, std::size_t nbytes, const void *data,
                         bool nowait);
  RowTxResult GetMessage(uchar *msgtype, std::size_t *nbytesp, void **datap,
                         bool nowait, MessageBuffer *buf);

  std::size_t m_send_partial_bytes{0};
  std::size_t m_recv_partial_bytes{0};
  std::size_t m_recv_expected_bytes{0};
  uchar m_recv_last_msgtype{'\0'};

  RowTxConnection m_connection;
};

RowTxConnection::Result RowTxConnection::flush(bool nowait) {
  uchar *bufptr = m_send_buffer + m_send_start;
  uchar *bufend = m_send_buffer + m_send_cursor;

  while (bufptr < bufend) {
    if (m_thd->killed) return Result::ERROR;
    int r = send(m_socket, bufptr, bufend - bufptr, MSG_DONTWAIT);
    if (r <= 0) {
      int error = errno;
      if (error == EINTR) continue; /* Ok if we were interrupted */

      /*
       * Ok if no data writable without blocking, and the socket is in
       * non-blocking mode.
       */
      if (error == EAGAIN) {
        if (nowait) return Result::WOULD_BLOCK;
        m_event->Wait(m_thd);
        continue;
      }

      MyOsError(error, ER_PARALLEL_ROW_CHANNEL_TX_ERROR, MYF(0));
      return Result::ERROR;
    }

    bufptr += r;
    m_send_start += r;
  }

  m_send_start = m_send_cursor = 0;

  return Result::SUCCESS;
}

RowTxConnection::Result RowTxConnection::SendBytes(
    std::size_t nbytes, const void *data, bool nowait,
    std::size_t *nbytes_written) {
  const uchar *send_bytes = static_cast<const uchar *>(data);
  auto len = nbytes;
  while (len > 0) {
    /* If buffer is full, then flush it out */
    if (m_send_cursor >= m_send_buffer_size) {
      Result result;
      // Wait inside of flush if no-wait is not specified.
      if ((result = flush(nowait)) != Result::SUCCESS) {
        assert(result != Result::WOULD_BLOCK || nowait);

        *nbytes_written = nbytes - len;
        return result;
      }
    }

    std::size_t amount = m_send_buffer_size - m_send_cursor;
    if (amount > len) amount = len;
    memcpy(m_send_buffer + m_send_cursor, send_bytes, amount);
    m_send_cursor += amount;
    send_bytes += amount;
    len -= amount;
  }

  *nbytes_written = nbytes - len;
  return Result::SUCCESS;
}

// load some bytes into the input buffer
RowTxConnection::Result RowTxConnection::RecvBuf(bool nowait) {
  if (m_recv_cursor > 0) {
    if (m_recv_length > m_recv_cursor) {
      /* still some unread data, left-justify it in the buffer */
      memmove(m_recv_buffer, m_recv_buffer + m_recv_cursor,
              m_recv_length - m_recv_cursor);
      m_recv_length -= m_recv_cursor;
      m_recv_cursor = 0;
    } else
      m_recv_length = m_recv_cursor = 0;
  }

  /* Can fill buffer from m_recv_length and upwards */
  for (;;) {
    if (m_thd->killed) return Result::ERROR;

    int r = recv(m_socket, m_recv_buffer + m_recv_length,
                 m_recv_buffer_size - m_recv_length, MSG_DONTWAIT);

    // Peer close connection unexpectedly, error should be reported there.
    if (r == 0) return Result::ERROR;
    if (r < 0) {
      int error = errno;
      if (error == EINTR) continue; /* Ok if interrupted */

      if (error == EAGAIN) {
        if (nowait) return Result::WOULD_BLOCK;
        m_event->Wait(m_thd);
        continue;
      }

      MyOsError(error, ER_PARALLEL_ROW_CHANNEL_TX_ERROR, MYF(0));
      return Result::ERROR;
    }

    /* r contains number of bytes read, so just incr length */
    m_recv_length += r;
    return Result::SUCCESS;
  }
}

RowTxConnection::Result RowTxConnection::RecvBytes(std::size_t bytes_needed,
                                                   std::size_t *nbytesp,
                                                   void **datap, bool nowait) {
  while (m_recv_cursor >= m_recv_length) {
    Result res;
    if ((res = RecvBuf(nowait)) != Result::SUCCESS) return res;
  }

  std::size_t amount = m_recv_length - m_recv_cursor;
  if (amount > bytes_needed) amount = bytes_needed;
  *datap = m_recv_buffer + m_recv_cursor;
  *nbytesp = amount;
  m_recv_cursor += amount;

  return Result::SUCCESS;
}

RowTxConnection::Result RowTxConnection::RecvByte(uchar *byte, bool nowait) {
  Result res;
  while (m_recv_cursor >= m_recv_length) {
    /* If nothing in buffer, then recv some */
    if ((res = RecvBuf(nowait)) != Result::SUCCESS) return res;
  }

  *byte = m_recv_buffer[m_recv_cursor++];

  return Result::SUCCESS;
}

TcpRowChannel::Result TcpRowChannel::PutMessage(uchar msgtype,
                                                std::size_t nbytes,
                                                const void *data, bool nowait) {
  constexpr auto typelen = sizeof(msgtype);
  constexpr auto sizelen = sizeof(nbytes);
  constexpr auto headlen = typelen + sizelen;
  auto msglen = nbytes + sizelen;
  auto sendlen = msglen + typelen;
  RowTxResult res;
  std::size_t written;

  // Send 1 byte message type
  while (m_send_partial_bytes < typelen) {
    res = m_connection.SendBytes(typelen, &msgtype, nowait, &written);
    assert(written == typelen || res != Result::SUCCESS);
    m_send_partial_bytes += written;
    if (res != RowTxResult::SUCCESS) return res;
  }

  while (m_send_partial_bytes < headlen) {
    res = m_connection.SendBytes(sizelen - (m_send_partial_bytes - typelen),
                                 &msglen + (m_send_partial_bytes - typelen),
                                 nowait, &written);
    // The data could be partially sent even WOULD_BLOCK returned, so add
    // written bytes first.
    m_send_partial_bytes += written;
    if (res != RowTxResult::SUCCESS) return res;
  }

  while (m_send_partial_bytes < sendlen) {
    res = m_connection.SendBytes(
        nbytes - (m_send_partial_bytes - headlen),
        pointer_cast<const uchar *>(data) + m_send_partial_bytes - headlen,
        nowait, &written);
    m_send_partial_bytes += written;
    if (res != Result::SUCCESS) return res;
  }

  m_send_partial_bytes = 0;  // Reset for next message

  return res;
}

RowTxResult TcpRowChannel::GetMessage(uchar *msgtype, std::size_t *nbytesp,
                                      void **datap, bool nowait,
                                      MessageBuffer *buf) {
  constexpr auto typelen = sizeof(*msgtype);
  constexpr auto sizelen = sizeof(m_recv_expected_bytes);
  constexpr auto headlen = typelen + sizelen;

  std::size_t len;
  Result res;
  void *bytes;
  while (m_recv_partial_bytes < typelen) {
    if ((res = m_connection.RecvByte(&m_recv_last_msgtype, nowait)) !=
        Result::SUCCESS)
      return res;
    m_recv_partial_bytes += typelen;
  }
  while (m_recv_partial_bytes < headlen) {
    if ((res =
             m_connection.RecvBytes(sizelen - (m_recv_partial_bytes - typelen),
                                    &len, &bytes, nowait)) != Result::SUCCESS)
      return res;
    memcpy(pointer_cast<uchar *>(&m_recv_expected_bytes) +
               (m_recv_partial_bytes - typelen),
           bytes, len);

    m_recv_partial_bytes += len;
  }

  assert(m_recv_expected_bytes >= sizelen &&
         m_recv_expected_bytes < MaxMessageSize + headlen);

  // Attempt to receive message body, we can just use receive buffer if
  // complete message body received at first try. Otherwise we should use
  // buf to reassemble message. Note, message body could be empty e.g. EOF
  if (m_recv_partial_bytes < m_recv_expected_bytes + typelen &&
      m_recv_partial_bytes == headlen) {
    auto remain_bytes =
        m_recv_expected_bytes - (m_recv_partial_bytes - typelen);
    if ((res = m_connection.RecvBytes(remain_bytes, &len, &bytes, nowait)) !=
        Result::SUCCESS)
      return res;

    if (remain_bytes == len) {
      *msgtype = m_recv_last_msgtype;
      *nbytesp = remain_bytes;
      *datap = bytes;

      // Reset for next message
      m_recv_partial_bytes = 0;
      m_recv_expected_bytes = 0;
      m_recv_last_msgtype = '\0';
      return Result::SUCCESS;
    }

    auto msgbody_len = m_recv_expected_bytes - sizelen;
    std::size_t newbuflen = my_round_up_to_next_power(msgbody_len);
    assert(newbuflen >= msgbody_len);  // Avoid overflow.
    newbuflen = std::min(newbuflen, MaxMessageSize);
    if (unlikely(buf->reserve(newbuflen))) return Result::ERROR;
    memcpy(buf->buf + m_recv_partial_bytes - headlen, bytes, len);
    m_recv_partial_bytes += len;
  }

  while (m_recv_partial_bytes < m_recv_expected_bytes + typelen) {
    auto remain_bytes =
        m_recv_expected_bytes - (m_recv_partial_bytes - typelen);
    if ((res = m_connection.RecvBytes(remain_bytes, &len, &bytes, nowait)) !=
        Result::SUCCESS)
      return res;

    memcpy(buf->buf + m_recv_partial_bytes - headlen, bytes, len);
    m_recv_partial_bytes += len;
  }

  *msgtype = m_recv_last_msgtype;
  *nbytesp = m_recv_expected_bytes - sizelen;
  *datap = buf->buf;

  // Reset for next message
  m_recv_partial_bytes = 0;
  m_recv_expected_bytes = 0;
  m_recv_last_msgtype = '\0';

  return Result::SUCCESS;
}

RowChannel *CreateTcpRowChannel(MEM_ROOT *mem_root, int *sock) {
  assert(sock);
  int mysock = *sock;
  if (mysock == -1) {
    int spair[2];
    int res = socketpair(AF_UNIX, SOCK_STREAM, 0, spair);
    if (res == -1) return nullptr;
    *sock = spair[1];
    mysock = spair[0];
  }

  return new (mem_root) TcpRowChannel(mysock);
}

}  // namespace comm
}  // namespace pq
