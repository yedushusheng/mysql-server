#ifndef PARALLEL_QUERY_COMM_TYPES_H
#define PARALLEL_QUERY_COMM_TYPES_H
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
class THD;
namespace pq {
namespace comm {
// Don't greater that UINT32_MAX because we use my_round_up_to_next_power()
// to determine allocation size, its parameter is uint32. This is the
// maximum length of LONGBLOB and LONGTEXT
constexpr std::size_t MaxMessageSize = UINT32_MAX;

enum class RowTxResult { SUCCESS, DETACHED, WOULD_BLOCK, ERROR};

class Event {
 public:
  Event() {
    mysql_mutex_init(PSI_INSTRUMENT_ME, &m_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(PSI_INSTRUMENT_ME, &m_cond);
  }
  Event(const Event &) = delete;
  ~Event() {
    mysql_mutex_destroy(&m_mutex);
    mysql_cond_destroy(&m_cond);
  }

  inline void Set() {
    mysql_mutex_lock(&m_mutex);
    if (!m_set) {
      m_set = true;
      mysql_cond_signal(&m_cond);
    }
    mysql_mutex_unlock(&m_mutex);
  }
  inline void Reset() {
    mysql_mutex_lock(&m_mutex);
    if (m_set) m_set = false;
    mysql_mutex_unlock(&m_mutex);
  }

  // Note: If the param thd is nullptr, then kill session will not be
  // able to wake up the waiting session and exit Wait. Therefore, it
  // should only be used when the waiting time is relatively short.
  void Wait(THD *thd, bool auto_reset = true);

 private:
  bool m_set{false};
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
};

/**
  For row data that are larger or happen to wrap, we reassemble the message
  locally by copying the chunks into a local buffer. buf is the buffer,
  and buflen is the number of bytes allocated for it.
*/
struct MessageBuffer {
  uchar *buf{nullptr};
  std::size_t buflen{0};

  bool reserve(std::size_t len);
  ~MessageBuffer();
};

}  // namespace comm
}  // namespace pq
#endif
