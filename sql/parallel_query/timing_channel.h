#ifndef PARALLEL_QUERY_TIMING_CHANNEL_H
#define PARALLEL_QUERY_TIMING_CHANNEL_H
#include <chrono>
#include <string>

#include "sql/parallel_query/row_channel.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
class MEM_ROOT;

namespace pq {
namespace comm {
template <class RealRowChannel>
class TimingRowChannel : public RowChannel {
 public:
  template <class... Args>
  TimingRowChannel(Args &&... args)
      : m_row_channel(std::forward<Args>(args)...) {}

  virtual bool Init(THD *thd, Event *event, bool receiver) override {
    ++m_num_init_calls;
    steady_clock::time_point start = now();
    bool err = m_row_channel.Init(thd, event, receiver);
    steady_clock::time_point end = now();
    m_time_spent_in_first_row += end - start;
    m_first_row = true;

    return err;
  }
  virtual Result Send(std::size_t nbytes, const void *data, bool nowait) override {
    return m_row_channel.Send(nbytes, data, nowait);
  }
  virtual Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                         MessageBuffer *buf) override {
    steady_clock::time_point start = now();
    Result err = m_row_channel.Receive(nbytesp, datap, nowait, buf);
    steady_clock::time_point end = now();
    if (m_first_row) {
      m_time_spent_in_first_row += end - start;
      m_first_row = false;
    } else {
      m_time_spent_in_other_rows += end - start;
    }
    if (err == Result::SUCCESS) {
      ++m_num_rows;
    }

    return err;
  }
  virtual Result SendEOF() override { return m_row_channel.SendEOF(); }

  // Call this function to send EOF after all rows are sent out.
  virtual void Close() override { return m_row_channel.Close(); }
  virtual bool IsClosed() const override { return m_row_channel.IsClosed(); }

  std::string TimingString() const override;

  RowChannel *real_row_channel() { return &m_row_channel; }
  const RowChannel *real_row_channel() const { return &m_row_channel; }

 private:
  // To avoid a lot of repetitive writing.
  using steady_clock = std::chrono::steady_clock;
  template <class T>
  using duration = std::chrono::duration<T>;

  steady_clock::time_point now() const {
#ifdef __SUNPRO_CC
    // This no-op cast works around an optimization bug in Developer Studio
    // where it attempts to dereference an integral time value, leading to
    // crashes.
    return std::chrono::time_point_cast<std::chrono::nanoseconds>(
        steady_clock::now());
#elif defined(__linux__)
    // Work around very slow libstdc++ implementations of std::chrono
    // (those compiled with _GLIBCXX_USE_CLOCK_GETTIME_SYSCALL).
    timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return steady_clock::time_point(
        steady_clock::duration(std::chrono::seconds(tp.tv_sec) +
                               std::chrono::nanoseconds(tp.tv_nsec)));
#else
    return steady_clock::now();
#endif
  }

  // These are at the same offset for all TimingIterator specializations,
  // in the hope that the linker can manage to fold all the TimingString()
  // implementations into one.
  uint64_t m_num_rows = 0;
  uint64_t m_num_init_calls = 0;
  steady_clock::time_point::duration m_time_spent_in_first_row{0};
  steady_clock::time_point::duration m_time_spent_in_other_rows{0};
  bool m_first_row;

  RealRowChannel m_row_channel;
};

/*
  The function NewRowChannel is used to create a TimingRowChannel which
  encapsulates the real RowChannel and offers the runtime statistics of the
  RowChannel.
*/
template <class RealRowChannel, class... Args>
RowChannel *NewRowChannel(THD *thd, Args &&... args) {
  if (thd->lex->is_explain_analyze) {
    return new (thd->mem_root) pq::comm::TimingRowChannel<RealRowChannel>(
        std::forward<Args>(args)...);
  } else {
    return new (thd->mem_root) RealRowChannel(std::forward<Args>(args)...);
  }
}

template <class RealRowChannel>
std::string comm::TimingRowChannel<RealRowChannel>::TimingString() const {
  double first_row_ms =
      duration<double>(m_time_spent_in_first_row).count() * 1e3;
  double last_row_ms =
      duration<double>(m_time_spent_in_first_row + m_time_spent_in_other_rows)
          .count() *
      1e3;
  char buf[1024];
  const uint64_t num_init_calls = m_num_init_calls;
  const uint64_t num_rows = m_num_rows;
  if (num_init_calls == 0) {
    snprintf(buf, sizeof(buf), "(never executed)");
  } else {
    snprintf(buf, sizeof(buf),
             "(actual time=%.3f..%.3f rows=%lld loops=%" PRIu64 ")",
             first_row_ms / num_init_calls, last_row_ms / num_init_calls,
             llrintf(static_cast<double>(num_rows) / num_init_calls),
             num_init_calls);
  }
  return buf;
}
}  // namespace comm
}  // namespace pq
#endif
