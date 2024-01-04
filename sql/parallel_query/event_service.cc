#include "sql/parallel_query/event_service.h"

#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <atomic>
#include <deque>
#include "my_systime.h"
#include "mysql/psi/mysql_thread.h"
#include "mysqld_error.h"
#include "sql/mysqld.h"
#include "sql/parallel_query/comm_types.h"

namespace pq {
namespace comm {
#define WAIT_EVENT_COUNT 256

struct EventSlot {
  enum State { UNPINNED, PINNED, FREE };

  EventSlot(Event *e) {
    event = e;
    ref = 1;
    state.store(State::UNPINNED, std::memory_order_relaxed);
  }

  /// Reuse a exist slot
  void reuse(Event *e) {
    assert(state.load(std::memory_order_relaxed) == State::FREE);
    event = e;
    ref = 1;
    state.store(State::UNPINNED, std::memory_order_release);
  }

  /// Detect that a slot is free for reuse, used in leader thread only
  inline bool is_free() const {
    return state.load(std::memory_order_relaxed) == State::FREE;
  }

  /// Set it to free means its event is ready for delete, only UNPINNED
  /// can be change to FREE
  inline void set_free() {
    State cur_state;
    do {
      cur_state = State::UNPINNED;
    } while (!state.compare_exchange_strong(cur_state, State::FREE,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed));
  }

  /// Called by event service thread before actually event set, only UNPINNED
  /// state can be pinned.
  inline bool pin() {
    auto cur_state = State::UNPINNED;
    return state.compare_exchange_strong(cur_state, State::PINNED,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  /// Called by event service thread after event set
  inline void unpin() {
    assert(state == State::PINNED);
    state.store(State::UNPINNED, std::memory_order_release);
  }

  std::atomic<State> state;
  Event *event;

  /// Event can be shared by multiple fds. Set it FREE if ref decreases to
  /// 0.
  uint ref;
};

class EventSession {
 public:
  std::deque<EventSlot> m_events;
  EventSession *next_free{nullptr};
};

class EventService {
 public:
  enum class State { Started, Stopping, Stopped };
  void stop();
  bool setup();
  EventSession *register_session();
  void unregister_session(EventSession *es);
  void free_sessions();
  bool add_fd(int fd, EventSlot *event, bool poll_in);
  bool del_fd(int fd);
  void cleaup();
  void mainloop();

  my_thread_handle thread_handle;

 private:
  static constexpr uint ctrl_magic = 0xdead;
  std::atomic<State> state{State::Stopped};
  std::atomic<EventSession *> free_session_list{nullptr};
  int epoll_fd{-1};
  int control_fd{-1};
  struct epoll_event events[WAIT_EVENT_COUNT];
};

static EventService *event_service = nullptr;

EventSession *EventService::register_session() {
  auto *es = new EventSession;
  return es;
}

EventSession *RegisterEventSession() {
  // Note, TDSQL 3.0 don't need start event service when system is up and it is
  // unsafe that starting event service here.
  if (!start_service_when_system_up && !event_service) StartEventService();

  return event_service->register_session();
}

void UnregisterEventSession(EventSession *es) {
  event_service->unregister_session(es);
}

void EventService::unregister_session(EventSession *es) {
  EventSession *cur_head;
  do {
    cur_head = event_service->free_session_list.load();
    es->next_free = cur_head;
  } while (!free_session_list.compare_exchange_strong(
      cur_head, es, std::memory_order_relaxed));

  uint64_t val = 1;
  write(control_fd, &val, sizeof(val));
}

void EventService::free_sessions() {
  EventSession *cur_head;
  do {
    cur_head = event_service->free_session_list.load();
  } while (!free_session_list.compare_exchange_strong(
      cur_head, nullptr, std::memory_order_relaxed));

  while (cur_head) {
      auto *next = cur_head->next_free;
#ifndef NDEBUG
      // Every fd should be removed from epoll.
      for (auto &eslot : cur_head->m_events) assert(eslot.is_free());
#endif
      delete cur_head;
      cur_head = next;
  }

  // drain out event fd
  uint64_t val;
  while (read(control_fd, &val, sizeof(val)) != -1)
      ;
}

bool EventServiceAddFd(EventSession *es, int fd, Event *event, bool poll_in) {
  EventSlot *slot = nullptr;
  EventSlot *reuse_slot = nullptr;
  for (auto &ev : es->m_events) {
      auto *pev = &ev;
      // Multiple fds can share same Event, found it if it is already in vector.
      if (!ev.is_free() && pev->event == event) {
        slot = pev;
        break;
      }

      // Find first FREE one to reuse later.
      if (!reuse_slot && ev.is_free()) reuse_slot = pev;
  }

  if (slot)
      ++slot->ref;
  else if (reuse_slot) {
      reuse_slot->reuse(event);
      slot = reuse_slot;
  }

  if (!slot) {
      es->m_events.emplace_back(event);
      slot = &es->m_events[es->m_events.size() - 1];
  }

  return event_service->add_fd(fd, slot, poll_in);
}

bool EventService::add_fd(int fd, EventSlot *event, bool poll_in) {
  struct epoll_event ev;
  ev.events = poll_in ? EPOLLIN : EPOLLOUT;
  ev.events |= EPOLLRDHUP;
  ev.events |= EPOLLET;
  ev.data.ptr = event;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == 0) return false;

  auto error = errno;
  MyOsError(error, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
  return true;
}

bool EventServiceRemoveFd(EventSession *es, int fd, Event *event) {
  assert(event_service);
  if (event_service->del_fd(fd)) return true;

  EventSlot *slot = nullptr;
  for (auto &ev : es->m_events) {
      if (ev.event == event) {
        slot = &ev;
        break;
      }
  }
  assert(slot);

  if (--slot->ref == 0) {
      slot->set_free();
      slot->event = nullptr;
  }

  return false;
}

bool EventService::del_fd(int fd) {
  struct epoll_event ev;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &ev) == 0) return false;

  MyOsError(errno, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
  return true;
}

bool EventService::setup() {
  if ((control_fd = eventfd(0, EFD_NONBLOCK)) == -1 ||
      (epoll_fd = epoll_create(128)) == -1) {
    MyOsError(errno, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
    if (control_fd != -1) close(control_fd);
    return true;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.u64 = ctrl_magic;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, control_fd, &ev) != 0) {
    MyOsError(errno, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
    return true;
  }

  return false;
}

void EventService::stop() {
  assert(control_fd != -1);
  state = State::Stopping;
  uint64_t val = 1;
  write(control_fd, &val, sizeof(val));
  while (state != State::Stopped) my_sleep(1);
}

void EventService::mainloop() {
  state = State::Started;
  int nfds;
  for (;;) {
    if ((nfds = epoll_wait(epoll_fd, events, sizeof(events) / sizeof(events[0]),
                           -1)) == -1)
        continue;

    bool need_free_session = false;
    for (int n = 0; n < nfds; ++n) {
        if (events[n].data.u64 == ctrl_magic) {
          if (state == State::Stopping) {
            state = State::Stopped;
            return;
          }

          need_free_session = true;
          continue;
        }

        auto *evslot = static_cast<EventSlot *>(events[n].data.ptr);
        if (evslot->pin()) {
          evslot->event->Set();
          evslot->unpin();
        }
    }

    if (need_free_session) free_sessions();
  }
}

void EventService::cleaup() {
  free_sessions();
  close(epoll_fd);
  close(control_fd);
}

static void *launch_service_handle(void *arg) {
  auto *service = static_cast<EventService *>(arg);
  service->mainloop();

  return nullptr;
}

bool StartEventService() {
  assert(!event_service);
  event_service = new (std::nothrow) EventService;
  if (!event_service) {
    my_error(ER_OUTOFMEMORY, MYF(0));
    return true;
  }

  if (event_service->setup()) return true;

  int res = mysql_thread_create(
      PSI_INSTRUMENT_ME, &event_service->thread_handle,
      &connection_attrib, launch_service_handle, event_service);
  if (res != 0) {
    MyOsError(errno, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
    delete event_service;
    event_service = nullptr;
  }

  return false;
}

void StopEventService() {
  if (!event_service) return;
  event_service->stop();
  event_service->cleaup();
  delete event_service;
  event_service = nullptr;
}
}  // namespace comm
}  // namespace pq
