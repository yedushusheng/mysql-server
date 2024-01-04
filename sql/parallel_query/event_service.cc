#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "my_systime.h"
#include "mysql/psi/mysql_thread.h"
#include "mysqld_error.h"
#include "sql/mysqld.h"
#include "sql/parallel_query/comm_types.h"

namespace pq {
namespace comm {

class EventService {
 public:
  enum class State { Stopped, Started };
  void stop();
  bool setup();
  bool add_fd(int fd, Event *event, bool poll_in);
  bool del_fd(int fd);
  void cleaup();
  void mainloop();

  my_thread_handle thread_handle;

 private:
  static constexpr int stop_magic{0xdead};
  std::atomic<State> state{State::Stopped};
  int epoll_fd{-1};
  int control_fd{-1};
  struct epoll_event events[64];
};

static EventService *event_service = nullptr;

bool EventService::add_fd(int fd, Event *event, bool poll_in) {
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

bool EventService::del_fd(int fd) {
  struct epoll_event ev;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &ev) == 0) return false;

  auto error = errno;
  MyOsError(error, ER_OPERATION_PARALLEL_EVENT_SERVICE_ERROR, MYF(0));
  return true;
}

bool EventService::setup() {
  if ((control_fd = eventfd(1, 0)) == -1 ||
      (epoll_fd = epoll_create(128)) == -1) {
    // TODO: Log some meaningful error message.
    if (control_fd != -1) close(control_fd);
    return true;
  }

  return false;
}

void EventService::stop() {
  assert(control_fd != -1);

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.u64 = stop_magic;
  auto res [[maybe_unused]] =
      epoll_ctl(epoll_fd, EPOLL_CTL_ADD, control_fd, &ev);
  assert(res == 0);
  while (state != State::Stopped) my_sleep(1);
}

void EventService::mainloop() {
  state = State::Started;
  int nfds;
  for (;;) {
    if ((nfds = epoll_wait(epoll_fd, events, 64, -1)) == -1) continue;
    // TODO: there is a race condition here when removing a fd from epoll:
    // If a user removes a fd from epoll that is in events, here event set
    // operation would lead to a segv.
    for (int n = 0; n < nfds; ++n) {
      if (events[n].data.u64 == stop_magic) {
        state = State::Stopped;
        return;
      }
      Event *ev = static_cast<Event *>(events[n].data.ptr);
      ev->Set();
    }
  }
}

void EventService::cleaup() {
  close(epoll_fd);
  close(control_fd);
}

static void *launch_service_handle(void *arg) {
  auto *service = static_cast<EventService *>(arg);
  service->mainloop();

  return nullptr;
}

int StartEventService() {
  assert(!event_service);
  event_service = new (std::nothrow) EventService;
  if (!event_service) return ENOMEM;
  if (event_service->setup()) return true;

  int res = mysql_thread_create(
      PSI_INSTRUMENT_ME, &event_service->thread_handle,
      &connection_attrib, launch_service_handle, event_service);
  if (res != 0) {
    delete event_service;
    event_service = nullptr;
  }
  return res;
}

void StopEventService() {
  if (!event_service) return;
  event_service->stop();
  event_service->cleaup();
  delete event_service;
  event_service = nullptr;
}

bool EventServiceAddFd(int fd, Event *event, bool poll_in) {
  int res;
  // TODO: start event service in background when system startup or in the
  // system variable callback.
  if (!event_service && (res = StartEventService()) != 0) return true;
  return event_service->add_fd(fd, event, poll_in);
}

bool EventServiceRemoveFd(int fd) {
  assert(event_service);

  return event_service->del_fd(fd);
}

}  // namespace comm
}  // namespace pq
