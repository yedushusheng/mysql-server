#ifndef PARALLEL_QUERY_EVENT_SERVICE_H
#define PARALLEL_QUERY_EVENT_SERVICE_H

namespace pq {
namespace comm {
class Event;
class EventSession;
// SQLEngine (TDSQL 3.0) don't need event service in production environment.
constexpr bool start_service_when_system_up{false};
bool StartEventService();
void StopEventService();
EventSession *RegisterEventSession();
void UnregisterEventSession(EventSession *es);
bool EventServiceAddFd(EventSession *es, int fd, Event *event, bool poll_in);
bool EventServiceRemoveFd(EventSession *es, int fd, Event *event);
}  // namespace comm
}  // namespace pq

#endif
