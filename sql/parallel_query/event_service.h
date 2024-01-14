#ifndef PARALLEL_QUERY_EVENT_SERVICE_H
#define PARALLEL_QUERY_EVENT_SERVICE_H

namespace pq {
namespace comm {
class Event;
int StartEventService();
void StopEventService();
bool EventServiceAddFd(int fd, Event *event, bool poll_in);
bool EventServiceRemoveFd(int fd);
}  // namespace comm
}  // namespace pq

#endif
