#include <sys/types.h>

namespace pq {
namespace comm {
class Event;
}
class PartialPlan;
class Worker;

enum class WorkerScheduleType { bthread, SysThread };

constexpr ulong default_worker_schedule_type =
    static_cast<ulong>(WorkerScheduleType::bthread);
extern ulong worker_handling;

Worker *CreateLocalWorker(uint id, comm::Event *state_event, PartialPlan *plan);
}  // namespace pq
