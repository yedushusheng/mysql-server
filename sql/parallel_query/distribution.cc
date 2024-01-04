#include "sql/parallel_query/distribution.h"
#include "sql/parallel_query/planner.h"

namespace pq {
namespace dist {
PartialDistPlan *Adapter::MakePartialDistPlan(PartialPlan *partial_plan,
                                              dist::NodeArray *exec_nodes) {
  auto *dist_plan = DoMakePartialDistPlan(partial_plan, exec_nodes);
  if (dist_plan) partial_plan->SetDistPlan(dist_plan);
  return dist_plan;
}
}  // namespace dist
}  // namespace pq
