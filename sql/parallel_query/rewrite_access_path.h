#ifndef PARALLEL_QUERY_REWRITE_ACCESS_PATH_H
#define PARALLEL_QUERY_REWRITE_ACCESS_PATH_H

#include "my_base.h"
#include <cassert>

class MEM_ROOT;
class Item_clone_context;
class JOIN;
class AccessPath;
class TABLE;
struct ORDER;

namespace pq {
/**
   A access path rewriter to decompose access path tree into parallel plan: a
   plan evaluated on leader and the partial plan which is evaluated on workers.
   This class is also in charge of generation of worker plan from partial plan.
 */
class AccessPathRewriter {
 public:
  enum class GroupStrategy {
    NoPushed,
    TwoStage,
    Pushed,
  };
  struct SortingInfo {
    TABLE *table;
    int ref_item_slice;
  };
  GroupStrategy group_strategy{GroupStrategy::TwoStage};
  using super = AccessPathRewriter;
  MEM_ROOT *mem_root;
  Item_clone_context *clone_context;
  JOIN *join_in;
  JOIN *join_out;
  AccessPath *out_path{nullptr};
  virtual bool end_of_out_path() { return false; }
  void set_sorting_info(SortingInfo sorting_info) {
    m_sorting_info = sorting_info;
  }
  virtual bool rewrite_materialize(AccessPath *in, AccessPath *out);
  virtual bool rewrite_temptable_aggregate(AccessPath *, AccessPath *) = 0;
  virtual bool rewrite_sort(AccessPath *, AccessPath *) { return false; }

  virtual bool rewrite_aggregate(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_table_scan(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_filter(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_stream(AccessPath *, AccessPath *) {
    assert(false);
    return false;
  }
  virtual bool rewrite_limit_offset(AccessPath *, AccessPath *) {return false;}

 protected:
  bool do_rewrite(AccessPath **in);
  SortingInfo m_sorting_info{nullptr, 0};
};

class AccessPathParallelizer : public AccessPathRewriter {
 public:
  AccessPath *replacement_access_path{nullptr};
  AccessPath *collector_access_path() const {
    return replacement_access_path;
  }
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;

  bool rewrite_temptable_aggregate(AccessPath *in, AccessPath *) override;

  bool rewrite_sort(AccessPath *in, AccessPath *out) override;

  bool rewrite_aggregate(AccessPath *in, AccessPath *) override;
  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_stream(AccessPath *, AccessPath *) override;
  bool rewrite_limit_offset(AccessPath *in, AccessPath *out) override;

  bool end_of_out_path() override { return m_collector_path_pos != nullptr; }
  bool do_parallelize(AccessPath **in);
  ORDER *MergeSort() const { return merge_sort; }

 private:
  ORDER *merge_sort{nullptr};
  void set_collector_path_pos(AccessPath **path);
  AccessPath **collector_path_pos() const { return m_collector_path_pos; }

  AccessPath **m_collector_path_pos{nullptr};
};

class PartialAccessPathRewriter : public AccessPathRewriter {
public:
  TABLE *target_table{nullptr};
  TABLE *replacement_table(TABLE *) const { return target_table; }
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;
  bool rewrite_temptable_aggregate(AccessPath *, AccessPath *out) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_table_scan(AccessPath *, AccessPath *) override;
  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_aggregate(AccessPath *, AccessPath *) override;
  bool clone_and_rewrite(AccessPath **from) { return do_rewrite(from); }
};

}  // namespace pq
#endif
