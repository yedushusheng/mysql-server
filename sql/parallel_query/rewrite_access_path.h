#ifndef PARALLEL_QUERY_REWRITE_ACCESS_PATH_H
#define PARALLEL_QUERY_REWRITE_ACCESS_PATH_H

#include "my_base.h"
#include "sql/item.h"

class Item_clone_context;
class JOIN;

namespace pq {
/**
   A access path rewriter to decompose access path tree into parallel plan: a
   plan evaluated on leader and the partial plan which is evaluated on workers.
   This class is also in charge of generation of worker plan from partial plan.
 */
class AccessPathRewriter {
 public:
  AccessPathRewriter(Item_clone_context *item_clone_context, JOIN *join_in,
                     JOIN *join_out)
      : m_item_clone_context(item_clone_context),
        m_join_in(join_in),
        m_join_out(join_out) {}

  AccessPath *out_path() const { return m_out_path; }

 protected:
  using super = AccessPathRewriter;
  struct SortingInfo {
    TABLE *table;
    int ref_item_slice;
  };

  bool do_rewrite(AccessPath *in);
  virtual bool end_of_out_path() { return false; }
  void set_sorting_info(SortingInfo sorting_info) {
    m_sorting_info = sorting_info;
  }
  bool rewrite_each_access_path(AccessPath *in);

  // See access_path.h, rewrite functions are followed same order with it.
  virtual bool rewrite_table_scan(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_index_scan(AccessPath *, AccessPath *) { return false; }

  virtual bool rewrite_filter(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_sort(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_aggregate(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_temptable_aggregate(AccessPath *, AccessPath *) = 0;
  virtual bool rewrite_limit_offset(AccessPath *, AccessPath *) {return false;}
  virtual bool rewrite_stream(AccessPath *, AccessPath *) = 0;
  virtual bool rewrite_materialize(AccessPath *in, AccessPath *out);

  MEM_ROOT *mem_root() const { return m_item_clone_context->mem_root(); };

  Item_clone_context *m_item_clone_context;
  JOIN *m_join_in;
  JOIN *m_join_out;
  SortingInfo m_sorting_info{nullptr, 0};

 private:
  AccessPath *m_out_path{nullptr};
};

class AccessPathParallelizer : public AccessPathRewriter {
 public:
  AccessPathParallelizer(Item_clone_context *item_clone_context, JOIN *join_in,
                         JOIN *join_out)
      : AccessPathRewriter(item_clone_context, join_in, join_out) {}
  AccessPath *parallelize_access_path(AccessPath *in);
  ORDER *MergeSort() const { return merge_sort; }
  void set_collector_access_path(AccessPath *path) {
    m_collector_access_path = path;
  }
  AccessPath *collector_access_path() const { return m_collector_access_path; }

 private:
  bool end_of_out_path() override { return m_collector_path_pos != nullptr; }
  void set_collector_path_pos(AccessPath **path);
  bool init_table_parallel_scan(TABLE *table, uint keynr, bool reverse);
  AccessPath **collector_path_pos() const { return m_collector_path_pos; }

  // Rewrite routines for each access path
  bool rewrite_table_scan(AccessPath *, AccessPath *) override;
  bool rewrite_index_scan(AccessPath *, AccessPath *) override;

  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *in, AccessPath *) override;
  bool rewrite_temptable_aggregate(AccessPath *in, AccessPath *) override;
  bool rewrite_limit_offset(AccessPath *in, AccessPath *out) override;
  bool rewrite_stream(AccessPath *, AccessPath *) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;

  AccessPath *m_collector_access_path{nullptr};
  AccessPath **m_collector_path_pos{nullptr};
  ORDER *merge_sort{nullptr};
};

class PartialAccessPathRewriter : public AccessPathRewriter {
 public:
  PartialAccessPathRewriter(Item_clone_context *item_clone_context,
                            JOIN *join_in, JOIN *join_out)
      : AccessPathRewriter(item_clone_context, join_in, join_out) {}

  bool clone_and_rewrite(AccessPath *from) { return do_rewrite(from); }

 private:
  TABLE *find_leaf_table(TABLE *) const;

  // Rewrite routines for each access path
  bool rewrite_table_scan(AccessPath *, AccessPath *) override;
  bool rewrite_index_scan(AccessPath *, AccessPath *) override;

  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *, AccessPath *) override;
  bool rewrite_temptable_aggregate(AccessPath *, AccessPath *out) override;
  bool rewrite_stream(AccessPath *, AccessPath *) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;

  TABLE *m_leaf_table{nullptr};
};

}  // namespace pq
#endif
