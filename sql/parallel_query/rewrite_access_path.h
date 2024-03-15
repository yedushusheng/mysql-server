#ifndef PARALLEL_QUERY_REWRITE_ACCESS_PATH_H
#define PARALLEL_QUERY_REWRITE_ACCESS_PATH_H

#include "sql/mem_root_array.h"
#include "sql/parallel_query/planner.h"

class RowIterator;

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

 protected:
  using super = AccessPathRewriter;
  struct SortingInfo {
    TABLE *table;
    int ref_item_slice;
  };

  /// Some query can generate new access path for leader, so here @param path
  /// could be changed by underlying rewrite routines, currently AGGREGATE
  /// access path from select_count.
  bool do_rewrite(AccessPath *&path, AccessPath *curjoin, AccessPath *&out);
  virtual bool end_of_out_path() { return false; }
  void set_sorting_info(SortingInfo sorting_info) {
    m_sorting_info = sorting_info;
  }

  /// workhorse dispatcher for each access path rewrite, parameters: @param
  /// in: current input access path, it can be changed by type driver
  /// routine. @param curjoin: innermost parent JOIN access path node which
  /// has 2 children; @outer_path the outer child access path if it is a
  /// JOIN; @param out: current result access path node until now, it is the
  /// access path of partial plan for plan parallelizer and the worker
  /// execution copy of a plan for the partial access path rewriter.
  bool rewrite_each_access_path(AccessPath *&in, AccessPath *curjoin,
                                AccessPath *outer_path, AccessPath *&out);

  // See access_path.h, rewrite functions are followed same order with it.
  virtual bool rewrite_table_scan(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_index_scan(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_ref(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_ref_or_null(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_eq_ref(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_mrr(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_const_table(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_index_range_scan(AccessPath *, AccessPath *) {
    return false;
  }
  virtual bool rewrite_unqualified_count(AccessPath *&, AccessPath *) {
    return false;
  }

  virtual bool rewrite_nested_loop_semijoin_with_duplicate_removal(
      AccessPath *, AccessPath *) {
    return false;
  }
  virtual bool rewrite_hash_join(AccessPath *in, AccessPath *out);

  virtual bool rewrite_filter(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_sort(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_aggregate(AccessPath *, AccessPath *&) { return false; }
  virtual bool rewrite_temptable_aggregate(AccessPath *in,
                                           AccessPath *&out) = 0;
  virtual bool rewrite_limit_offset(AccessPath *, AccessPath *, bool) {
    return false;
  }
  virtual bool rewrite_stream(AccessPath *in, AccessPath *out) = 0;
  virtual bool rewrite_materialize(AccessPath *in, AccessPath *&out,
                                   bool under_join);
  virtual bool rewrite_weedout(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_remove_duplicates_on_index(AccessPath *, AccessPath *) {
    return false;
  }

  bool do_stream_rewrite(JOIN *join, AccessPath *path);

  MEM_ROOT *mem_root() const { return m_item_clone_context->mem_root(); }
  AccessPath *accesspath_dup(AccessPath *path);

  Item_clone_context *m_item_clone_context;
  JOIN *m_join_in;
  JOIN *m_join_out;
  SortingInfo m_sorting_info{nullptr, 0};

 private:
  AccessPath *accesspath_dup_if_out(AccessPath *path) {
    return end_of_out_path() ? nullptr : accesspath_dup(path);
  }
};

class AccessPathParallelizer : public AccessPathRewriter {
 public:
  AccessPathParallelizer(ParallelPlan *parallel_plan,
                         Item_clone_context *item_clone_context);
  /// Return parallelized access path tree. It may not be @param in e.g. All
  /// plan has been pushed.
  AccessPath *parallelize_access_path(Collector *collector, AccessPath *in,
                                      AccessPath *&partial_path);
  ORDER *MergeSort(bool *remove_duplicates) const {
    *remove_duplicates = merge_sort_remove_duplicates;
    return merge_sort;
  }
  bool has_pushed_limit_offset() const { return m_pushed_limit_offset; }

 private:
  bool end_of_out_path() override { return m_collector_path_pos != nullptr; }
  void set_collector_path_pos(AccessPath **path);
  AccessPath **collector_path_pos() const { return m_collector_path_pos; }

  // Rewrite routines for each access path, Don't need rewrite_table_scan(),
  // nothing to do
  bool rewrite_index_scan(AccessPath *in, AccessPath *out) override;
  bool rewrite_ref(AccessPath *in, AccessPath *out) override;
  bool rewrite_ref_or_null(AccessPath *in, AccessPath *out) override;
  bool rewrite_eq_ref(AccessPath *in, AccessPath *out) override;
  bool rewrite_index_range_scan(AccessPath *in, AccessPath *out) override;
  bool rewrite_unqualified_count(AccessPath *&, AccessPath *) override;

  bool rewrite_filter(AccessPath *in, AccessPath *out) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *in, AccessPath *&out) override;
  bool rewrite_temptable_aggregate(AccessPath *in, AccessPath *&out) override;
  bool rewrite_limit_offset(AccessPath *in, AccessPath *out,
                            bool under_join) override;
  bool rewrite_stream(AccessPath *in, AccessPath *out) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *&out,
                           bool under_join) override;

  void rewrite_index_access_path(TABLE *table, bool use_order, bool reverse);
  template <typename aptype>
  bool rewrite_base_ref(aptype &out, bool reverse);

  ParallelPlan *m_parallel_plan;
  TABLE *m_collector_table{nullptr};
  AccessPath **m_collector_path_pos{nullptr};
  ORDER *merge_sort{nullptr};
  bool merge_sort_remove_duplicates{false};
  bool m_pushed_limit_offset{false};
};

class PartialAccessPathRewriter : public AccessPathRewriter {
 public:

  PartialAccessPathRewriter(Item_clone_context *item_clone_context,
                            JOIN *join_in, JOIN *join_out);
      : AccessPathRewriter(item_clone_context, join_in, join_out) {}

  AccessPath *clone_and_rewrite(AccessPath *from) {
    AccessPath *out = nullptr;
    if (do_rewrite(from, nullptr, out)) return nullptr;
    return out;
  }

  // Return a new object of Mem_root_array on current MEM_ROOT
  Mem_root_array<QEP_execution_state> *qep_execution_state();
 private:
  TABLE *find_leaf_table(TABLE *table) const;

  // Rewrite routines for each access path
  bool rewrite_table_scan(AccessPath *in, AccessPath *out) override;
  bool rewrite_index_scan(AccessPath *in, AccessPath *out) override;
  bool rewrite_ref(AccessPath *in, AccessPath *out) override;
  bool rewrite_ref_or_null(AccessPath *in, AccessPath *out) override;
  bool rewrite_eq_ref(AccessPath *in, AccessPath *out) override;
  bool rewrite_mrr(AccessPath *in, AccessPath *out) override;
  bool rewrite_const_table(AccessPath *in, AccessPath *out) override;
  bool rewrite_index_range_scan(AccessPath *in, AccessPath *out) override;
  bool rewrite_unqualified_count(AccessPath *&, AccessPath *) override;
  template <typename aptype>
  bool rewrite_base_scan(aptype &out, uint keyno);
  template <typename aptype>
  bool rewrite_base_ref(aptype &out);

  bool rewrite_nested_loop_semijoin_with_duplicate_removal(
      AccessPath *in, AccessPath *out) override;
  bool rewrite_hash_join(AccessPath *in, AccessPath *out) override;

  bool rewrite_filter(AccessPath *in, AccessPath *out) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *in, AccessPath *&out) override;
  bool rewrite_temptable_aggregate(AccessPath *in, AccessPath *&out) override;
  bool rewrite_stream(AccessPath *in, AccessPath *out) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *&out,
                           bool under_join) override;
  bool rewrite_weedout(AccessPath *in, AccessPath *out) override;
  bool rewrite_remove_duplicates_on_index(AccessPath *in,
                                          AccessPath *out) override;
Mem_root_array<QEP_execution_state> m_qep_execution_state;                                          
};

}  // namespace pq
#endif
