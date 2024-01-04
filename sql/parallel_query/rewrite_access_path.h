#ifndef PARALLEL_QUERY_REWRITE_ACCESS_PATH_H
#define PARALLEL_QUERY_REWRITE_ACCESS_PATH_H

#include "my_base.h"
#include "sql/item.h"

class Item_clone_context;
class JOIN;
class RowIterator;
class QUICK_SELECT_I;

namespace pq {
class PartialPlan;

struct AccessPathChanges {
  AccessPath *access_path;
  union {
    // materialize, temptable_aggregate and stream;
    struct {
      TABLE *table;
      Temp_table_param *temp_table_param;
    } recreated_temp_table;
    struct {
      TABLE *table;
    } table_path;
    struct {
      JOIN *join;
    } source;
  } u;
  void restore();
};

class AccessPathChangesStore {
 public:
  AccessPathChangesStore(JOIN *join)
      : m_join(join) {}
  ~AccessPathChangesStore() { restore_changes(); }
  void register_changes(AccessPathChanges &&changes) {
    m_changes.push_back(changes);
  }
  void set_root_path(AccessPath *path) { m_access_path = path; }
  void register_collector_path(AccessPath **path) {
    m_collector_path_pos = path;
    m_access_path = *path;
  }
  void clear() {
    m_changes.clear();
    m_access_path = nullptr;
  }

 private:
  void restore_changes();
  std::vector<AccessPathChanges> m_changes;
  AccessPath **m_collector_path_pos{nullptr};
  AccessPath *m_access_path{nullptr};
  JOIN *m_join;
};

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
  virtual bool rewrite_ref(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_ref_or_null(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_eq_ref(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_index_range_scan(AccessPath *, AccessPath *) {
    return false;
  }
  virtual bool rewrite_filter(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_sort(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_aggregate(AccessPath *, AccessPath *) { return false; }
  virtual bool rewrite_temptable_aggregate(AccessPath *, AccessPath *) = 0;
  virtual bool rewrite_limit_offset(AccessPath *, AccessPath *) {
    return false;
  }
  virtual bool rewrite_stream(AccessPath *, AccessPath *) = 0;
  virtual bool rewrite_materialize(AccessPath *in, AccessPath *out);

  bool do_stream_rewrite(JOIN *join, AccessPath *path);

  MEM_ROOT *mem_root() const { return m_item_clone_context->mem_root(); }
  /**
    Do some additional rewrites for out access path, access path parallelizer
    use it to set fake timing iterator.
  */
  virtual void post_rewrite_out_path(AccessPath *out [[maybe_unused]]) {}

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
                         PartialPlan *partial_plan,
                         AccessPathChangesStore *path_changes_store);
  AccessPath *parallelize_access_path(AccessPath *in);
  ORDER *MergeSort() const { return merge_sort; }
  void set_collector_access_path(AccessPath *path) {
    m_collector_access_path = path;
  }
  AccessPath *collector_access_path() const { return m_collector_access_path; }
  void set_fake_timing_iterator(RowIterator *iterator) {
    m_fake_timing_iterator = iterator;
  }
  bool has_pushed_limit_offset() const { return m_pushed_limit_offset; }

 private:
  bool end_of_out_path() override { return m_collector_path_pos != nullptr; }
  void set_collector_path_pos(AccessPath **path);
  void set_table_parallel_scan(TABLE *table, uint keynr, bool reverse);

  AccessPath **collector_path_pos() const { return m_collector_path_pos; }

  // Rewrite routines for each access path
  bool rewrite_table_scan(AccessPath *, AccessPath *) override;
  bool rewrite_index_scan(AccessPath *, AccessPath *) override;
  bool rewrite_ref(AccessPath *, AccessPath *) override;
  bool rewrite_ref_or_null(AccessPath *, AccessPath *) override;
  bool rewrite_eq_ref(AccessPath *, AccessPath *) override;
  bool rewrite_index_range_scan(AccessPath *, AccessPath *) override;

  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *in, AccessPath *) override;
  bool rewrite_temptable_aggregate(AccessPath *in, AccessPath *) override;
  bool rewrite_limit_offset(AccessPath *in, AccessPath *out) override;
  bool rewrite_stream(AccessPath *, AccessPath *) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;

  void post_rewrite_out_path(AccessPath *out) override;

  void rewrite_index_access_path(
      TABLE *table, uint keynr, bool use_order, bool reverse,
      std::function<void(uint16_t *, key_range **, key_range **, bool *)>
          get_scan_range);

  PartialPlan *m_partial_plan;
  AccessPath *m_collector_access_path{nullptr};
  AccessPath **m_collector_path_pos{nullptr};
  ORDER *merge_sort{nullptr};
  RowIterator *m_fake_timing_iterator{nullptr};
  bool m_pushed_limit_offset{false};
  AccessPathChangesStore *m_path_changes_store;
};

class PartialAccessPathRewriter : public AccessPathRewriter {
 public:
  PartialAccessPathRewriter(Item_clone_context *item_clone_context,
                            JOIN *join_in, JOIN *join_out)
      : AccessPathRewriter(item_clone_context, join_in, join_out) {}

  bool clone_and_rewrite(AccessPath *from) { return do_rewrite(from); }

 private:
  TABLE *find_leaf_table(TABLE *table) const;

  // Rewrite routines for each access path
  bool rewrite_table_scan(AccessPath *, AccessPath *) override;
  bool rewrite_index_scan(AccessPath *, AccessPath *) override;
  bool rewrite_ref(AccessPath *, AccessPath *) override;
  bool rewrite_ref_or_null(AccessPath *, AccessPath *) override;
  bool rewrite_eq_ref(AccessPath *, AccessPath *) override;
  bool rewrite_index_range_scan(AccessPath *in, AccessPath *out) override;
  template <typename aptype>
  bool rewrite_base_scan(aptype &out, uint keyno);
  template <typename aptype>
  bool rewrite_base_ref(aptype &out);

  bool rewrite_filter(AccessPath *, AccessPath *) override;
  bool rewrite_sort(AccessPath *in, AccessPath *out) override;
  bool rewrite_aggregate(AccessPath *, AccessPath *) override;
  bool rewrite_temptable_aggregate(AccessPath *, AccessPath *out) override;
  bool rewrite_stream(AccessPath *, AccessPath *) override;
  bool rewrite_materialize(AccessPath *in, AccessPath *out) override;
};

}  // namespace pq
#endif
