#ifndef PARALLEL_QUERY_BRPC_STREAM_COMM_H
#define PARALLEL_QUERY_BRPC_STREAM_COMM_H

#include <unordered_set>
#include <unordered_map>
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "mysql/psi/mysql_mutex.h"
typedef unsigned __int128 uint128;

struct uint128_hash {
  std::size_t operator()(const uint128 &x) const { return butil::fmix64(x); }
};

namespace pq {

class CheckUnique {
 public:
  static CheckUnique &getInstance() {
    static CheckUnique instance;
    return instance;
  }

  void Check(uint64_t trans_id, uint64_t collector_id) {
    uint128 group_id = static_cast<uint128>(trans_id) << 64 | collector_id;
    mysql_mutex_lock(&set_mtx_);
    auto result = set_.insert(group_id);
    mysql_mutex_unlock(&set_mtx_);
    if (!result.second) assert(0);
  }

  void Erase(uint64_t trans_id, uint64_t collector_id) {
    uint128 group_id = static_cast<uint128>(trans_id) << 64 | collector_id;
    mysql_mutex_lock(&set_mtx_);
    auto result = set_.erase(group_id);
    mysql_mutex_unlock(&set_mtx_);
    if (result != 1) assert(0);
  }

 private:
  CheckUnique() { mysql_mutex_init(PSI_NOT_INSTRUMENTED, &set_mtx_, nullptr); }

  ~CheckUnique() { mysql_mutex_destroy(&set_mtx_); }

  mysql_mutex_t set_mtx_;
  std::unordered_set<uint128,uint128_hash> set_;
};

}  // namespace pq
#endif
