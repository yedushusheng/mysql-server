#ifndef CDB_PLAN_CACHE_INCLUDED
#define CDB_PLAN_CACHE_INCLUDED

#include <sys/types.h>
#include <string>
#include <unordered_map>
#include <vector>
#include "my_psi_config.h"
#include "mysql/components/services/mysql_rwlock_bits.h"
#include "mysql/components/services/psi_rwlock_bits.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysql/psi/mysql_thread.h"

#define MAX_PLAN_CACHE_SQL_LEN 10240
using std::pair;
using std::string;
using std::unordered_map;
using std::vector;

enum distribute_plan_cache_mode { PREPARE_STMT_MODE = 0 };
typedef struct distribute_plan_cache_info {
  std::string sql;
  distribute_plan_cache_mode mode;
  ulong hits;
} distribute_plan_cache_info;

/**
  We use `Distribute_plan_cache_stats` to view the query plan hit statistics,
  the statistics owned by the current THD, if the THD destroy, the statistic
  will be clear.
*/
class Distribute_plan_cache_stats {
  typedef unordered_map<string, distribute_plan_cache_info> plan_cache_stats;
  typedef unordered_map<string, distribute_plan_cache_info>::iterator
      plan_cache_stats_iterator;

 public:
  Distribute_plan_cache_stats() { mysql_rwlock_init(m_key_lock, &m_lock); }
  ~Distribute_plan_cache_stats() { mysql_rwlock_destroy(&m_lock); }

 public:
  /**
   get the plan cache info from `Distribute_plan_cache_stats` by rw_lock to
   show.
  */
  void get_plan_cache_stat(vector<distribute_plan_cache_info> &stat);
  void add_plan_cache_info(const string &sql);
  bool hit_plan_cache(const string &sql);

  /**
   clear the plan cache info from `Distribute_plan_cache_stats` by rw_lock, when
   destructor of `Prepare_statement` or `THD` destroy.
  */
  void clear_plan_cache_info();

 private:
  PSI_rwlock_key m_key_lock;
  mysql_rwlock_t m_lock;
  plan_cache_stats m_pc_stat;
};

bool plan_cache_enabled();
bool plan_cache_stats_enabled();

#endif /* CDB_PLAN_CACHE_INCLUDED */