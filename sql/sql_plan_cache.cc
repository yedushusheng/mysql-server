#include "sql_plan_cache.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysql/psi/mysql_thread.h"
#include "sql/mysqld.h"


/**
  MySQL server processing includes parse, prepare, optimize and execution.

  For prepare statement, it contains LEX struct as the main member variable,
  which help omit the whole parse phase when exeucting the statment multiple
  times. Prepare phase is aslo omited in WL#9384, but optimize phase will
  be done every statement execution.

  For some short query, optimize phase still occupys a large proportion.
  Skipping optimize phase will get great performace gains.

  Since MySQL server introduced the volcano framework since official version
  8.0.16. Physical execution plan which contains physical iterator operators
  can be saved easier, though some member variables of certain physical operator
  are nested with deeply structure, like QEP_TAB in TableScanIterator, which
  is not easy to be saved.

  To enhance prepare once issue(WL#9384) for some SQL. We need to save more
  optimize-phase structure in prepare statement than current realization, that
  can help optimize once in prepare phase. Most importantly, the precondition
  is that all iterators in physical plan can be binded.

  Currently, plan cache management only works on some limited prepare statement.
  We only support ConstIterator parameters binding, thus we only support
  the Primary key and Unique key query restrictedly.

  In terms of memory management, We keep same with current prepare statement,
  THD level memory will be used for allocate optimize phase structure.
*/

void Distribute_plan_cache_stats::get_plan_cache_stat(
    vector<distribute_plan_cache_info> &stat) {
  mysql_rwlock_rdlock(&m_lock);
  plan_cache_stats_iterator ri = m_pc_stat.begin();
  while (ri != m_pc_stat.end()) {
    stat.push_back(ri->second);
    ri++;
  }
  mysql_rwlock_unlock(&m_lock);
}

void Distribute_plan_cache_stats::add_plan_cache_info(const string &sql) {
  mysql_rwlock_wrlock(&m_lock);
  distribute_plan_cache_info pci = {sql, distribute_plan_cache_mode::PREPARE_STMT_MODE,
                               0};
  m_pc_stat.insert(pair<string, distribute_plan_cache_info>(sql, pci));
  mysql_rwlock_unlock(&m_lock);
}

bool Distribute_plan_cache_stats::hit_plan_cache(const string &sql) {
  bool ret = false;
  mysql_rwlock_wrlock(&m_lock);
  plan_cache_stats_iterator itr = m_pc_stat.find(sql);
  if (itr == m_pc_stat.end())
    ret = true;
  else {
    itr->second.hits++;
    ret = false;
  }
  mysql_rwlock_unlock(&m_lock);
  return ret;
}

void Distribute_plan_cache_stats::clear_plan_cache_info() {
  mysql_rwlock_wrlock(&m_lock);
  m_pc_stat.clear();
  mysql_rwlock_unlock(&m_lock);
}

bool plan_cache_enabled() { return distribute_plan_cache_enabled; }
bool plan_cache_stats_enabled() { return distribute_plan_cache_stats_enabled; }