# include/not_parallel_query.inc

if ($PARALLEL_QUERY_TEST) {
  --skip Not supported under '--parallel-query'
}
