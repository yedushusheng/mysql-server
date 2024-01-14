--disable_query_log
--let $degree=1
eval SET max_parallel_degree = $degree;
--enable_query_log

--echo enable parallel query with max_parallel_degree = $degree
