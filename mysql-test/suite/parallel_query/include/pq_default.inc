--disable_query_log
--let $degree=1
eval SET max_parallel_degree = $degree;
SET parallel_plan_cost_threshold=0;
SET parallel_scan_records_threshold=0;
--enable_query_log

--echo enable parallel query forcibly with max_parallel_degree = $degree and cost/records threshold = 0
