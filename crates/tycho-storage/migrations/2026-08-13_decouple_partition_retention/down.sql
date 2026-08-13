-- Revert to coupled creation + retention: restore the part_config retention set by
-- 2024-09-16_v0.17.1_reduce_pg_partman_retention so run_maintenance() drops expired
-- partitions again, and remove the separate drop job.

SELECT cron.unschedule(jobid)
FROM cron.job
WHERE command LIKE '%drop_expired_partitions%';

DROP PROCEDURE IF EXISTS drop_expired_partitions(interval, text, integer);

UPDATE partman.part_config
SET retention = '1 month'
WHERE parent_table IN ('public.component_balance', 'public.contract_storage', 'public.protocol_state');
