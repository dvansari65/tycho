-- Decouple partition retention from partition creation
--
-- pg_partman's run_maintenance() creates the upcoming partitions and drops the expired ones
-- inside one transaction per parent table. By the time it issues the retention DROP TABLE it
-- already holds locks on the default partition from the earlier maintenance steps, and the
-- DROP needs an AccessExclusiveLock on the parent. Any concurrent query that acquired an
-- AccessShareLock on the parent in between and then waits on the default partition closes a
-- lock cycle; Postgres resolves it by killing the maintenance run. The rollback also discards
-- the partitions created earlier in the transaction, so a lost retention race silently stops
-- partition premake. When the premade partitions run out, inserts of closed row versions land
-- in the default partition and collide with its (component, token) unique index, crashing the
-- writer.
--
-- Retention is therefore removed from part_config, making the nightly run_maintenance()
-- creation-only: it can no longer be rolled back by a retention failure. Expired partitions
-- are dropped by drop_expired_partitions() below, scheduled separately. Each partition is
-- dropped in its own transaction that holds no prior locks when it requests the parent lock,
-- so it can queue behind running queries but cannot deadlock. A short lock_timeout bounds how
-- long the queued request may stall other queries on the parent (lock requests behind a
-- waiting AccessExclusiveLock cannot be granted until it is); on timeout the attempt rolls
-- back and is retried after a pause. A run that exhausts its attempts raises at the end, so
-- the failure is visible in cron.job_run_details, and the next scheduled run picks up the
-- surviving partitions. The worst outcome of a lost race is now an expired partition living
-- one day longer, instead of a frozen premake runway.
--
-- The procedure issues DROP TABLE itself instead of calling partman.drop_partition_time():
-- partman wraps errors in a plain RAISE EXCEPTION, which replaces the SQLSTATE needed to
-- distinguish a retryable lock timeout from a real failure. partman.show_partition_info() is
-- still used to derive each child's boundaries from its name.

UPDATE partman.part_config
SET retention = NULL
WHERE parent_table IN ('public.component_balance', 'public.contract_storage', 'public.protocol_state');

-- Retention now lives in p_retention below instead of part_config (part_config.retention
-- would put the drop back inside run_maintenance). To change it, alter this default; to
-- change which tables are swept, update the array.
CREATE OR REPLACE PROCEDURE drop_expired_partitions(
    p_retention interval DEFAULT '1 month',
    p_lock_timeout text DEFAULT '2s',
    p_max_attempts integer DEFAULT 5
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_horizon timestamptz := clock_timestamp() - p_retention;
    v_parent text;
    v_children text[];
    v_child text;
    v_attempt integer;
    v_done boolean;
    v_failed text[] := '{}';
BEGIN
    FOREACH v_parent IN ARRAY ARRAY[
        'public.component_balance',
        'public.contract_storage',
        'public.protocol_state'
    ] LOOP
        -- Materialize the expired children before dropping: COMMIT is not allowed while a
        -- query cursor is open. A child is expired when its whole range lies behind the
        -- horizon. The default partition has no bounds and is never dropped. Names are
        -- partman-generated (parent_pYYYYMMDD), so they are safe to splice into DDL.
        SELECT coalesce(array_agg(n.nspname || '.' || c.relname ORDER BY c.relname), '{}')
        INTO v_children
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.inhparent = v_parent::regclass
          AND pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
          AND (SELECT child_end_time
               FROM partman.show_partition_info(
                   n.nspname || '.' || c.relname,
                   p_parent_table := v_parent
               )) <= v_horizon;

        FOREACH v_child IN ARRAY v_children LOOP
            v_done := false;
            v_attempt := 0;
            WHILE NOT v_done AND v_attempt < p_max_attempts LOOP
                v_attempt := v_attempt + 1;
                BEGIN
                    -- Applies to the current transaction only. If the parent lock is not
                    -- granted within the timeout, give up and retry instead of stalling the
                    -- queries that queue behind the AccessExclusiveLock request.
                    PERFORM set_config('lock_timeout', p_lock_timeout, true);
                    EXECUTE format('DROP TABLE %s', v_child);
                    v_done := true;
                EXCEPTION
                    WHEN lock_not_available OR deadlock_detected THEN
                        RAISE NOTICE 'drop_expired_partitions: lock not available for % (attempt % of %)',
                            v_child, v_attempt, p_max_attempts;
                END;
                -- One transaction per drop: releases the parent lock (or nothing, after a
                -- caught lock failure) before the backoff sleep or the next partition.
                COMMIT;
                IF NOT v_done AND v_attempt < p_max_attempts THEN
                    PERFORM pg_sleep(30);
                END IF;
            END LOOP;
            IF NOT v_done THEN
                -- Siblings need the same parent lock, so skip the rest of this parent; the
                -- next run retries them.
                v_failed := v_failed || v_child;
                EXIT;
            END IF;
            RAISE NOTICE 'drop_expired_partitions: dropped %', v_child;
        END LOOP;
    END LOOP;

    -- Completed drops are already committed; raising here only marks the run as failed in
    -- cron.job_run_details so persistent lock contention is observable.
    IF array_length(v_failed, 1) > 0 THEN
        RAISE EXCEPTION 'drop_expired_partitions: lock not granted for % within % attempts, remaining partitions retry next run',
            v_failed, p_max_attempts;
    END IF;
END;
$$;

-- 00:30, after the (now creation-only) midnight run_maintenance and clear of the 02:00
-- transaction cleanup.
SELECT cron.schedule(
    'drop_expired_partitions',
    '30 0 * * *',
    'CALL drop_expired_partitions();'
);
