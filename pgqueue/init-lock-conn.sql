CREATE TEMPORARY TABLE acquired_jobs (job_id bigint not null, queue text not null);

CREATE FUNCTION pg_temp.acquire_jobs(text, int) RETURNS SETOF pgqueue.jobs AS $$
DECLARE
    acquired_job_ids bigint[];
    owned_lock_count int;
    expected_lock_count int;
BEGIN
    SELECT count(*) INTO owned_lock_count FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid();

    WITH available_jobs AS (
        SELECT j.id
        FROM pgqueue.jobs AS j
        WHERE queue = $1
        AND NOT(id = ANY(SELECT job_id FROM acquired_jobs WHERE queue = $1))
        AND (run_at IS NULL OR run_at <= now())
        AND (expires_at IS NULL OR now() < expires_at)
        AND (failed_at IS NULL OR retryable = true)
        ORDER BY priority DESC, run_at NULLS LAST, expires_at NULLS LAST, id
        OFFSET 0
    ) SELECT array_agg(j.id) INTO acquired_job_ids FROM (SELECT j.id FROM available_jobs j WHERE pg_try_advisory_xact_lock(j.id) LIMIT $2) j;

    expected_lock_count = owned_lock_count + array_length(acquired_job_ids, 1);

    SELECT count(*) INTO owned_lock_count FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid();

    IF owned_lock_count != expected_lock_count THEN
        RAISE EXCEPTION 'Expected % to be the total lock count, but got %', expected_lock_count, owned_lock_count;
    END IF;

    INSERT INTO acquired_jobs SELECT unnest(acquired_job_ids), $1;
    RETURN QUERY SELECT * FROM pgqueue.jobs WHERE id = ANY(acquired_job_ids);
    PERFORM pg_advisory_lock(jid) FROM unnest(acquired_job_ids) jid;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.unlock_jobs(bigint[]) RETURNS void AS $$
BEGIN
    IF (SELECT count(*) FROM acquired_jobs WHERE job_id = ANY($1)) != (SELECT count(*) FROM unnest($1)) THEN
        RAISE EXCEPTION 'You do not have a lock for one of the pgqueue.jobs';
    END IF;

    IF (SELECT sum(pg_try_advisory_xact_lock(jid)::int) FROM unnest($1) AS jid) != (SELECT count(*) FROM unnest($1)) THEN
        RAISE EXCEPTION 'Failed to convert one of the job session locks to a transaction lock';
    END IF;

    DELETE FROM acquired_jobs WHERE job_id = ANY($1);
    PERFORM pg_advisory_unlock(jid) FROM unnest($1) AS jid;
END
$$ LANGUAGE plpgsql;
