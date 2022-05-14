CREATE TEMPORARY TABLE acquired_jobs (job_id bigint not null, queue text not null);

CREATE FUNCTION pg_temp.acquire_jobs(text, int) RETURNS SETOF pgqueue.jobs AS $$
DECLARE
    job pgqueue.jobs%rowtype;
    counter int;
BEGIN
    counter = 0;
    FOR job IN
        SELECT j.*
        FROM pgqueue.jobs AS j
        WHERE queue = $1
        AND NOT(id = ANY(SELECT job_id FROM acquired_jobs WHERE queue = $1))
        AND (run_at IS NULL OR run_at <= now())
        AND (expires_at IS NULL OR now() < expires_at)
        AND (failed_at IS NULL OR retryable = true)
        ORDER BY priority DESC, run_at NULLS LAST, expires_at NULLS LAST, id
    LOOP
        IF pg_try_advisory_lock(job.id) THEN
            counter = counter + 1;
            INSERT INTO acquired_jobs VALUES (job.id, job.queue);
            RETURN NEXT job;
        END IF;
        IF counter >= $2 THEN
            RETURN;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;	

CREATE FUNCTION pg_temp.unlock_jobs(bigint[]) RETURNS void AS $$
DECLARE
    jid bigint;
BEGIN
    IF (SELECT count(*) FROM acquired_jobs WHERE job_id = ANY($1)) != (SELECT count(*) FROM unnest($1)) THEN
        RAISE EXCEPTION 'You do not have a lock for one of the pgqueue.jobs';
    END IF;

    FOR jid IN SELECT unnest($1)
    LOOP
        IF pg_advisory_unlock(jid) THEN
            DELETE FROM acquired_jobs aj WHERE aj.job_id = jid;
        ELSE
            RAISE EXCEPTION 'Failed to unlock job id %', jid;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;
