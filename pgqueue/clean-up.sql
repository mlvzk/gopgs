DELETE FROM pgqueue.jobs WHERE NOT (
    (run_at IS NULL OR run_at <= now())
	AND (expires_at IS NULL OR now() < expires_at)
	AND (failed_at IS NULL OR retryable = true)
) AND (failed_at < now() - INTERVAL '1 hour')
	AND (expires_at IS NULL OR expires_at < now() - INTERVAL '1 hour')
	AND id NOT IN (SELECT (classid::bigint << 32) + objid::bigint FROM pg_locks WHERE locktype = 'advisory');
