DELETE
FROM pgqueue.jobs
WHERE
	(
		(
			expires_at IS NOT NULL
			AND expires_at < now() - INTERVAL '24 hour'
		)
		OR
		(
			failed_at IS NOT NULL
			AND failed_at < now() - INTERVAL '24 hour'
			AND retryable = false
		)
	)
	AND id NOT IN (
		SELECT (classid::bigint << 32) + objid::bigint FROM pg_locks WHERE locktype = 'advisory'
	);
