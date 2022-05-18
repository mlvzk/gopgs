CREATE INDEX jobs_acquire_jobs_idx ON pgqueue.jobs (queue, run_at, expires_at, failed_at, retryable, priority);
