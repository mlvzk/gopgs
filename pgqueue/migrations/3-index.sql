DROP INDEX pgqueue.jobs_acquire_jobs_idx;
CREATE INDEX jobs_acquire_jobs_idx ON pgqueue.jobs (queue, priority DESC, run_at NULLS LAST, expires_at NULLS LAST, id);
