package pgqueue

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mlvzk/gopgs/internal/helper"
	"github.com/mlvzk/gopgs/migrate"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	_ "embed"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/valyala/gozstd"
)

func tracer() trace.Tracer {
	return otel.Tracer("github.com/mlvzk/gopgs/pgqueue")
}

type Job struct {
	JobWithoutId
	Id                 int64
	dictId             *int64
	compressedArgs     []byte
	decompressedArgs   []byte
	decompressArgsOnce sync.Once
	decompressArgs     func() []byte
}

func (job *Job) Args() []byte {
	job.decompressArgsOnce.Do(func() {
		job.decompressedArgs = job.decompressArgs()
	})
	return job.decompressedArgs
}

type JobWithoutId struct {
	Queue      string
	Priority   int
	EnqueuedAt time.Time
	RunAt      *time.Time
	ExpiresAt  *time.Time
	FailedAt   *time.Time
	ErrorCount int
	LastError  string
	Retryable  bool
}

type Queue struct {
	lockConn      *pgx.Conn
	db            *pgxpool.Pool
	lockConnLock  sync.Mutex
	cleanUpCancel context.CancelFunc
}

func New(url string) (*Queue, error) {
	db, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database: %w", err)
	}

	lockConn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database (lockConn): %w", err)
	}

	q := Queue{
		lockConn: lockConn,
		db:       db,
	}

	if err := q.runMigrations(context.Background()); err != nil {
		return nil, fmt.Errorf("Failed to run migrations: %w", err)
	}

	if err := q.initLockConn(context.Background()); err != nil {
		return nil, fmt.Errorf("Failed to init lock connection: %w", err)
	}

	cleanUpCtx, cleanUpCancel := context.WithCancel(context.Background())
	go func() {
		if err := q.cleanUp(cleanUpCtx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			panic(err)
		}
	}()

	q.cleanUpCancel = cleanUpCancel

	return &q, nil
}

func (q *Queue) Close(ctx context.Context) error {
	q.cleanUpCancel()

	conn, release := q.getLockConn(ctx)
	defer release()
	conn.Close(ctx)

	q.db.Close()

	return nil
}

//go:embed init-lock-conn.sql
var initLockConnSql string

func (q *Queue) initLockConn(ctx context.Context) error {
	conn, release := q.getLockConn(ctx)
	defer release()

	_, err := conn.Exec(ctx, initLockConnSql)
	return err
}

//go:embed migrations
var migrationFiles embed.FS

func (q *Queue) runMigrations(ctx context.Context) error {
	migrations := make([]migrate.Migration, 0)
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			panic("migration files can't be directories")
		}

		version, err := strconv.Atoi(strings.Split(entry.Name(), "-")[0])
		if err != nil {
			panic("migration files must be named like '<id>-<name>.sql'")
		}

		content, err := migrationFiles.ReadFile(filepath.Join("migrations", entry.Name()))
		if err != nil {
			panic("failed to read migration file '" + entry.Name() + "'")
		}

		migrations = append(migrations, migrate.Migration{
			Version:    version,
			Statements: string(content),
		})
	}

	conn, err := q.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer conn.Release()

	return migrate.Migrate(ctx, conn.Conn(), "pgqueue", migrations)
}

type JobForEnqueue struct {
	Queue     string
	RunAt     *time.Time
	ExpiresAt *time.Time
	Priority  int16
	Args      []byte
}

func (q *Queue) Enqueue(ctx context.Context, jobs []JobForEnqueue) ([]int64, error) {
	ctx, span := tracer().Start(ctx, "Enqueue")
	defer span.End()

	span.SetAttributes(attribute.Int("jobs_length", len(jobs)))

	allJobArgs := make([][]byte, len(jobs))
	allJobArgsLen := 0
	for i, job := range jobs {
		allJobArgs[i] = job.Args
		allJobArgsLen += len(job.Args)
	}
	avgJobArgsLen := allJobArgsLen / len(jobs)
	span.SetAttributes(attribute.Int("avg_job_args_length", avgJobArgsLen))
	maxDictionarySize := avgJobArgsLen * 2
	dictionary := makeDictionary(ctx, allJobArgs, maxDictionarySize)

	var cdict *gozstd.CDict
	if len(dictionary) > 0 {
		cd, err := gozstd.NewCDictLevel(dictionary, 11)
		if err != nil {
			return nil, fmt.Errorf("Failed to create a zstd cdict: %w", err)
		}
		cdict = cd
	}

	jobsWithCompressedArgs, err := compressJobs(ctx, jobs, cdict)
	if err != nil {
		return nil, fmt.Errorf("Failed to compress jobs: %w", err)
	}

	now := time.Now()
	args, valsSelect := helper.GenerateSelect(jobsWithCompressedArgs, func(t *jobWithCompressedArgs, cols *helper.ColumnSetter) {
		cols.Set("queue", "text", func() any { return t.Queue })
		cols.Set("priority", "smallint", func() any { return t.Priority })
		cols.Set("enqueued_at", "timestamptz", func() any { return now })
		cols.Set("run_at", "timestamptz", func() any { return t.RunAt })
		cols.Set("expires_at", "timestamptz", func() any { return t.ExpiresAt })
		cols.Set("args", "bytea", func() any { return t.compressedArgs })
	})
	args = append(args, dictionary)
	dictArg := strconv.Itoa(len(args))

	ctx, span = tracer().Start(ctx, "Query")
	defer span.End()

	rows, err := q.db.Query(ctx, `
		with vals as (
			`+valsSelect+`
		), dict_to_insert as (
			select * from (values ($`+dictArg+`::bytea)) as t(data) where length(data) > 0
		), dict_id as (
			insert into pgqueue.dictionaries (data) select * from dict_to_insert returning id
		), inserted_jobs as (
			insert into pgqueue.jobs (
				queue, priority, enqueued_at, run_at, expires_at, failed_at, args, error_count, last_error, retryable, dict_id
			) select queue, priority, enqueued_at, run_at, expires_at, null, args, 0, '', false, (select id from dict_id) as dict_id from vals returning id
		) select id from inserted_jobs`, args...)
	if err != nil {
		return nil, fmt.Errorf("Failed to insert jobs: %w", err)
	}

	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("Failed to scan id: %w", err)
		}

		ids = append(ids, id)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("rows.Err(): %w", err)
	}

	return ids, nil
}

func makeDictionary(ctx context.Context, samples [][]byte, maxDictionarySize int) []byte {
	_, span := tracer().Start(ctx, "makeDictionary")
	defer span.End()

	var dictionary []byte
	if len(samples) > 1 {
		dictionary = gozstd.BuildDict(samples, maxDictionarySize)
		span.SetAttributes(attribute.Int("dictionary_length", len(dictionary)))
	}
	if dictionary == nil {
		dictionary = []byte{}
	}

	return dictionary
}

type jobWithCompressedArgs struct {
	JobForEnqueue
	compressedArgs []byte
}

func compressJobs(ctx context.Context, jobs []JobForEnqueue, cdict *gozstd.CDict) ([]jobWithCompressedArgs, error) {
	ctx, span := tracer().Start(ctx, "compressJobs")
	defer span.End()

	jobsWithCompressedArgs := make([]jobWithCompressedArgs, 0, len(jobs))
	zstdWriter := gozstd.NewWriter(io.Discard)
	defer zstdWriter.Release()
	for i, job := range jobs {
		compressedArgs, err := compressZstd(job.Args, zstdWriter, cdict)
		if err != nil {
			return nil, fmt.Errorf("Failed to compress args of job %d: %w", i, err)
		}
		jobsWithCompressedArgs = append(jobsWithCompressedArgs, jobWithCompressedArgs{
			JobForEnqueue:  job,
			compressedArgs: compressedArgs,
		})
	}

	compressedArgsLen := 0
	for _, job := range jobsWithCompressedArgs {
		compressedArgsLen += len(job.compressedArgs)
	}
	avgCompressedArgsLen := compressedArgsLen / len(jobsWithCompressedArgs)
	span.SetAttributes(attribute.Int("avg_compressed_args_length", avgCompressedArgsLen))

	return jobsWithCompressedArgs, nil
}

func compressZstd(data []byte, writer *gozstd.Writer, cdict *gozstd.CDict) ([]byte, error) {
	out := bytes.NewBuffer([]byte{})

	writer.ResetWriterParams(out, &gozstd.WriterParams{
		Dict:             cdict,
		CompressionLevel: 11,
		WindowLog:        27,
	})

	defer writer.Close()

	_, err := writer.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write data to gozstd writer: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gozstd writer: %w", err)
	}

	return out.Bytes(), nil
}

type JobResult struct {
	Id        int64
	Error     error
	Retryable bool
}

func (q *Queue) Finish(ctx context.Context, jobResults []JobResult) error {
	ctx, span := tracer().Start(ctx, "Finish")
	defer span.End()
	span.SetAttributes(attribute.Int("job_results_length", len(jobResults)))

	args, resultsSelect := helper.GenerateSelect(jobResults, func(t *JobResult, cols *helper.ColumnSetter) {
		cols.Set("id", "bigint", func() any { return t.Id })
		cols.Set("error", "text", func() any {
			if t.Error == nil {
				return ""
			}

			return t.Error.Error()
		})
		cols.Set("retryable", "boolean", func() any { return t.Retryable })
	})

	_, err := q.db.Exec(ctx, `
		with results as (
			`+resultsSelect+`
		), results_for_deletion as (
			select id from results where error = ''
		), results_for_update as (
			select * from results where error != ''
		), deletion as (
			delete from pgqueue.jobs using results_for_deletion where pgqueue.jobs.id = results_for_deletion.id
		)
		update pgqueue.jobs set last_error = results_for_update.error, retryable = results_for_update.retryable, error_count = error_count + 1, priority = priority - 1, failed_at = now() from results_for_update where pgqueue.jobs.id = results_for_update.id
	`, args...)
	if err != nil {
		return err
	}

	ids := make([]int64, len(jobResults))
	for i, res := range jobResults {
		ids[i] = res.Id
	}

	if err := q.unlock(ctx, ids); err != nil {
		return fmt.Errorf("Failed to unlock jobs: %w", err)
	}

	return nil
}

func (q *Queue) unlock(ctx context.Context, ids []int64) error {
	ctx, span := tracer().Start(ctx, "unlock")
	defer span.End()

	conn, release := q.getLockConn(ctx)
	defer release()

	_, err := conn.Exec(ctx, `
		select pg_temp.unlock_jobs($1)
	`, ids)
	if err != nil {
		return err
	}

	return nil
}

// Work runs your function `fn`, returning a JobResult.
// If your function returns an error or panics, the JobResult.Error field will be set.
//
// You have to call (*Queue).Finish with the returned JobResult.
//
// This function never panics.
func Work(ctx context.Context, job *Job, fn func(*Job) error) (jobResult JobResult) {
	defer func() {
		if r := recover(); r != nil {
			var err error
			if rErr, ok := r.(error); ok {
				err = fmt.Errorf("Recovered from panic in pgqueue.Work: %w", rErr)
			} else {
				err = fmt.Errorf("Recovered from panic in pgqueue.Work: %v", r)
			}
			jobResult = JobResult{Id: job.Id, Error: err}
		}
	}()

	return JobResult{Id: job.Id, Error: fn(job)}
}

func (q *Queue) Get(ctx context.Context, queue string, limit int) ([]Job, error) {
	ctx, span := tracer().Start(ctx, "Get")
	defer span.End()
	span.SetAttributes(attribute.String("queue", queue), attribute.Int("limit", limit))

	jobs, err := q.acquireJobs(ctx, queue, limit)
	if err != nil {
		return nil, fmt.Errorf("Failed to acquire jobs: %w", err)
	}

	uniqDictIds := make(map[int64]struct{}, 0)
	for i := range jobs {
		if jobs[i].dictId != nil {
			uniqDictIds[*jobs[i].dictId] = struct{}{}
		}
	}
	dictIds := make([]int64, 0, len(uniqDictIds))
	for id := range uniqDictIds {
		dictIds = append(dictIds, id)
	}

	dictionaries, err := q.getDictionaries(ctx, dictIds)
	if err != nil {
		return nil, fmt.Errorf("Failed to get dictionaries: %w", err)
	}

	dictIdToDDict := make(map[int64]*gozstd.DDict, len(dictionaries))
	for _, dict := range dictionaries {
		ddict, err := gozstd.NewDDict(dict.data)
		if err != nil {
			return nil, fmt.Errorf("Failed to create DDict: %w", err)
		}
		dictIdToDDict[dict.id] = ddict
	}

	for i := range jobs {
		job := &jobs[i]
		if job.dictId != nil {
			ddict := dictIdToDDict[*job.dictId]
			job.decompressArgs = func() []byte {
				args, err := gozstd.DecompressDict([]byte{}, job.compressedArgs, ddict)
				if err != nil {
					panic(fmt.Errorf("Failed to decompress args with dictionary for job %d: %w", job.Id, err))
				}

				return args
			}
		} else {
			job.decompressArgs = func() []byte {
				args, err := gozstd.Decompress([]byte{}, job.compressedArgs)
				if err != nil {
					panic(fmt.Errorf("Failed to decompress args for job %d: %w", job.Id, err))
				}

				return args
			}
		}
	}

	return jobs, nil
}

type dictionary struct {
	id   int64
	data []byte
}

func (q *Queue) getDictionaries(ctx context.Context, dictIds []int64) ([]dictionary, error) {
	ctx, span := tracer().Start(ctx, "getDictionaries")
	defer span.End()

	rows, err := q.db.Query(ctx, `
		select id, data from pgqueue.dictionaries where id = ANY($1)
	`, dictIds)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	dicts := make([]dictionary, 0)
	for rows.Next() {
		var id int64
		var data []byte

		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}

		dicts = append(dicts, dictionary{
			id:   id,
			data: data,
		})
	}

	if rows.Err() != nil {
		return nil, err
	}

	return dicts, nil
}

func (q *Queue) acquireJobs(ctx context.Context, queue string, limit int) ([]Job, error) {
	ctx, span := tracer().Start(ctx, "acquireJobs")
	defer span.End()

	conn, release := q.getLockConn(ctx)
	defer release()

	rows, err := conn.Query(ctx, `
	SELECT id, queue, priority, enqueued_at, run_at, expires_at, failed_at, args, error_count, last_error, retryable, dict_id FROM pg_temp.acquire_jobs($1, $2)
	`, queue, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		err := rows.Scan(&job.Id, &job.Queue, &job.Priority, &job.EnqueuedAt, &job.RunAt, &job.ExpiresAt, &job.FailedAt, &job.compressedArgs, &job.ErrorCount, &job.LastError, &job.Retryable, &job.dictId)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return jobs, nil
}

type Statistics struct {
	JobsCount       int
	LockedJobsCount int
}

func (q *Queue) Statistics(ctx context.Context) (map[string]*Statistics, error) {
	rows, err := q.db.Query(ctx, `
	SELECT queue, count(*)
	FROM pgqueue.jobs AS j
	WHERE (run_at IS NULL OR run_at <= now())
	AND (expires_at IS NULL OR now() < expires_at)
	AND (failed_at IS NULL OR retryable = true)
	GROUP BY queue
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to send a query: %w", err)
	}

	defer rows.Close()

	stats := make(map[string]*Statistics)
	for rows.Next() {
		var queue string
		var count int
		if err := rows.Scan(&queue, &count); err != nil {
			return nil, err
		}

		stats[queue] = &Statistics{JobsCount: count}
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("failed during reading: %w", rows.Err())
	}

	conn, release := q.getLockConn(ctx)
	defer release()

	rows, err = conn.Query(ctx, `
	SELECT queue, count(*)
	FROM acquired_jobs
	GROUP BY queue
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to send a query with lockConn: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var queue string
		var count int
		if err := rows.Scan(&queue, &count); err != nil {
			return nil, err
		}

		if _, exists := stats[queue]; !exists {
			stats[queue] = &Statistics{}
		}
		stats[queue].LockedJobsCount = count
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("failed during reading a lockConn query: %w", rows.Err())
	}

	return stats, nil
}

//go:embed clean-up-jobs.sql
var cleanUpJobsSql string

//go:embed clean-up-dictionaries.sql
var cleanUpDictionariesSql string

func (q *Queue) cleanUp(ctx context.Context) error {
	for {
		locked, err := q.cleanUpTryLock(ctx)
		if err != nil {
			return fmt.Errorf("failed to lock: %w", err)
		}

		if locked {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute * 5):
		}
	}
	defer q.cleanUpUnlock(ctx)

	for {
		if _, err := q.db.Exec(ctx, cleanUpJobsSql); err != nil {
			return fmt.Errorf("failed to execute clean up jobs SQL: %w", err)
		}

		if _, err := q.db.Exec(ctx, cleanUpDictionariesSql); err != nil {
			return fmt.Errorf("failed to execute clean up dictionaries SQL: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Hour):
		}
	}
}

func (q *Queue) cleanUpTryLock(ctx context.Context) (bool, error) {
	conn, release := q.getLockConn(ctx)
	defer release()

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('pgqueue_clean_up_lock'))`).Scan(&locked); err != nil {
		return false, err
	}

	return locked, nil
}

func (q *Queue) cleanUpUnlock(ctx context.Context) error {
	conn, release := q.getLockConn(ctx)
	defer release()

	var unlocked bool
	if err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext('pgqueue_clean_up_lock'))`).Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return fmt.Errorf("lock was not owned")
	}

	return nil
}

func (q *Queue) getLockConn(ctx context.Context) (lockConn *pgx.Conn, release func()) {
	_, lockWaitSpan := tracer().Start(ctx, "waiting for lockConnLock")
	q.lockConnLock.Lock()
	lockWaitSpan.End()

	_, lockHoldingSpan := tracer().Start(ctx, "holding lockConnLock")
	return q.lockConn, func() {
		q.lockConnLock.Unlock()
		lockHoldingSpan.End()
	}
}
