package pgqueue

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mlvzk/gopgs/internal/helper"
	"github.com/mlvzk/gopgs/migrate"

	_ "embed"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/valyala/gozstd"
)

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
	lockConn     *pgx.Conn
	db           *pgxpool.Pool
	lockConnLock sync.Mutex
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

	return &q, nil
}

//go:embed init-lock-conn.sql
var initLockConnSql string

func (q *Queue) initLockConn(ctx context.Context) error {
	_, err := q.lockConn.Exec(ctx, initLockConnSql)
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

		id, err := strconv.Atoi(strings.Split(entry.Name(), "-")[0])
		if err != nil {
			panic("migration files must be named like '<id>-<name>.sql'")
		}

		content, err := migrationFiles.ReadFile(filepath.Join("migrations", entry.Name()))
		if err != nil {
			panic("failed to read migration file '" + entry.Name() + "'")
		}

		migrations = append(migrations, migrate.Migration{
			Id:         id,
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
	allJobArgs := make([][]byte, len(jobs))
	for i, job := range jobs {
		allJobArgs[i] = job.Args
	}
	dictionary := gozstd.BuildDict(allJobArgs, 1024*1024)
	if dictionary == nil {
		dictionary = []byte{}
	}

	compress := func(data []byte) []byte {
		return gozstd.CompressLevel([]byte{}, data, 11)
	}
	if len(dictionary) != 0 {
		cdict, err := gozstd.NewCDictLevel(dictionary, 11)
		if err != nil {
			return nil, err
		}
		defer cdict.Release()

		compress = func(data []byte) []byte {
			return gozstd.CompressDict([]byte{}, data, cdict)
		}
	}

	now := time.Now()
	var valRows [][]any
	for _, job := range jobs {
		compressedArgs := compress(job.Args)
		valRows = append(valRows, []any{job.Queue, 0, now, job.RunAt, job.ExpiresAt, nil, compressedArgs, 0, "", false})
	}

	args, valuesSql := helper.GenerateValues(valRows, []string{"text", "integer", "timestamptz", "timestamptz", "timestamptz", "timestamptz", "bytea", "integer", "text", "boolean"})
	args = append(args, dictionary)

	rows, err := q.db.Query(ctx, `
		with vals as (
			values `+valuesSql+`
		), dict_to_insert as (
			select * from (values (0, $`+strconv.Itoa(len(args))+`::bytea)) as t(ref_count, data) where length(data) > 0
		), dict_id as (
			insert into pgqueue.dictionaries (ref_count, data) select * from dict_to_insert returning id
		), inserted_jobs as (
			insert into pgqueue.jobs (
				queue, priority, enqueued_at, run_at, expires_at, failed_at, args, error_count, last_error, retryable, dict_id
			) select *, (select id from dict_id) as dict_id from vals returning id
		) select id from inserted_jobs`, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	if rows.Err() != nil {
		panic(rows.Err())
	}

	return ids, nil
}

type JobResult struct {
	Id        int64
	Error     error
	Retryable bool
}

func (q *Queue) Finish(ctx context.Context, jobResults []JobResult) error {
	if len(jobResults) == 0 {
		return nil
	}

	var rows [][]any
	for _, job := range jobResults {
		var errText string
		if job.Error != nil {
			errText = job.Error.Error()
		}
		rows = append(rows, []any{job.Id, errText, job.Retryable})
	}

	args, valuesSql := helper.GenerateValues(rows, []string{"bigint", "text", "boolean"})

	_, err := q.db.Exec(ctx, `
		with results(id, error, retryable) as (
			values `+valuesSql+`
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

	return q.unlock(ctx, ids)
}

func (q *Queue) unlock(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	q.lockConnLock.Lock()
	defer q.lockConnLock.Unlock()

	_, err := q.lockConn.Exec(ctx, `
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
				err = fmt.Errorf("Recovered from panic in (*Queue).Work: %w", rErr)
			} else {
				err = fmt.Errorf("Recovered from panic in (*Queue).Work: %v", r)
			}
			jobResult = JobResult{Id: job.Id, Error: err}
		}
	}()

	return JobResult{Id: job.Id, Error: fn(job)}
}

func (q *Queue) Get(ctx context.Context, queue string, limit int) ([]Job, error) {
	q.lockConnLock.Lock()
	defer q.lockConnLock.Unlock()

	var jobs []Job

	rows, err := q.lockConn.Query(ctx, `
	SELECT id, queue, priority, enqueued_at, run_at, expires_at, failed_at, args, error_count, last_error, retryable, dict_id FROM pg_temp.acquire_jobs($1, $2)
	`, queue, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	uniqDictIds := make(map[int64]struct{}, 0)
	for rows.Next() {
		var job Job
		err := rows.Scan(&job.Id, &job.Queue, &job.Priority, &job.EnqueuedAt, &job.RunAt, &job.ExpiresAt, &job.FailedAt, &job.compressedArgs, &job.ErrorCount, &job.LastError, &job.Retryable, &job.dictId)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
		if job.dictId != nil {
			uniqDictIds[*job.dictId] = struct{}{}
		}
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	dictIds := make([]int64, 0, len(uniqDictIds))
	for id := range uniqDictIds {
		dictIds = append(dictIds, id)
	}

	rows, err = q.db.Query(ctx, `
		select id, data from pgqueue.dictionaries where id = ANY($1)
	`, dictIds)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	dictIdToDecomp := make(map[int64]func([]byte) ([]byte, error))
	for rows.Next() {
		var id int64
		var data []byte

		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}

		ddict, err := gozstd.NewDDict(data)
		if err != nil {
			return nil, err
		}
		defer ddict.Release()
		decompress := func(data []byte) ([]byte, error) {
			return gozstd.DecompressDict([]byte{}, data, ddict)
		}

		dictIdToDecomp[id] = decompress
	}

	for i := range jobs {
		job := &jobs[i]
		if job.dictId != nil {
			decompress := dictIdToDecomp[*job.dictId]
			job.decompressArgs = func() []byte {
				args, err := decompress(job.compressedArgs)
				if err != nil {
					panic(err)
				}

				return args
			}
		} else {
			job.decompressArgs = func() []byte {
				args, err := gozstd.Decompress([]byte{}, job.compressedArgs)
				if err != nil {
					panic(err)
				}

				return args
			}
		}
	}

	if rows.Err() != nil {
		return nil, err
	}

	return jobs, nil
}
