package workr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mlvzk/gopgs/pgqueue"
)

type Work[Arg any] func(context.Context, Arg) error

func ExampleFnWorker(ctx context.Context, arg struct{}) error {
	return nil
}

var _ = Work[struct{}](ExampleFnWorker)

type Worker func(context.Context) error

func ExampleWorkFeeder[Arg any](work Work[Arg]) Worker {
	return func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var arg Arg
				if err := work(ctx, arg); err != nil {
					return err
				}
			}
		}
	}
}

type QueueContext struct {
	SetRetryable func(bool)
	ErrorCount   int
}

func QueueFeeder[Arg any](queue *pgqueue.Queue, queueName string, work func(*QueueContext) Work[Arg]) Worker {
	return func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			jobs, err := queue.Get(ctx, queueName, 1)
			if err != nil {
				return fmt.Errorf("Failed to get jobs from queue %s: %w", queueName, err)
			}

			jobResults := []pgqueue.JobResult{}
			for _, job := range jobs {
				retryable := false
				result := pgqueue.Work(ctx, &job, func(job *pgqueue.Job) error {
					var arg Arg
					if err := json.Unmarshal(job.ArgsNoCache(), &arg); err != nil {
						return fmt.Errorf("Failed to unmarshal job args: %w", err)
					}

					return work(&QueueContext{
						SetRetryable: func(r bool) {
							retryable = r
						},
						ErrorCount: job.ErrorCount,
					})(ctx, arg)
				})
				result.Retryable = retryable

				jobResults = append(jobResults, result)
			}

			if err := queue.Finish(ctx, jobResults); err != nil {
				return fmt.Errorf("Failed to finish jobs in queue %s: %w", queueName, err)
			}

			if len(jobResults) == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
		}
	}
}

type JobForEnqueue[Arg any] struct {
	// JobForEnqueue's Args will be overwritten
	pgqueue.JobForEnqueue
	Arg Arg
}

func Enqueue[Arg any](queue *pgqueue.Queue, queueName string) func(context.Context, []JobForEnqueue[Arg]) error {
	return func(ctx context.Context, jobs []JobForEnqueue[Arg]) error {
		pgqueueJobs := []pgqueue.JobForEnqueue{}
		for _, job := range jobs {
			args, err := json.Marshal(job.Arg)
			if err != nil {
				return fmt.Errorf("Failed to marshal job args: %w", err)
			}

			pgqueueJob := job.JobForEnqueue
			pgqueueJob.Args = args
		}

		_, err := queue.Enqueue(ctx, pgqueueJobs)
		return err
	}
}
