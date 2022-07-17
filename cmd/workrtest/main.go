package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/mlvzk/gopgs/pgqueue"
	"github.com/mlvzk/gopgs/workr"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	queue, err := pgqueue.New("postgres://postgres@localhost")
	if err != nil {
		log.Fatal(err)
	}

	w := workr.ExampleWorkFeeder(HelloWorldWork)

	workers := map[string]workr.Worker{
		"HelloWorldWorker": w,
		"Welcomer": workr.QueueFeeder(queue, "welcomer", func(qc *workr.QueueContext) workr.Work[WelcomerArg] {
			return func(ctx context.Context, arg WelcomerArg) error {
				if err := Welcomer(ctx, arg); err != nil {
					if qc.ErrorCount == 0 {
						qc.SetRetryable(true)
					}

					return err
				}

				return nil
			}
		}),
		"RandomWelcomeEnqueuer": workr.ExampleWorkFeeder(RandomWelcomeEnqueuer(workr.Enqueue[WelcomerArg](queue, "welcomer"))),
	}

	var wg sync.WaitGroup
	for name, worker := range workers {
		wg.Add(1)

		go func(name string, worker func(context.Context) error) {
			defer wg.Done()

			if err := worker(ctx); err != nil {
				log.Printf("Worker %s failed: %v", name, err)
			}
		}(name, worker)
	}

	wg.Wait()
}

func HelloWorldWork(ctx context.Context, arg struct{}) error {
	log.Println("Hello World")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
	}

	return nil
}

type WelcomerArg struct {
	Name string
}

func Welcomer(ctx context.Context, arg WelcomerArg) error {
	log.Printf("Welcome %s", arg.Name)

	return nil
}

func RandomWelcomeEnqueuer(enqueue func(context.Context, []workr.JobForEnqueue[WelcomerArg]) error) func(context.Context, WelcomerArg) error {
	return func(ctx context.Context, arg WelcomerArg) error {
		time.Sleep(time.Second)
		return enqueue(ctx, []workr.JobForEnqueue[WelcomerArg]{{Arg: WelcomerArg{Name: "John"}}})
	}
}
