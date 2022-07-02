package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgqUrl := os.Getenv("PGQUEUE_URL")
	if pgqUrl == "" {
		panic("PGQUEUE_URL is not set")
	}

	db, err := pgxpool.Connect(ctx, pgqUrl)
	if err != nil {
		panic(err)
	}

	jobsGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "pgqueue",
		Name:      "jobs",
	}, []string{"queue"})

	prometheus.MustRegister(jobsGauge)

	go func() {
		queues := map[string]struct{}{}

		for {
			rows, err := db.Query(ctx, `
			SELECT queue, count(*)
			FROM pgqueue.jobs AS j
			WHERE (run_at IS NULL OR run_at <= now())
			AND (expires_at IS NULL OR now() < expires_at)
			AND (failed_at IS NULL OR retryable = true)
			GROUP BY queue
			`)
			if err != nil {
				panic(err)
			}

			queueToCount := map[string]int{}
			for rows.Next() {
				var queue string
				var count int
				if err := rows.Scan(&queue, &count); err != nil {
					panic(err)
				}

				queueToCount[queue] = count
			}

			if err := rows.Err(); err != nil {
				panic(err)
			}

			rows.Close()

			for queue := range queues {
				if _, ok := queueToCount[queue]; !ok {
					queueToCount[queue] = 0
				}
			}

			for queue, count := range queueToCount {
				queues[queue] = struct{}{}
				jobsGauge.WithLabelValues(queue).Set(float64(count))
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}()

	log.Println("Starting server on :7688")
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":7688", nil)
}
