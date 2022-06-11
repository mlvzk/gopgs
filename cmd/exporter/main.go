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

			for rows.Next() {
				var queue string
				var count int
				if err := rows.Scan(&queue, &count); err != nil {
					panic(err)
				}

				jobsGauge.WithLabelValues(queue).Set(float64(count))
			}

			if err := rows.Err(); err != nil {
				panic(err)
			}

			rows.Close()

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}()

	log.Println("Starting server on :8080")
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":7688", nil)
}
