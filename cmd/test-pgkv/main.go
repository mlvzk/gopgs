package main

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mlvzk/gopgs/pgkv"
)

func main() {
	ctx := context.TODO()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		panic("DATABASE_URL is not set")
	}

	makeStore := func(ctx context.Context) *pgkv.Store {
		db, err := pgkv.New(ctx, databaseURL)
		if err != nil {
			panic(err)
		}

		return db
	}

	multipleGetOrSets(ctx, makeStore)
}

func multipleGetOrSets(ctx context.Context, makeStore func(context.Context) *pgkv.Store) {
	startTime := time.Now()

	winnerValue := -1
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			store := makeStore(ctx)
			defer store.Close()

			store.GetOrSet(ctx, "multipleGetOrSetsOneRunner", startTime, func() ([]byte, error) {
				random := rand.Int()
				winnerValue = random
				return []byte(strconv.Itoa(winnerValue)), nil
			})
		}(i)
	}

	wg.Wait()

	store := makeStore(ctx)
	defer store.Close()

	value, _, err := store.Get(ctx, "multipleGetOrSetsOneRunner")
	if err != nil {
		panic(err)
	}

	if string(value) != strconv.Itoa(winnerValue) {
		panic("Expected value to be " + strconv.Itoa(winnerValue) + " but was " + string(value))
	}
}
