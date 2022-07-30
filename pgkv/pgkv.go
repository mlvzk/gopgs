package pgkv

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BurntSushi/locker"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mlvzk/gopgs/migrate"
)

type Store struct {
	db             *pgxpool.Pool
	lockConn       *pgxpool.Conn
	lockConnLock   sync.Mutex
	channels       sync.Map
	cancelListener context.CancelFunc
	keyLock        *locker.Locker
}

//go:embed init-conn.sql
var initConnSql string

func New(ctx context.Context, connStr string) (*Store, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connStr: %w", err)
	}

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	if err := db.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		if err := runMigrations(ctx, c.Conn()); err != nil {
			return fmt.Errorf("failed to run migrations: %w", err)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to acquire connection or run migrations: %w", err)
	}

	lockConn, err := db.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock connection: %w", err)
	}

	if _, err := lockConn.Exec(ctx, initConnSql); err != nil {
		return nil, fmt.Errorf("failed to run initConnSql: %w", err)
	}

	listenConn, err := db.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire listen connection: %w", err)
	}

	cancelCtx, cancelListener := context.WithCancel(context.Background())
	store := &Store{db: db, lockConn: lockConn, cancelListener: cancelListener, keyLock: locker.NewLocker()}

	err = store.listenToStoreUpdate(ctx, listenConn)
	if err != nil {
		return nil, fmt.Errorf("failed to listen to store update: %w", err)
	}

	go func() {
		err := store.storeUpdateListener(cancelCtx, listenConn)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			panic("pgkv listener failed: " + err.Error())
		}
	}()

	return store, nil
}

func (s *Store) Close() {
	s.cancelListener()
	s.lockConn.Release()
	s.db.Close()
}

//go:embed migrations/*
var migrations embed.FS

func runMigrations(ctx context.Context, conn *pgx.Conn) error {
	migrations, err := migrate.EmbedFsToMigrations(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to convert migration files to migrations: %w", err)
	}

	return migrate.Migrate(ctx, conn, "pgkv", migrations)
}

func (s *Store) listenToStoreUpdate(ctx context.Context, listenConn *pgxpool.Conn) error {
	if _, err := listenConn.Exec(ctx, `LISTEN pgkv_store_update`); err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	return nil
}

func (s *Store) storeUpdateListener(ctx context.Context, listenConn *pgxpool.Conn) error {
	defer listenConn.Release()

	conn := listenConn.Conn()
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("failed to wait for notification: %w", err)
		}

		if value, loaded := s.channels.LoadAndDelete(notification.Payload); loaded {
			close(value.(chan struct{}))
		}
	}
}

func (s *Store) listenForStoreUpdate(key string) <-chan struct{} {
	ch, _ := s.channels.LoadOrStore(key, make(chan struct{}))
	return ch.(chan struct{})
}

type getOrLockFound struct {
	value      []byte
	compressed bool
}

func (s *Store) getOrLock(ctx context.Context, key string, oldest time.Time) (result *getOrLockFound, locked bool, err error) {
	s.lockConnLock.Lock()
	defer s.lockConnLock.Unlock()

	var found bool
	var value *[]byte
	var compressed *bool
	row := s.lockConn.QueryRow(ctx, `SELECT found, locked, (c.kv::pgkv.store).value, (c.kv::pgkv.store).compressed FROM pg_temp.get_or_lock($1, $2) c`, key, oldest)
	if err := row.Scan(&found, &locked, &value, &compressed); err != nil {
		return nil, false, fmt.Errorf("failed to scan value: %w", err)
	}

	if !found {
		return nil, locked, nil
	}

	return &getOrLockFound{
		value:      *value,
		compressed: *compressed,
	}, locked, nil
}

// GetOrSet returns the value associated with the key.
// If the key does not exist,
// the function fn is called to generate the value.
// The value is stored in the database and returned.
//
// If fn returns an error, GetOrSet returns that error.
//
// If the value is older than argument `refreshOlderThan`,
// fn is called and the value is updated.
//
// If you don't care about updating old values,
// pass time.Time{}
func (s *Store) GetOrSet(ctx context.Context, key string, refreshOlderThan time.Time, fn func() ([]byte, error)) (retVal []byte, retErr error) {
	s.keyLock.Lock(key)
	defer s.keyLock.Unlock(key)

start:
	storeUpdateCh := s.listenForStoreUpdate(key)
	res, locked, err := s.getOrLock(ctx, key, refreshOlderThan)
	if err != nil {
		return nil, fmt.Errorf("failed to get or lock: %w", err)
	}

	if locked {
		var ownsLockConn bool
		defer func() {
			if retErr != nil {
				if !ownsLockConn {
					s.lockConnLock.Lock()
					defer s.lockConnLock.Unlock()
				}

				s.lockConn.Exec(ctx, `SELECT pg_temp.unlock_get_or_lock($1)`, key)
			}
		}()

		val, err := fn()
		if err != nil {
			return nil, fmt.Errorf("failed to call fn: %w", err)
		}

		s.lockConnLock.Lock()
		ownsLockConn = true
		defer func() {
			s.lockConnLock.Unlock()
			ownsLockConn = false
		}()

		compressed := false
		if _, err := s.lockConn.Exec(ctx, `SELECT pg_temp.set_and_unlock($1, $2, $3)`, key, val, compressed); err != nil {
			return nil, fmt.Errorf("failed to set and unlock: %w", err)
		}

		return val, nil
	}

	if res == nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second * 10):
			goto start
		case <-storeUpdateCh:
			goto start
		}
	}

	if res.compressed {
		panic("compression is not implemented")
	}

	return res.value, nil
}

func (s *Store) Get(ctx context.Context, key string) (_ []byte, found bool, _ error) {
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to acquire connection: %w", err)
	}

	defer conn.Release()

	row := conn.QueryRow(ctx, `SELECT value, compressed FROM pgkv.store WHERE key = $1`, key)
	var value []byte
	var compressed bool
	if err := row.Scan(&value, &compressed); err == nil {
		if compressed {
			panic("compression is not implemented")
		}
		return value, true, nil
	} else if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	} else {
		return nil, false, fmt.Errorf("failed to scan value: %w", err)
	}
}
