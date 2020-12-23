// Package dbmutex implements a DB-based mutex that can be used to synchronize work
// among multiple processes.
//
// Features
//
// - minimal dependencies
//
// - works with mysql and postgres
//
// - works with database failovers
//
// - mutex lock and unlock operations can timeout if desired
//
// - callers can be notified when mutexes are unlocked
//
// Usage
//
// See the examples.
package dbmutex

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
	"github.com/dynata/go-dbmutex/driver"
	"github.com/google/uuid"
)

const (
	// DefaultMutexTableName is the default table name that will be used for storing mutexes unless overridden
	// via WithMutexTableName.
	DefaultMutexTableName = "dbmutex"

	// DefaultMutexName is the default mutex name that will be used unless overridden via WithMutexName. A unique
	// mutex name should be used for each locking scenario. It is advised to always use WithMutexName.
	DefaultMutexName = "mutex"

	// DefaultRefresh is the default time to wait between refreshing locked Mutexes. See WithRefresh.
	DefaultRefresh = 1 * time.Second

	// DefaultExpiration is the default time after which a mutex will expire if it has not been automatically
	// refreshed.
	DefaultExpiration = 5 * time.Minute

	// DefaultPollInterval is the default time that will be used to poll a locked mutex when attempting to
	// lock a mutex. Override it via WithPollInterval
	DefaultPollInterval = 1 * time.Second
)

// A Mutex is used to provide mutual exclusion among multiple processes that have access to a shared
// database.  In order to provide mutual exclusion, the same underlying database table and mutex name
// (which ties to a row) should be used.  (See WithMutexTableName and WithMutexName.) Multiple mutexes can
// be stored in the same table but different names should be used for different application specific locking
// scenarios.
//
// Behaviour can be be customized via the MutexOption parameters passed to New and the LockOption parameters
// passed to Lock.
type Mutex struct {
	options        *mutexOptions
	db             *sql.DB
	lock           *sync.Mutex
	quitRefresh    chan struct{}
	doneRefreshing <-chan struct{}
	hostname       string
	pid            int
	lockerId       string
}

// An Identity uniquely identifies a Mutex. Note that two different Mutex Identities can still be used for
// mutual exclusion because only TableName, MutexName and DB server are used when determining exclusion.
// The other data elements like Hostname, Pid and LockerId are additional information.
type Identity struct {
	TableName string
	MutexName string
	Hostname  string
	Pid       int
	LockerId  string
}

// LogError is an ErrorNotifier that simply logs errors using the standard logger.
func LogError(e error) error {
	log.Print(e.Error())
	return nil
}

// IgnoreLogError is an ErrorNotifier that does nothing with passed errors.
func IgnoreLogError(error) error {
	return nil
}

// An ErrorNotifier is called to notify when an error occurs while locking, unlocking and refreshing Mutexes.
// The function should typically return null so that the retries will occur. However, if the function returns
// non-nil, then the calling code will exit any retry loop. Normally, an ErrorNotifier can be used to
// simply log the fact that a transient error occurred and then return nil.  See WithErrorNotifier.
type ErrorNotifier func(error) error

// The Lock operation attempts to acquire the Mutex by updating the "locked" column in the underlying database
// table. If unable to update the row, Lock will poll attempting to acquire the mutex by updating the row.  Any database
// related errors that occur during Lock are reported via an ErrorNotifier (see WithErrorNotifier)
// but are typically ignored in order to complete the Lock operation.  Lock can be timed out by using
// a ctx parameter that has a deadline (see examples). The poll interval and ability to fail fast after a
// database error can be controlled via MutexOption passed to New. If the mutex is acquired, a nil error and a context
// that expires when the Mutex is unlocked are returned. Because polling is used even after database generated
// errors, Lock will typically not return unless the Mutex is acquired or the ctx expires.
func (m *Mutex) Lock(ctx context.Context) (context.Context, error) {
	if m == nil || m.db == nil {
		return nil, dbmerr.NewMutexUninitializedError()
	}

	// pollChan will be used to repeatedly try to acquire the lock. It is only initialized after we have failed
	// once to acquire the lock. Select immediately returns for closed channels so we use a closed channel during
	// the first loop iteration.
	pollChan := getClosedTimeChan()
	var ticker *time.Ticker
Loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pollChan:
			lockAcquired, err := m.options.driver.Lock(
				ctx,
				m.db,
				m.options.tableName,
				m.options.mutexName,
				m.hostname,
				m.pid,
				m.lockerId,
				m.options.refresh,
				m.options.expiration,
			)
			if lockAcquired {
				break Loop
			}
			if m.options.failFast {
				return nil, dbmerr.NewLockFailFastError(m.options.tableName, m.options.mutexName, err)
			}
			if err != nil {
				err2 := dbmerr.NewLockError(m.options.tableName, m.options.mutexName, err)
				if m.options.lockErrNotifier(err2) != nil {
					return nil, err2
				}
			}
			if ticker == nil {
				ticker = time.NewTicker(m.options.pollInterval)
				// This defer inside a for loop is ok because we will only defer once.
				defer ticker.Stop()
				pollChan = ticker.C
			}
		}
	}

	// If we are here, then we have acquired db level lock.
	// Acquire our own sync.Mutex so we can update internal state.
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.quitRefresh != nil {
		// This should not happen unless the Driver has some issues.
		return nil, dbmerr.NewDriverAllowedConcurrentLocksError(m.options.tableName, m.options.mutexName)
	}

	m.quitRefresh = make(chan struct{})
	lockCtx, lockCanceledFunc := context.WithCancel(context.Background())
	m.doneRefreshing = goRefreshLock(
		m.options.driver,
		m.db,
		m.options.tableName,
		m.options.mutexName,
		m.hostname,
		m.pid,
		m.lockerId,
		m.options.refresh,
		m.options.expiration,
		m.quitRefresh,
		time.Now,
		m.options.refreshErrNotifier,
		lockCanceledFunc,
	)
	return lockCtx, nil
}

// The Unlock operation attempts to release the Mutex by updating the "locked" column in the underlying database
// table. If unable to update the row, Lock will poll attempting to release the mutex by updating the row.  Any database
// related errors that occur during Unlock are reported via an ErrorNotifier (see WithErrorNotifier)
// but are typically ignored in order to complete the Unlock operation.  Unlock can be timed out by using
// a ctx parameter that has a deadline (see examples). The poll interval and ability to fail fast after a
// database error can be controlled via MutexOption passed to New. Because polling is used even after database generated
// errors, Unlock will typically not return unless the Mutex is released or the ctx expires.
func (m *Mutex) Unlock(ctx context.Context) error {
	if m == nil || m.db == nil {
		return dbmerr.NewMutexUninitializedError()
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	if m.quitRefresh == nil {
		return dbmerr.NewNotLockedError(m.options.tableName, m.options.mutexName)
	}

	close(m.quitRefresh)
	<-m.doneRefreshing
	m.quitRefresh = nil
	m.doneRefreshing = nil

	// pollChan will be used to repeatedly try to release the lock. It is only initialized after we have failed
	// once to release the lock. Select immediately returns for closed channels so we use a closed channel during
	// the first loop iteration.
	pollChan := getClosedTimeChan()
	var ticker *time.Ticker
Loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollChan:
			_, err := m.options.driver.Unlock(
				ctx,
				m.db,
				m.options.tableName,
				m.options.mutexName,
				m.hostname,
				m.pid,
				m.lockerId,
			)
			if err == nil {
				// If there was no error then we assume lock was either unlocked or we were no longer
				// the owner. In both cases, we consider the lock to be unlocked.
				break Loop
			}
			if m.options.failFast {
				return dbmerr.NewUnlockFailFastError(m.options.tableName, m.options.mutexName, err)
			}
			if err != nil {
				err2 := dbmerr.NewUnlockError(m.options.tableName, m.options.mutexName, err)
				if m.options.unlockErrNotifier(err2) != nil {
					return err2
				}
			}
			if ticker == nil {
				ticker = time.NewTicker(m.options.pollInterval)
				// This defer inside a for loop is ok because we will only defer once.
				defer ticker.Stop()
				pollChan = ticker.C
			}
		}
	}

	return nil
}

// Identity returns the Identity of this Mutex.
func (m *Mutex) Identity() Identity {
	if m == nil || m.db == nil {
		return Identity{}
	}
	return Identity{
		TableName: m.options.tableName,
		MutexName: m.options.mutexName,
		Hostname:  m.hostname,
		Pid:       m.pid,
		LockerId:  m.lockerId,
	}
}

func initMutexOptions(options ...MutexOption) *mutexOptions {
	mo := &mutexOptions{
		tableName:          DefaultMutexTableName,
		mutexName:          DefaultMutexName,
		refresh:            DefaultRefresh,
		expiration:         DefaultExpiration,
		createMissingTable: true,
		lockErrNotifier:    IgnoreLogError,
		unlockErrNotifier:  IgnoreLogError,
		refreshErrNotifier: IgnoreLogError,
	}
	for _, o := range options {
		o(mo)
	}
	if mo.tableName == "" {
		mo.tableName = DefaultMutexTableName
	}
	if mo.mutexName == "" {
		mo.mutexName = DefaultMutexName
	}
	if mo.refresh <= 0 {
		mo.refresh = DefaultRefresh
	}
	if mo.expiration <= 0 {
		mo.expiration = DefaultExpiration
	}
	if mo.lockErrNotifier == nil {
		mo.lockErrNotifier = IgnoreLogError
	}
	if mo.unlockErrNotifier == nil {
		mo.unlockErrNotifier = IgnoreLogError
	}
	if mo.refreshErrNotifier == nil {
		mo.refreshErrNotifier = IgnoreLogError
	}
	if mo.pollInterval <= 0 {
		mo.pollInterval = DefaultPollInterval
	}
	return mo
}

func createMutexTableIfNotExists(
	ctx context.Context,
	db *sql.DB,
	driver driver.Driver,
	tableName string,
) error {
	err := driver.CreateMutexTableIfNotExists(ctx, db, tableName)
	if err != nil {
		// It is possible that create table if not exists operation will fail with a message like
		// pq: duplicate key value violates unique constraint "pg_type_typname_nsp_index".  This race condition
		// should only occur if multiple callers attempt to create the same table at the same time. A simple
		// retry should eliminate the error.
		err2 := driver.CreateMutexTableIfNotExists(ctx, db, tableName)
		if err2 != nil {
			return err
		}
	}
	return err
}

// New creates a new Mutex. The passed db is used for all database interactions. Behaviour can be customized
// by passing options.  The option that should almost always be passed is the lock name (See WithMutexName.)
func New(
	ctx context.Context,
	db *sql.DB,
	options ...MutexOption,
) (*Mutex, error) {
	mo := initMutexOptions(options...)
	var err error
	if mo.driver == nil {
		mo.driver, err = driver.ResolveDriver(ctx, db)
		if err != nil {
			return nil, err
		}
	}

	if mo.createMissingTable {
		err = createMutexTableIfNotExists(ctx, db, mo.driver, mo.tableName)
		if err != nil {
			return nil, err
		}
	}

	if !mo.delayAddMutexRow {
		err = mo.driver.CreateMutexEntryIfNotExists(ctx, db, mo.tableName, mo.mutexName)
		if err != nil {
			// mysql can fail on concurrent inserts :( so try one more time.
			// Error 1213: Deadlock found when trying to get lock; try restarting transaction
			err2 := mo.driver.CreateMutexEntryIfNotExists(ctx, db, mo.tableName, mo.mutexName)
			if err2 != nil {
				return nil, err
			}
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	if len(hostname) > driver.MaxHostnameLength {
		hostname = hostname[:driver.MaxHostnameLength]
	}
	pid := os.Getpid()

	return &Mutex{
		options:  mo,
		db:       db,
		lock:     &sync.Mutex{},
		hostname: hostname,
		pid:      pid,
		lockerId: uuid.New().String(),
	}, nil
}

// goRefreshLock keeps the database row corresponding to the passed parameters up-to-date by periodically
// updating the row in order to indicate that the mutex is still held.
func goRefreshLock(
	driver driver.Driver,
	ex driver.Execer,
	tableName string,
	mutexName string,
	hostname string,
	pid int,
	lockerId string,
	refresh time.Duration,
	expiration time.Duration,
	quit <-chan struct{},
	now func() time.Time,
	errorNotifier ErrorNotifier,
	lockCanceled context.CancelFunc,
) <-chan struct{} {
	doneRefreshing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(refresh)
		defer func() {
			ticker.Stop()
			lockCanceled()
			close(doneRefreshing)
		}()
		expiresAt := now().Add(expiration)
		bgContext := context.Background()
		for {
			select {
			case <-ticker.C:
				lockRefreshed, err := driver.Refresh(
					bgContext,
					ex,
					tableName,
					mutexName,
					hostname,
					pid,
					lockerId,
					refresh,
					expiration,
				)
				if err != nil {
					if now().After(expiresAt) {
						// we haven't had a refresh and should now be expired. bail out.
						_ = errorNotifier(dbmerr.NewRefreshLockError(tableName, mutexName, false, err))
						return
					}
					// report the error, but don't bail from loop unless calling error notifier forces bail out.
					// we'll try again later.
					if errorNotifier(dbmerr.NewRefreshLockError(tableName, mutexName, true, err)) != nil {
						return
					}
				} else if !lockRefreshed {
					// we are no longer owner of lock. wtf.
					_ = errorNotifier(dbmerr.NewRefreshLockError(tableName, mutexName, false,
						errors.New("no longer owner of lock")))
					return
				} else {
					// good refresh. reset expiration time.
					expiresAt = now().Add(expiration)
				}
			case <-quit:
				return
			}
		}
	}()
	return doneRefreshing
}

var (
	closedTimeChan     chan time.Time
	closedTimeChanOnce sync.Once
)

func getClosedTimeChan() <-chan time.Time {
	closedTimeChanOnce.Do(func() {
		closedTimeChan = make(chan time.Time)
		close(closedTimeChan)
	})
	return closedTimeChan
}
