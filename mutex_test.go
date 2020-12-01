package dbmutex

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
	"github.com/dynata/go-dbmutex/driver"
)

type testDriver struct {
	quitAfterRefresh    int
	quit                chan struct{}
	refreshCalledCount  int
	refreshReturnsError bool
	refreshReturnsFalse bool
	driver.Driver
}

func (d *testDriver) Refresh(
	ctx context.Context,
	ex driver.Execer,
	tableName string,
	lockName string,
	hostname string,
	pid int,
	lockerId string,
	refresh time.Duration,
	expires time.Duration,
) (bool, error) {
	d.refreshCalledCount++
	d.quitAfterRefresh--
	if d.quitAfterRefresh == 0 {
		if d.quit != nil {
			close(d.quit)
			d.quit = nil
		}
	}
	if d.refreshReturnsError {
		return false, errors.New("test refresh error")
	}
	if d.refreshReturnsFalse {
		return false, nil
	}
	return true, nil
}

func waitChanTimeout(done <-chan struct{}, timeout time.Duration) bool {
	// c := make(chan struct{})
	// go func() {
	// 	defer close(c)
	// 	wg.Wait()
	// }()
	select {
	case <-done:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func Test_goRefreshLock(t *testing.T) {
	const testLockTableName = "testLockTable"
	const testLockName = "test lock"
	type args struct {
		driver        *testDriver
		ex            driver.Execer
		tableName     string
		lockName      string
		refresh       time.Duration
		expiration    time.Duration
		quit          chan struct{}
		now           func() time.Time
		errorNotifier ErrorNotifier
	}
	tests := []struct {
		name     string
		args     args
		preCall  func(args *args)
		postCall func(t *testing.T, args *args, done <-chan struct{}, lockCtx context.Context)
	}{
		{
			name: "close channel as if unlock called",
			args: args{
				driver:        &testDriver{},
				ex:            nil,
				tableName:     testLockTableName,
				lockName:      testLockName,
				refresh:       time.Minute,
				expiration:    time.Hour,
				quit:          make(chan struct{}),
				now:           time.Now,
				errorNotifier: LogError,
			},
			postCall: func(t *testing.T, args *args, done <-chan struct{}, lockCtx context.Context) {
				close(args.quit)
				timedOut := waitChanTimeout(done, time.Second*5)
				if timedOut {
					t.Error("expected termination quickly because quit channel was closed")
				}
				if lockCtx.Err() != context.Canceled {
					t.Error("expected lockCtx to be canceled")
				}
			},
		},
		{
			name: "close chan after refresh few times",
			args: args{
				driver:        nil,
				ex:            nil,
				tableName:     testLockTableName,
				lockName:      testLockName,
				refresh:       time.Nanosecond,
				expiration:    time.Hour,
				quit:          make(chan struct{}),
				now:           time.Now,
				errorNotifier: LogError,
			},
			preCall: func(args *args) {
				args.driver = &testDriver{
					quitAfterRefresh: 3,
					quit:             args.quit,
				}
			},
			postCall: func(t *testing.T, args *args, done <-chan struct{}, lockCtx context.Context) {
				timedOut := waitChanTimeout(done, time.Second*5)
				if timedOut {
					t.Error("expected termination quickly after calling refresh 3 times")
				}
				if lockCtx.Err() != context.Canceled {
					t.Error("expected lockCtx to be canceled")
				}
				if args.driver.refreshCalledCount < 3 {
					t.Errorf("expected refresh to be called 3 times but was called %d times", args.driver.refreshCalledCount)
				}
			},
		},
		{
			name: "failure to refresh and then expiration",
			args: args{
				driver:        nil,
				ex:            nil,
				tableName:     testLockTableName,
				lockName:      testLockName,
				refresh:       time.Second / 5,
				expiration:    time.Second,
				quit:          make(chan struct{}),
				now:           time.Now,
				errorNotifier: LogError,
			},
			preCall: func(args *args) {
				args.driver = &testDriver{
					// quitAfterRefresh:    15,
					// quit:                args.quit,
					refreshReturnsError: true,
				}
			},
			postCall: func(t *testing.T, args *args, done <-chan struct{}, lockCtx context.Context) {
				timedOut := waitChanTimeout(done, time.Second*5)
				if timedOut {
					t.Error("expected termination quickly after calling refresh a few times")
				}
				if lockCtx.Err() != context.Canceled {
					t.Error("expected lockCtx to be canceled")
				}
				close(args.quit)
			},
		},
		{
			name: "failure to refresh because lock no longer held",
			args: args{
				driver:        nil,
				ex:            nil,
				tableName:     testLockTableName,
				lockName:      testLockName,
				refresh:       time.Nanosecond,
				expiration:    time.Second,
				quit:          make(chan struct{}),
				now:           time.Now,
				errorNotifier: LogError,
			},
			preCall: func(args *args) {
				args.driver = &testDriver{
					refreshReturnsFalse: true,
				}
			},
			postCall: func(t *testing.T, args *args, done <-chan struct{}, lockCtx context.Context) {
				timedOut := waitChanTimeout(done, time.Second*5)
				if timedOut {
					t.Error("expected termination quickly after failure to refresh")
				}
				if lockCtx.Err() != context.Canceled {
					t.Error("expected lockCtx to be canceled")
				}
				close(args.quit)
			},
		},
	}

	for _, tt := range tests {
		// capture tt because t.Run runs a go routine
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			lockCtx, lockCanceledFunc := context.WithCancel(context.Background())
			if tt.preCall != nil {
				tt.preCall(&tt.args)
			}
			wg := goRefreshLock(
				tt.args.driver,
				tt.args.ex,
				tt.args.tableName,
				tt.args.lockName,
				"test hostname",
				1,
				"test locker id",
				tt.args.refresh,
				tt.args.expiration,
				tt.args.quit,
				tt.args.now,
				tt.args.errorNotifier,
				lockCanceledFunc,
			)
			tt.postCall(t, &tt.args, wg, lockCtx)
		})
	}
}

// In this simple scenario we wait forever for a lock to be acquired. Note that
// the lock name is application specific and should be specific to the area of code that needs to be protected.
func ExampleMutex_Lock() {
	// Initialization
	// Typically the Mutex is created outside of the func that needs to use Lock...
	var db *sql.DB // acquire DB as normal
	dbm, err := New(context.Background(), db, WithMutexName("application specific"))
	if err != nil {
		panic(err)
	}

	// In the func that needs to protect a section of code...
	// Wait until the lock is acquired.
	_, err = dbm.Lock(context.Background())
	if err != nil {
		// Unable to acquire lock. Because lock acquisition is retried (even in the case
		// of errors), a non-nil err will typically not be returned.
	}
	defer func() { _ = dbm.Unlock(context.Background()) }()
	// Do "critical section" application logic here...
}

// In this scenario we wait for a maximum of 10 seconds to acquire the lock.
func ExampleMutex_Lock_withTimeout() {
	// In the func that needs to protect a section of code...
	var dbm Mutex // Mutex normally acquired via New()
	// Use background context here but could have gotten context from an input parameter.
	ctx := context.Background()
	// Wait until the lock is acquired or 10 seconds passes.
	deadlineCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	defer cancelFunc()
	_, err := dbm.Lock(deadlineCtx)
	if err == context.DeadlineExceeded {
		// Timed out waiting to acquire lock.
	} else if err != nil {
		// Some other error when attempting to acquire lock.
	}
	defer func() { _ = dbm.Unlock(context.Background()) }()
	// Do "critical section" application logic here...
}

// In this scenario we want to acquire the lock or immediately fail if unable to do so.
func ExampleMutex_Lock_failFast() {
	// Initialization
	// Typically the Mutex is created outside of the func that needs to use Lock...
	var db *sql.DB // acquire DB as normal
	dbm, err := New(
		context.Background(),
		db,
		WithMutexName("application specific"),
		WithFailFast(true),
	)
	if err != nil {
		panic(err)
	}

	// In the func that needs to protect a section of code...
	// Use background context here but could have gotten context from an input parameter.
	ctx := context.Background()
	_, err = dbm.Lock(ctx)
	var e *dbmerr.LockFailFastError
	if errors.As(err, &e) {
		// Failed to immediately acquire the lock.
	} else if err != nil {
		// Some other error when attempting to acquire lock.
	}
	defer func() { _ = dbm.Unlock(context.Background()) }()
	// Do "critical section" application logic here...
}

// In this scenario we want to acquire the lock with a fast poll. Note that if you use a
// fast poll, you may cause performance problems for the database server.
func ExampleMutex_Lock_fastPoll() {
	// Initialization
	// Typically the Mutex is created outside of the func that needs to use Lock...
	var db *sql.DB // acquire DB as normal
	dbm, err := New(
		context.Background(),
		db,
		WithMutexName("application specific"),
		WithPollInterval(time.Millisecond),
	)
	if err != nil {
		panic(err)
	}

	// In the func that needs to protect a section of code...
	// Use background context here but could have gotten context from an input parameter.
	ctx := context.Background()
	// This will cause a 1 millisecond poll interval - which will probably cause DB performance problems.
	_, err = dbm.Lock(ctx)
	if err != nil {
		// Some error when attempting to acquire lock.
	}
	defer func() { _ = dbm.Unlock(context.Background()) }()
	// Do "critical section" application logic here...
}
