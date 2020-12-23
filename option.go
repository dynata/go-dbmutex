package dbmutex

import (
	"time"

	"github.com/dynata/go-dbmutex/driver"
)

type mutexOptions struct {
	tableName          string
	mutexName          string
	refresh            time.Duration
	expiration         time.Duration
	createMissingTable bool
	driver             driver.Driver
	lockErrNotifier    ErrorNotifier
	unlockErrNotifier  ErrorNotifier
	refreshErrNotifier ErrorNotifier
	failFast           bool
	pollInterval       time.Duration
	delayAddMutexRow   bool
}

// MutexOption is used to customize Mutex behaviour.
type MutexOption func(options *mutexOptions)

// WithErrorNotifier allows customization of the ErrorNotifier that will be used when acquiring the lock,
// releasing the lock and refreshing the lock.  This is a convenience call that sets all three ErrorNotifiers to
// the same value
func WithErrorNotifier(f ErrorNotifier) MutexOption {
	return func(o *mutexOptions) {
		o.lockErrNotifier = f
		o.unlockErrNotifier = f
		o.refreshErrNotifier = f
	}
}

// WithDelayAddMutexRow can be used to change the default (false) case for delaying the insertion of a row
// into the mutex table.
func WithDelayAddMutexRow(delay bool) MutexOption {
	return func(o *mutexOptions) {
		o.delayAddMutexRow = delay
	}
}

// WithLockErrorNotifier allows customization of the ErrorNotifier that will be used when acquiring the lock
// in the Lock call.
func WithLockErrorNotifier(f ErrorNotifier) MutexOption {
	return func(o *mutexOptions) {
		o.lockErrNotifier = f
	}
}

// WithUnlockErrorNotifier allows customization of the ErrorNotifier that will be used when releasing the lock
// in the Unlock call.
func WithUnlockErrorNotifier(f ErrorNotifier) MutexOption {
	return func(o *mutexOptions) {
		o.unlockErrNotifier = f
	}
}

// WithRefreshErrorNotifier allows customization of the ErrorNotifier that will be used when keeping
// the Mutex refreshed.
func WithRefreshErrorNotifier(f ErrorNotifier) MutexOption {
	return func(o *mutexOptions) {
		o.refreshErrNotifier = f
	}
}

// WithMutexTableName allows customization of the lock table name. Normally this is not needed since multiple
// named locks can be used in the same lock table.
func WithMutexTableName(name string) MutexOption {
	return func(o *mutexOptions) {
		o.tableName = name
	}
}

// WithMutexName uses name as the name of lock. Normally this option should be passed in order to define
// an application-specific scope of locking that can be used across multiple processes. Examples:
//  WithMutexName("order 234")
//  WithMutexName("customer 9854")
// If len(name) > driver.MaxMutexNameLength name will be silently truncated.
func WithMutexName(name string) MutexOption {
	return func(o *mutexOptions) {
		if len(name) > driver.MaxMutexNameLength {
			name = name[:driver.MaxMutexNameLength]
		}
		o.mutexName = name
	}
}

// WithRefresh allows customization of the interval at which lock refreshes are performed.
func WithRefresh(t time.Duration) MutexOption {
	return func(o *mutexOptions) {
		o.refresh = t
	}
}

// WithExpiration allows customization of the duration after which a lock will expire if it is not refreshed.
// Normally, as long as the locking process continues to run and can reach the database, refreshes happen
// automatically.  If a process cannot update the database, then the lock will expire after t Duration.
func WithExpiration(t time.Duration) MutexOption {
	return func(o *mutexOptions) {
		o.expiration = t
	}
}

// WithCreateMissingTable allows customization of table creation if it does not exist. Pass false in order to
// override the default behaviour and not create a missing table.
func WithCreateMissingTable(b bool) MutexOption {
	return func(o *mutexOptions) {
		o.createMissingTable = b
	}
}

// WithDriver allows explict setting of the Driver. Normally, mysql or postgres is automatically detected.
func WithDriver(d driver.Driver) MutexOption {
	return func(o *mutexOptions) {
		o.driver = d
	}
}

// WithFailFast, when passed true, causes (Un)Lock calls to immediately return if unable to interact with the
// underlying database. Normally (Un)Lock operations will retry until the passed context expires.
func WithFailFast(b bool) MutexOption {
	return func(o *mutexOptions) {
		o.failFast = b
	}
}

// WithPollInterval will change the interval at which (Un)Lock tries to acquire or release the lock. Normally,
// DefaultPollInterval is used.  Be careful when using a small poll interval because you can potentially cause
// increased load on the database server. Duration must be > 0 or else it will be set to DefaultPollInterval.
func WithPollInterval(d time.Duration) MutexOption {
	return func(o *mutexOptions) {
		o.pollInterval = d
	}
}
