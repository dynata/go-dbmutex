package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
)

// These constants hold maximum lengths for a few attributes.
const (
	// Max length of a named mutex.
	MaxMutexNameLength = 256
	// Max length of hostname.
	MaxHostnameLength = 256
	// Max length of locker id.
	MaxLockerIdLength = 256
)

// Execer is the contract used when executing statements against the DB.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Execer is the contract used when querying against the DB.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// QueryExecer is both an Execer and a Queryer.
type QueryExecer interface {
	Execer
	Queryer
}

// Driver is the interface that must be implemented by the database-based locker.  All operations
// should fail fast (returning an error) since retry logic is handled outside of Driver.
type Driver interface {
	// CreateLockTableIfNotExists creates tableName if it does not exists.
	CreateMutexTableIfNotExists(
		ctx context.Context,
		ex Execer,
		tableName string,
	) error

	// CreateLockEntryIfNotExists creates an entry in tableName for mutexName.
	CreateMutexEntryIfNotExists(
		ctx context.Context,
		qe QueryExecer,
		tableName string,
		mutexName string,
	) error

	// AcquireLock attempts to acquire the mutex identified by tableName and mutexName. It returns true
	// if the mutex was acquired. hostname, pid and lockerId are stored with the mutex in order to help
	// with lock holder identification.  The mutex should be refreshed (by calling Refresh) after
	// refresh time elapses and will expire after expires time.
	Lock(
		ctx context.Context,
		ex Execer,
		tableName string,
		mutexName string,
		hostname string,
		pid int,
		lockerId string,
		refresh time.Duration,
		expires time.Duration,
	) (bool, error)

	// Refresh refreshes a locked mutex by updating the row in tableName for the mutex identified by
	// mutexName, hostname, pid and lockerId.  Refresh should be called again in refresh time.
	// Return true if the lock was refreshed successfully.
	Refresh(
		ctx context.Context,
		ex Execer,
		tableName string,
		mutexName string,
		hostname string,
		pid int,
		lockerId string,
		refresh time.Duration,
		expires time.Duration,
	) (bool, error)

	// ReleaseLock releases a locked mutex by updating the row in tableName for the mutex identified by
	// mutexName, hostname, pid and lockerId. Return true if the mutex was unlocked successfully.
	Unlock(
		ctx context.Context,
		ex Execer,
		tableName string,
		mutexName string,
		hostname string,
		pid int,
		lockerId string,
	) (bool, error)
}

// ResolveDriver attempts to inspect db and determine which Driver should be used. If able to
// detect the Driver, it is returned.
func ResolveDriver(ctx context.Context, db *sql.DB) (Driver, error) {
	row := db.QueryRowContext(ctx, "select version()")
	var version string
	err := row.Scan(&version)
	if err != nil {
		return nil, dbmerr.NewDriverDetectionError(err)
	}
	version = strings.ToLower(version)
	if strings.Contains(version, "mysql") {
		return MysqlDriver{}, nil
	}
	if strings.Contains(version, "postgres") {
		return PostgresDriver{}, nil
	}

	// For mysql, it is likely that version does not contain "mysql". So check another way.
	row = db.QueryRowContext(ctx, "show variables where variable_name = 'version'")
	var variableName, variableValue string
	err = row.Scan(&variableName, &variableValue)
	if err != nil {
		return nil, dbmerr.NewDriverDetectionError(err)
	}
	if variableName == "version" {
		// we were able to read something from "show variables" so assume mysql
		return MysqlDriver{}, nil
	}

	return nil, dbmerr.NewDriverDetectionError(fmt.Errorf("only mysql and postgres are supported"))
}

func verifyMutexExists(
	ctx context.Context,
	queryer Queryer,
	tableName string,
	mutextName string,
	placeholder func(int) string,
) (found bool, retErr error) {
	query := "select lock_id from (select lock_id, name, locked, locker_host, locker_pid, locker_id, " +
		"locked_at, released_at, refresh_at, expires_at, created_at, updated_at " +
		"from " + tableName + " where name = " + placeholder(1) + " limit 1) x"
	rows, err := queryer.QueryContext(ctx, query, mutextName)
	if err != nil {
		// cannot even perform basic select on table. ouch
		return false, dbmerr.NewMissingMutexTableError(tableName, err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return false, nil
	}
	var lockId int64
	err = rows.Scan(&lockId)
	if err != nil {
		return false, dbmerr.NewMissingMutexTableError(tableName, err)
	}
	return true, nil
}

func createMutexEntryIfNotExists(
	ctx context.Context,
	queryer Queryer,
	tableName string,
	mutexName string,
	placeholder func(int) string,
	insertLock func() (sql.Result, error),
) error {
	found, err := verifyMutexExists(ctx, queryer, tableName, mutexName, placeholder)
	if err != nil {
		return err
	}
	if found {
		// found it. bail early in happy path.
		return nil
	}
	// Unable to get row. Try to insert.
	result, err := insertLock()
	if err != nil {
		return dbmerr.NewCannotObtainMutexEntryError(tableName, mutexName, err)
	}
	// we may be able to get last insert. if so, return that.
	if ra, err := result.RowsAffected(); err == nil && ra == 1 {
		if lastId, err := result.LastInsertId(); err == nil && lastId != 0 {
			return nil
		}
	}
	// Now, row should exist, so query again.
	found, err = verifyMutexExists(ctx, queryer, tableName, mutexName, placeholder)
	if err != nil {
		return err
	}
	if !found {
		return dbmerr.NewCannotObtainMutexEntryError(tableName, mutexName, nil)
	}
	return nil
}

func unlock(
	ctx context.Context,
	ex Execer,
	tableName string,
	mutexName string,
	hostname string,
	pid int,
	lockerId string,
	placeholder func(i int) string,
) (bool, error) {
	q := fmt.Sprintf(`update %s
set
	locked = false,
	locker_host = null,
	locker_pid = null,
	locker_id = null,
	refresh_at = null,
	expires_at = now(),
	released_at = now(),
	updated_at = now()
where
	(name, locked, locker_host, locker_pid, locker_id) = (%s, true, %s, %s, %s)
`, tableName, placeholder(1), placeholder(2), placeholder(3), placeholder(4))
	result, err := ex.ExecContext(ctx, q, mutexName, hostname, pid, lockerId)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected == 1, nil

}
