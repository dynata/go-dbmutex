package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
)

// MysqlDriver is the mysql specific Driver implementation.
type MysqlDriver struct{}

func (MysqlDriver) CreateMutexTableIfNotExists(
	ctx context.Context,
	ex Execer,
	tableName string,
) error {
	q := fmt.Sprintf(`create table if not exists %s (
name varchar(%d) not null primary key,
locked boolean not null default false,
locked_count bigint not null default 0,
locked_seconds double not null default 0,
locker_host varchar(%d) null,
locker_pid bigint null,
locker_id varchar(%d) null,
locked_at timestamp(6) null,
refresh_at timestamp(6) null,
expires_at timestamp(6) not null default current_timestamp(6),
released_at timestamp(6) null,
created_at timestamp(6) not null default current_timestamp(6),
updated_at timestamp(6) not null default current_timestamp(6)
)`, tableName, MaxMutexNameLength, MaxHostnameLength, MaxLockerIdLength)
	_, err := ex.ExecContext(ctx, q)
	if err != nil {
		return dbmerr.NewCreateMutexTableError(tableName, err)
	}
	return nil
}

func (MysqlDriver) CreateMutexEntryIfNotExists(
	ctx context.Context,
	qe QueryExecer,
	tableName string,
	mutexName string,
) error {
	return createMutexEntryIfNotExists(
		ctx,
		qe,
		tableName,
		mutexName,
		mysqlPlaceholder,
		func() (sql.Result, error) {
			q := "insert ignore into " + tableName + "(name) select ? from dual where not exists " +
				"(select 1 from " + tableName + " where name = ? limit 1 for update)"
			return qe.ExecContext(ctx, q, mutexName, mutexName)
		},
	)
}

func (MysqlDriver) Lock(
	ctx context.Context,
	ex Execer,
	tableName string,
	mutexName string,
	hostname string,
	pid int,
	lockerId string,
	refresh time.Duration,
	expires time.Duration,
) (bool, error) {
	q := fmt.Sprintf(`insert into
%s(
	name,
	locked,
	locker_host,
	locker_pid,
	locker_id,
	locked_at,
	refresh_at,
	expires_at,
	released_at,
	updated_at
) values (
	?,
	true,
	?,
	?,
	?,
	current_timestamp(6),
	timestampadd(microsecond, ?, current_timestamp(6)),
	timestampadd(microsecond , ?, current_timestamp(6)),
	null,
	current_timestamp(6)
) on duplicate key update
	locked = if(not locked or expires_at < current_timestamp(6), true, null),
	locked_count = locked_count + 1,
	locker_host = values(locker_host),
	locker_pid = values(locker_pid),
	locker_id = values(locker_id),
	locked_at = values(locked_at),
	refresh_at = values(refresh_at),
	expires_at = values(expires_at),
	released_at = values(released_at),
	updated_at = values(updated_at)`, tableName)

	var err error
	var result sql.Result
	result, err = ex.ExecContext(ctx, q, mutexName, hostname, pid, lockerId, refresh.Microseconds(), expires.Microseconds())
	if err != nil {
		// if we are unable to acquire the lock, err.Error() will be "Error 1048: Column 'locked' cannot be null"
		if strings.Contains(err.Error(), "Error 1048:") {
			return false, nil
		}
		return false, err
	}
	var rowsAffected int64
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return false, err
	}
	// With ON DUPLICATE KEY UPDATE, the affected-rows value per row is 1 if the row is inserted
	// as a new row and 2 if an existing row is updated.
	return rowsAffected == 1 || rowsAffected == 2, nil
}

func (MysqlDriver) Refresh(
	ctx context.Context,
	ex Execer,
	tableName string,
	mutexName string,
	hostname string,
	pid int,
	lockerId string,
	refresh time.Duration,
	expires time.Duration,
) (bool, error) {
	q := fmt.Sprintf(`update %s
set
	refresh_at = timestampadd(microsecond, ?, current_timestamp(6)),
	expires_at = timestampadd(microsecond, ?, current_timestamp(6)),
	updated_at = current_timestamp(6)
where
	(name, locked, locker_host, locker_pid, locker_id) = (?, true, ?, ?, ?)
`, tableName)
	result, err := ex.ExecContext(ctx, q, refresh.Microseconds(), expires.Microseconds(), mutexName, hostname, pid, lockerId)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected == 1, nil
}

func (MysqlDriver) Unlock(
	ctx context.Context,
	ex Execer,
	tableName string,
	mutexName string,
	hostname string,
	pid int,
	lockerId string,
) (bool, error) {
	q := fmt.Sprintf(`update %s
set
	locked = false,
	released_at = current_timestamp(6),
	updated_at = current_timestamp(6),
	locked_seconds = locked_seconds + (timestampdiff(microsecond, coalesce(locked_at, current_timestamp(6)), current_timestamp(6)) / 1000000)
where
	(name, locked, locker_host, locker_pid, locker_id) = (?, true, ?, ?, ?)`, tableName)
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

func mysqlPlaceholder(int) string {
	return "?"
}
