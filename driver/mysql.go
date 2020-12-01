package driver

import (
	"context"
	"database/sql"
	"fmt"
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
lock_id bigint not null auto_increment primary key,
name varchar(%d) not null,
locked boolean not null default false,
locker_host varchar(%d) null,
locker_pid bigint null,
locker_id varchar(%d) null,
locked_at timestamp null,
refresh_at timestamp null,
expires_at timestamp not null default current_timestamp,
released_at timestamp null,
created_at timestamp not null default current_timestamp,
updated_at timestamp not null default current_timestamp,
constraint %s_idx_name unique(name)
)`, tableName, MaxMutexNameLength, MaxHostnameLength, MaxLockerIdLength, tableName)
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
	q := fmt.Sprintf(`update %s lock_t
set
	locked = true,
	locker_host = ?,
	locker_pid = ?,
	locker_id = ?,
	locked_at = now(),
	refresh_at = timestampadd(microsecond, ?, now()),
	expires_at = timestampadd(microsecond, ?, now()),
	released_at = null,
	updated_at = now()
where
	name = ?
	and (not locked or expires_at < now())
`, tableName)
	result, err := ex.ExecContext(ctx, q, hostname, pid, lockerId, refresh.Microseconds(), expires.Microseconds(), mutexName)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected == 1, nil
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
	refresh_at = timestampadd(microsecond, ?, now()),
	expires_at = timestampadd(microsecond, ?, now()),
	updated_at = now()
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
	return unlock(
		ctx,
		ex,
		tableName,
		mutexName,
		hostname,
		pid,
		lockerId,
		mysqlPlaceholder,
	)
}

func mysqlPlaceholder(int) string {
	return "?"
}
