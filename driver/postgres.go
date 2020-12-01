package driver

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
)

// PostgresDriver is the postgres specific Driver implementation.
type PostgresDriver struct{}

func postgresPlaceholder(i int) string {
	return fmt.Sprintf("$%d", i)
}

func (PostgresDriver) CreateMutexTableIfNotExists(
	ctx context.Context,
	ex Execer,
	tableName string,
) error {
	q := fmt.Sprintf(`create table if not exists %s (
lock_id bigserial not null primary key,
name varchar(%d) not null,
locked boolean not null default false,
locker_host varchar(%d) null,
locker_pid bigint null,
locker_id varchar(%d) null,
locked_at timestamp with time zone null ,
refresh_at timestamp with time zone null,
expires_at timestamp with time zone not null default current_timestamp,
released_at timestamp with time zone null,
created_at timestamp with time zone not null default current_timestamp,
updated_at timestamp with time zone not null default current_timestamp,
constraint %s_idx_name unique(name)
)`, tableName, MaxMutexNameLength, MaxHostnameLength, MaxLockerIdLength, tableName)
	_, err := ex.ExecContext(ctx, q)
	if err != nil {
		return dbmerr.NewCreateMutexTableError(tableName, err)
	}
	return nil
}

func (PostgresDriver) CreateMutexEntryIfNotExists(
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
		postgresPlaceholder,
		func() (sql.Result, error) {
			return qe.ExecContext(
				ctx,
				"insert into "+tableName+"(name) values ($1) on conflict(name) do nothing",
				mutexName,
			)
		},
	)
}

func (PostgresDriver) Lock(
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
	locker_host = $1,
	locker_pid = $2,
	locker_id = $3,
	locked_at = now(),
	refresh_at = now() + ($4 * interval '1 microsecond'),
	expires_at = now() + ($5 * interval '1 microsecond'),
	released_at = null,
	updated_at = now()
where
	name = $6
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

func (PostgresDriver) Refresh(
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
	refresh_at = now() + ($1 * interval '1 microsecond'),
	expires_at = now() + ($2 * interval '1 microsecond'),
	updated_at = now()
where
	(name, locked, locker_host, locker_pid, locker_id) = ($3, true, $4, $5, $6)
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

func (PostgresDriver) Unlock(
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
		postgresPlaceholder,
	)
}
