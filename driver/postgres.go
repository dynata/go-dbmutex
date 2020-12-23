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
name varchar(%d) not null primary key,
locked boolean not null default false,
locked_count bigint not null default 0,
locked_seconds double precision not null default 0,
locker_host varchar(%d) null,
locker_pid bigint null,
locker_id varchar(%d) null,
locked_at timestamp with time zone null ,
refresh_at timestamp with time zone null,
expires_at timestamp with time zone not null default current_timestamp,
released_at timestamp with time zone null,
created_at timestamp with time zone not null default current_timestamp,
updated_at timestamp with time zone not null default current_timestamp
)`, tableName, MaxMutexNameLength, MaxHostnameLength, MaxLockerIdLength)
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
	q := fmt.Sprintf(`insert into
%s as lock_t (
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
)
values(
	$1,
	true,
	$2,
	$3,
	$4,
	now(),
	now() + ($5 * interval '1 microsecond'),
	now() + ($6 * interval '1 microsecond'),
	null,
	now()
)
on conflict(name) do update set
	locked = excluded.locked,
	locked_count = lock_t.locked_count + 1,
	locker_host = excluded.locker_host,
	locker_pid = excluded.locker_pid,
	locker_id = excluded.locker_id,
	locked_at = excluded.locked_at,
	refresh_at = excluded.refresh_at,
	expires_at = excluded.expires_at,
	released_at = excluded.released_at,
	updated_at = excluded.updated_at
where (not lock_t.locked or lock_t.expires_at < now())`, tableName)
	result, err := ex.ExecContext(ctx, q, mutexName, hostname, pid, lockerId, refresh.Microseconds(), expires.Microseconds())
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
	q := fmt.Sprintf(`update %s
set
	locked = false,
	released_at = now(),
	updated_at = now(),
	locked_seconds = locked_seconds + extract(epoch from (now() - coalesce(locked_at,now())))
where
	(name, locked, locker_host, locker_pid, locker_id) = ($1, true, $2, $3, $4)`, tableName)
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
