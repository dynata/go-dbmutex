package dbtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dynata/go-dbmutex"
	"github.com/dynata/go-dbmutex/dbmerr"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/luna-duclos/instrumentedsql"
	"github.com/ory/dockertest/v3"
)

const testTableName = "dbmutex_test_lk"
const testLockName = "test_lock"

func startupMysqlContainer(t *testing.T, pool *dockertest.Pool, dockerRepo, dockerTag string) (*dockertest.Resource, *sql.DB, error) {
	t.Logf("Starting %s:%s container. You may see some \"unexpected EOF\" messages which can be ignored.", dockerRepo, dockerTag)
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run(dockerRepo, dockerTag, []string{"MYSQL_ROOT_PASSWORD=secret"})
	if err != nil {
		return nil, nil, fmt.Errorf("could not start resource: %s", err)
	}

	var db *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open(
			"mysql",
			fmt.Sprintf("root:secret@(localhost:%s)/mysql", resource.GetPort("3306/tcp")),
		)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		return nil, nil, fmt.Errorf("could not connect to docker: %s", err)
	}

	return resource, db, nil
}

func startupPostgresContainer(t *testing.T, pool *dockertest.Pool, dockerRepo, dockerTag string) (*dockertest.Resource, *sql.DB, error) {
	t.Logf("Starting %s:%s container.", dockerRepo, dockerTag)

	database := "testdb"
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run(dockerRepo, dockerTag, []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=" + database})
	if err != nil {
		return nil, nil, fmt.Errorf("could not start resource: %s", err)
	}

	var db *sql.DB
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open(
			"postgres",
			fmt.Sprintf("postgres://postgres:secret@localhost:%s/%s?sslmode=disable",
				resource.GetPort("5432/tcp"),
				database),
		)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		return nil, nil, fmt.Errorf("could not connect to docker: %s", err)
	}

	return resource, db, nil
}

func runAllDbTests(t *testing.T, dbName string, db *sql.DB, cleanup func()) {
	if cleanup != nil {
		defer cleanup()
	}
	defer func() { _ = db.Close() }()
	t.Run(dbName, func(t *testing.T) {
		t.Run("New", func(t *testing.T) {
			testNew(t, db)
		})
		t.Run("ManyDifferentLockNames", func(t *testing.T) {
			testManyDifferentLockNames(t, db, 1000, 1000000)
		})
		t.Run("HammerMutexSameObject", func(t *testing.T) {
			testHammerMutex(t, db, 10, 30, true, testTableName, testLockName)
		})
		t.Run("HammerMutexDifferentObject", func(t *testing.T) {
			testHammerMutex(t, db, 10, 30, false, testTableName, testLockName)
		})
		t.Run("HammerMutexMapSameObject", func(t *testing.T) {
			testHammerMutexMap(t, db, 10, 30, true, testTableName, testLockName)
		})
		t.Run("DifferentLockTableNames", func(t *testing.T) {
			testDifferentLockTableNames(t, db)
		})
		t.Run("DifferentLockNames", func(t *testing.T) {
			testDifferentLockNames(t, db)
		})
		t.Run("Timeouts", func(t *testing.T) {
			testTimeouts(t, db)
		})
		t.Run("LockStripped", func(t *testing.T) {
			testLockStripped(t, db)
		})
		t.Run("LockCanceled", func(t *testing.T) {
			testLockCanceled(t, db)
		})
		t.Run("DuplicateUnlock", func(t *testing.T) {
			testDuplicateUnlock(t, db)
		})
		t.Run("UnlockBeforeLock", func(t *testing.T) {
			testUnlockBeforeLock(t, db)
		})
	})
}

func TestDatabases(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s", err)
	}

	type dbContainerStarter struct {
		repo      string
		tag       string
		startFunc func(t *testing.T, pool *dockertest.Pool, repo, tag string) (*dockertest.Resource, *sql.DB, error)
	}

	dbsToTest := []dbContainerStarter{
		{
			repo:      "mysql",
			tag:       "5.6",
			startFunc: startupMysqlContainer,
		},
		{
			repo:      "mysql",
			tag:       "5.7",
			startFunc: startupMysqlContainer,
		},
		{
			repo:      "mysql",
			tag:       "8.0.22",
			startFunc: startupMysqlContainer,
		},
		{
			repo:      "postgres",
			tag:       "9.6",
			startFunc: startupPostgresContainer,
		},
		{
			repo:      "postgres",
			tag:       "10.15",
			startFunc: startupPostgresContainer,
		},
		{
			repo:      "postgres",
			tag:       "11.10",
			startFunc: startupPostgresContainer,
		},
		{
			repo:      "postgres",
			tag:       "12.5",
			startFunc: startupPostgresContainer,
		},
		{
			repo:      "postgres",
			tag:       "13.1",
			startFunc: startupPostgresContainer,
		},
	}

	for _, dbToTest := range dbsToTest {
		res, db, err := dbToTest.startFunc(t, pool, dbToTest.repo, dbToTest.tag)
		if err != nil {
			t.Log("failed to start container")
			t.Error(err)
			return
		}
		cleanup := func() {
			purgeErr := pool.Purge(res)
			if purgeErr != nil {
				t.Errorf("error during purge: %s", purgeErr)
			}
		}
		runAllDbTests(t, dbToTest.repo+":"+dbToTest.tag, db, cleanup)
	}
}

func lockAndUnlock(t testing.TB, parentCtx context.Context, m *dbmutex.Mutex) {
	ctx, cancelFunc := context.WithDeadline(parentCtx, time.Now().Add(time.Second*5))
	defer cancelFunc()
	_, err := m.Lock(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	err = m.Unlock(ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

func hammerMutex(t testing.TB, ctx context.Context, m *dbmutex.Mutex, loops int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < loops; i++ {
		err := ctx.Err()
		if err != nil {
			t.Error(err)
			return
		}
		lockAndUnlock(t, ctx, m)
	}
}

func testHammerMutex(tb testing.TB, db *sql.DB, hammers, loops int, sameObject bool, tableName, lockName string) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(tableName),
		dbmutex.WithMutexName(lockName),
		dbmutex.WithPollInterval(time.Nanosecond),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	var m *dbmutex.Mutex
	var err error
	if sameObject {
		m, err = dbmutex.New(ctx, db, options...)
		if err != nil {
			tb.Fatal(err)
		}
	}
	wg := sync.WaitGroup{}
	for i := 0; i < hammers; i++ {
		if !sameObject {
			m, err = dbmutex.New(ctx, db, options...)
			if err != nil {
				tb.Error(err)
				return
			}
		}
		wg.Add(1)
		go hammerMutex(tb, ctx, m, loops, &wg)
	}
	waitForTestCompletion(tb, &wg, maxDuration)
}

func lockAndUnlockMap(t testing.TB, parentCtx context.Context, mm *dbmutex.MutexMap, name string) {
	ctx, cancelFunc := context.WithDeadline(parentCtx, time.Now().Add(time.Second*5))
	defer cancelFunc()
	_, err := mm.Lock(ctx, name)
	if err != nil {
		t.Error(err)
		return
	}
	err = mm.Unlock(ctx, name)
	if err != nil {
		t.Error(err)
		return
	}
}

func hammerMutexMap(t testing.TB, ctx context.Context, mm *dbmutex.MutexMap, name string, loops int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < loops; i++ {
		err := ctx.Err()
		if err != nil {
			t.Error(err)
			return
		}
		lockAndUnlockMap(t, ctx, mm, name)
	}
}

func testHammerMutexMap(tb testing.TB, db *sql.DB, hammers, loops int, sameObject bool, tableName, lockName string) {
	options := []dbmutex.MutexMapOption{
		dbmutex.WithMutexOptions(
			dbmutex.WithMutexTableName(tableName),
			dbmutex.WithPollInterval(time.Nanosecond),
		),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	var mm *dbmutex.MutexMap
	var err error
	if sameObject {
		mm, err = dbmutex.NewMutexMap(ctx, db, options...)
		if err != nil {
			tb.Fatal(err)
		}
	}
	wg := sync.WaitGroup{}
	for i := 0; i < hammers; i++ {
		if !sameObject {
			mm, err = dbmutex.NewMutexMap(ctx, db, options...)
			if err != nil {
				tb.Error(err)
				return
			}
		}
		wg.Add(1)
		go hammerMutexMap(tb, ctx, mm, lockName, loops, &wg)
	}
	waitForTestCompletion(tb, &wg, maxDuration)
}

var instrumentOnce sync.Once

func localTestDB() (*sql.DB, error) {
	instrumentOnce.Do(
		func() {
			logger := instrumentedsql.LoggerFunc(func(ctx context.Context, msg string, keyvals ...interface{}) {
				log.Printf("%s %v", msg, keyvals)
			})
			sql.Register("instrumented-mysql", instrumentedsql.WrapDriver(&mysql.MySQLDriver{}, instrumentedsql.WithLogger(logger)))
			sql.Register("instrumented-postgres", instrumentedsql.WrapDriver(&pq.Driver{}, instrumentedsql.WithLogger(logger)))
		},
	)

	logDB := false
	testPostgres := false
	connectStr := fmt.Sprintf("dev:dev@(localhost:%s)/dev", "13306")
	driverName := "mysql"
	if testPostgres {
		connectStr = fmt.Sprintf("postgres://postgres:dev@localhost:%s/dev?sslmode=disable", "5432")
		driverName = "postgres"
	}
	if logDB {
		driverName = "instrumented-" + driverName
	}
	return sql.Open(driverName, connectStr)
}

func BenchmarkUncontendedReusedMutex(b *testing.B) {
	db, err := localTestDB()
	if err != nil {
		b.Fatal(err)
	}
	testHammerMutex(b, db, 1, b.N, true, testTableName, testLockName)
}

func BenchmarkUncontendedNewMutex(b *testing.B) {
	db, err := localTestDB()
	if err != nil {
		b.Fatal(err)
	}
	testHammerMutex(b, db, 1, b.N, false, testTableName, testLockName)
}

func BenchmarkLockAndUnlockNewMutex(b *testing.B) {
	db, err := localTestDB()
	if err != nil {
		b.Fatal(err)
	}
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
		dbmutex.WithMutexName(testLockName),
		dbmutex.WithPollInterval(time.Nanosecond),
	}
	for i := 0; i < b.N; i++ {
		const maxDuration = time.Second * 30
		ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
		var m *dbmutex.Mutex
		m, err = dbmutex.New(ctx, db, options...)
		if err != nil {
			b.Fatal(err)
		}
		lockAndUnlock(b, ctx, m)
		cancelFunc()
	}
}

func BenchmarkLockAndUnlockMutexMap(b *testing.B) {
	db, err := localTestDB()
	if err != nil {
		b.Fatal(err)
	}
	options := []dbmutex.MutexMapOption{
		dbmutex.WithMutexOptions(
			dbmutex.WithMutexTableName(testTableName),
			dbmutex.WithPollInterval(time.Nanosecond),
		),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	mm, err := dbmutex.NewMutexMap(ctx, db, options...)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		lockAndUnlockMap(b, ctx, mm, testLockName)
	}
}

func BenchmarkUncontendedMutexMap(b *testing.B) {
	db, err := localTestDB()
	if err != nil {
		b.Fatal(err)
	}
	testHammerMutexMap(b, db, 1, b.N, true, testTableName, testLockName)
}

func waitForTestCompletion(t testing.TB, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	if waitTimeout(wg, d) {
		t.Error("timed out waiting to test to complete")
	}
}

func testTimeouts(t *testing.T, db *sql.DB) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
		dbmutex.WithRefresh(500 * time.Millisecond),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	m1, err := dbmutex.New(ctx, db, append(options, dbmutex.WithPollInterval(time.Second))...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m1.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	holdLockTime := 5 * time.Second
	go func() {
		defer wg.Done()
		// hold lock for some time
		time.Sleep(holdLockTime)
		err := m1.Unlock(ctx)
		if err != nil {
			t.Error(err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m2, err := dbmutex.New(ctx, db, append(options, dbmutex.WithPollInterval(time.Millisecond))...)
		if err != nil {
			t.Error(err)
			return
		}
		t1 := time.Now()
		// wait for less time than we know locker will hold lock.
		waitTime := holdLockTime / 2
		lockCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(waitTime))
		defer cancelFunc()
		_, err = m2.Lock(lockCtx)
		if err == nil {
			t.Error("expected timeout")
		}
		if time.Since(t1) > waitTime+(waitTime/2) {
			t.Error("should have timed out sooner")
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		// poll with a longer period than we know locker will hold lock so that we know that we should time out.
		pollTime := time.Nanosecond * time.Duration(float64(holdLockTime.Nanoseconds())*1.25)
		m3, err := dbmutex.New(ctx, db, append(options, dbmutex.WithPollInterval(pollTime))...)
		if err != nil {
			t.Error(err)
			return
		}
		t1 := time.Now()
		// wait for less time than we know locker will hold lock.
		waitTime := holdLockTime / 2
		lockCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(waitTime))
		defer cancelFunc()
		_, err = m3.Lock(lockCtx)
		if err == nil {
			t.Error("expected timeout")
		}
		if time.Since(t1) > waitTime+(waitTime/2) {
			t.Error("should have timed out sooner")
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		m4, err := dbmutex.New(ctx, db, append(options, dbmutex.WithPollInterval(time.Millisecond))...)
		if err != nil {
			t.Error(err)
			return
		}
		t1 := time.Now()
		// wait for more time than we know locker will hold lock.
		waitTime := holdLockTime * 2
		lockCtx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(waitTime))
		defer cancelFunc()
		_, err = m4.Lock(lockCtx)
		if err != nil {
			t.Error("did not expect timeout")
		}
		if time.Since(t1) > holdLockTime+(holdLockTime/2) {
			t.Error("took too long to acquire lock")
		}
		err = m4.Unlock(ctx)
		if err != nil {
			t.Error(err)
		}
	}()

	waitForTestCompletion(t, &wg, maxDuration)
}

func testNew(t *testing.T, db *sql.DB) {
	type args struct {
		ctx     context.Context
		db      *sql.DB
		options []dbmutex.MutexOption
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "happy path",
			args: args{
				ctx: context.Background(),
				db:  db,
				options: []dbmutex.MutexOption{
					dbmutex.WithMutexTableName(testTableName),
				},
			},
		},
		{
			// run this after a test that has created table
			name: "table already exists",
			args: args{
				ctx: context.Background(),
				db:  db,
				options: []dbmutex.MutexOption{
					dbmutex.WithMutexTableName(testTableName),
				},
			},
		},
		{
			// run this after a test that has created table
			name: "table already exists - skip create",
			args: args{
				ctx: context.Background(),
				db:  db,
				options: []dbmutex.MutexOption{
					dbmutex.WithMutexTableName(testTableName),
					dbmutex.WithCreateMissingTable(false),
				},
			},
		},
		{
			name: "table doesnt exist - skip create",
			args: args{
				ctx: context.Background(),
				db:  db,
				options: []dbmutex.MutexOption{
					dbmutex.WithMutexTableName("should_not_exist"),
					dbmutex.WithCreateMissingTable(false),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutex, err := dbmutex.New(tt.args.ctx, tt.args.db, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if mutex == nil {
				t.Error("New() unexpected nil mutex")
				return
			}
		})
	}
}

func TestLocalNew(t *testing.T) {
	skipLocal(t)
	db, err := localTestDB()
	if err != nil {
		t.Fatal(err)
	}
	testNew(t, db)
}

func TestAllLocalDb(t *testing.T) {
	skipLocal(t)
	db, err := localTestDB()
	if err != nil {
		t.Fatal(err)
	}
	runAllDbTests(t, "local", db, nil)
}

func testDifferentLockTableNames(t *testing.T, db *sql.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		tableName := fmt.Sprintf("%s_%d", testTableName, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			testHammerMutex(t, db, 5, 20, false, tableName, testLockName)
		}()
	}
	const maxDuration = time.Second * 30
	waitForTestCompletion(t, &wg, maxDuration)
}

func testDifferentLockNames(t *testing.T, db *sql.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		lockName := fmt.Sprintf("%s_%d", testLockName, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			testHammerMutex(t, db, 5, 20, false, testTableName, lockName)
		}()
	}
	const maxDuration = time.Second * 30
	waitForTestCompletion(t, &wg, maxDuration)
}

func testManyDifferentLockNames(t *testing.T, db *sql.DB, iterations, maxName int) {
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	rand.Seed(time.Now().Unix())
	for i := 0; i < iterations; i++ {
		lockName := fmt.Sprintf("%s_%d", testLockName, rand.Intn(maxName))
		options := []dbmutex.MutexOption{
			dbmutex.WithMutexTableName(testTableName),
			dbmutex.WithMutexName(lockName),
		}
		var m *dbmutex.Mutex
		var err error
		m, err = dbmutex.New(ctx, db, options...)
		if err != nil {
			t.Fatal(err)
		}
		lockAndUnlock(t, ctx, m)
	}
}

func skipLocal(tb testing.TB) {
	tb.Skip("skipping local test")
}

func TestLocalManyDifferentLockNames(t *testing.T) {
	skipLocal(t)
	db, err := localTestDB()
	if err != nil {
		t.Fatal(err)
	}
	testManyDifferentLockNames(t, db, 1000, 1000000)
}

func testLockCanceled(t *testing.T, db *sql.DB) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
		dbmutex.WithRefresh(500 * time.Millisecond),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	m, err := dbmutex.New(ctx, db, options...)
	if err != nil {
		t.Fatal(err)
	}
	lockCtx, err := m.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		err := m.Unlock(ctx)
		if err != nil {
			t.Error(err)
		}
	}()

	select {
	case <-lockCtx.Done():
		// expect to come here after lock is released.
	case <-time.After(10 * time.Second):
		t.Error("failed to be notified when lock was released")
	}
	waitForTestCompletion(t, &wg, 3*time.Second)
}

func testLockStripped(t *testing.T, db *sql.DB) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
		dbmutex.WithRefresh(500 * time.Millisecond),
	}
	const maxDuration = time.Second * 30
	ctx, cancelFunc := context.WithDeadline(context.Background(), time.Now().Add(maxDuration))
	defer cancelFunc()
	m, err := dbmutex.New(ctx, db, options...)
	if err != nil {
		t.Fatal(err)
	}
	lockCtx, err := m.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}

	stripWg := sync.WaitGroup{}
	stripWg.Add(1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if waitTimeout(&stripWg, 3*time.Second) {
			t.Error("timed out waiting for lock to be stripped")
			return
		}
		select {
		case <-lockCtx.Done():
			// expect to come here after lock is stripped.
		case <-time.After(10 * time.Second):
			t.Error("failed to be notified when lock was released")
		}
		err := m.Unlock(ctx)
		if err != nil {
			t.Error(err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stripWg.Done()
		identity := m.Identity()
		q := fmt.Sprintf("update %s set locked = false where name = '%s'", identity.TableName, identity.MutexName)
		results, err := db.ExecContext(ctx, q)
		if err != nil {
			t.Error(err)
			return
		}
		affected, err := results.RowsAffected()
		if err != nil {
			t.Error(err)
			return
		}
		if affected != 1 {
			t.Error("expected to modify one row")
			return
		}
	}()

	select {
	case <-lockCtx.Done():
		// expect to come here after lock is stripped.
	case <-time.After(10 * time.Second):
		t.Error("failed to be notified when lock was released")
	}
	waitForTestCompletion(t, &wg, 5*time.Second)
}

func testDuplicateUnlock(t *testing.T, db *sql.DB) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
	}
	ctx := context.Background()
	m, err := dbmutex.New(ctx, db, options...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Unlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Unlock(ctx)
	var lockNotHeld dbmerr.NotLockedError
	if !errors.As(err, &lockNotHeld) {
		t.Fatal("expected to get NotLockedError")
	}
}

func testUnlockBeforeLock(t *testing.T, db *sql.DB) {
	options := []dbmutex.MutexOption{
		dbmutex.WithMutexTableName(testTableName),
	}
	ctx := context.Background()
	m, err := dbmutex.New(ctx, db, options...)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Unlock(ctx)
	var lockNotHeld dbmerr.NotLockedError
	if !errors.As(err, &lockNotHeld) {
		t.Fatal("expected to get NotLockedError")
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
