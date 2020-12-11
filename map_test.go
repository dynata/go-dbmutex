package dbmutex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dynata/go-dbmutex/dbmerr"
)

type testMutex struct {
	l                   sync.Mutex
	lockCalled          int32
	failOnLockCallNum   int32
	unlockCalled        int32
	failOnUnlockCallNum int32
}

var errLockFailure = errors.New("lock failure")
var errUnlockFailure = errors.New("unlock failure")

func (t *testMutex) Lock(ctx context.Context) (context.Context, error) {
	callNum := atomic.AddInt32(&t.lockCalled, 1)
	if callNum == t.failOnLockCallNum {
		return nil, errLockFailure
	}
	t.l.Lock()
	return context.Background(), nil
}

func (t *testMutex) Unlock(ctx context.Context) error {
	callNum := atomic.AddInt32(&t.unlockCalled, 1)
	// go ahead and unlock even if we might return error since were just simulating a failure
	t.l.Unlock()
	if callNum == t.failOnUnlockCallNum {
		return errUnlockFailure
	}
	return nil
}

type testMutexAllocator struct {
	called                  int32
	failOnAllocationCallNum int32
	failOnLockCallNum       int32
	failOnUnlockCallNum     int32
}

var errFailedAllocation = errors.New("failed allocation")

func (t *testMutexAllocator) mutexAllocator(context.Context, *sql.DB, ...MutexOption) (mutexOperations, error) {
	// fmt.Println("mutexAllocator called")
	callNum := atomic.AddInt32(&t.called, 1)
	if callNum == t.failOnAllocationCallNum {
		return nil, errFailedAllocation
	}
	return &testMutex{
		failOnLockCallNum:   t.failOnLockCallNum,
		failOnUnlockCallNum: t.failOnUnlockCallNum,
	}, nil
}

func TestMutexMap_SimpleLockUnlock(t *testing.T) {
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	_, err := mm.Lock(context.Background(), "X")
	if err != nil {
		t.Error(err)
		return
	}
	err = mm.Unlock(context.Background(), "X")
	if err != nil {
		t.Error(err)
		return
	}
	if 1 != int(ma.called) {
		t.Errorf("expected 1 alloation but was %d", ma.called)
	}
	if mm.len() != 0 {
		t.Errorf("expected map len to be 0 but was %d", mm.len())
	}
}

func TestMutexMap_Concurrent(t *testing.T) {
	concurrency := 10000
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))

	acc := int32(0)
	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := mm.Lock(context.Background(), "X")
			if err != nil {
				t.Error(err)
				return
			}
			acc++
			err = mm.Unlock(context.Background(), "X")
			if err != nil {
				t.Error(err)
				return
			}
		}(i)
	}
	wg.Wait()
	if int(acc) != concurrency {
		t.Errorf("expected to accumulate %d but was %d", concurrency, acc)
	}
	if mm.len() != 0 {
		t.Errorf("expected map len to be 0 but was %d", mm.len())
	}
}

func TestMutexMap_MapStaysSmall(t *testing.T) {
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	maxI := 100
	maxJ := 100
	for i := 0; i < maxI; i++ {
		for j := 0; j < maxJ; j++ {
			lockName := fmt.Sprintf("%d", i)
			_, err := mm.Lock(context.Background(), lockName)
			if err != nil {
				t.Error(err)
				return
			}
			err = mm.Unlock(context.Background(), lockName)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}
	expectedAllocations := maxI * maxJ
	if expectedAllocations != int(ma.called) {
		t.Errorf("expected %d alloations but was %d", expectedAllocations, ma.called)
	}
	if mm.len() != 0 {
		t.Errorf("expected map len to be 0 but was %d", mm.len())
	}
}

func TestMutexMap_MapStaysSmallWithConcurrency(t *testing.T) {
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	maxI := 100
	maxJ := 100
	wg := sync.WaitGroup{}
	for i := 0; i < maxI; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < maxJ; j++ {
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					lockName := fmt.Sprintf("%d", i)
					_, err := mm.Lock(context.Background(), lockName)
					if err != nil {
						t.Error(err)
						return
					}
					// time.Sleep(10 * time.Microsecond)
					err = mm.Unlock(context.Background(), lockName)
					if err != nil {
						t.Error(err)
						return
					}
				}(j)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("allocate called %d times", ma.called)
	if int(ma.called) < maxI {
		t.Errorf("expected at least %d alloations but was %d", maxI, ma.called)
	}
	if int(ma.called) > maxI*maxJ {
		t.Errorf("expected fewer than %d alloations but was %d", maxI*maxJ, ma.called)
	}
	if mm.len() != 0 {
		t.Errorf("expected map len to be 0 but was %d", mm.len())
	}
}

func TestMutexMap_MaxLocalWaiters(t *testing.T) {
	ma := &testMutexAllocator{}
	maxWaiters := 10
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator), WithMaxLocalWaiters(int32(maxWaiters)))
	maxI := 100
	wg := sync.WaitGroup{}
	lockName := "testLock"
	acquiredLock := 0
	maxWaitersErrs := int32(0)
	for i := 0; i < maxI; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := mm.Lock(context.Background(), lockName)
			if err != nil {
				var maxWaitersErr dbmerr.MaxWaitersExceededError
				if !errors.As(err, &maxWaitersErr) {
					t.Error(err)
					return
				}
				// unable to acquire lock because of max waiters
				atomic.AddInt32(&maxWaitersErrs, 1)
				return
			}
			acquiredLock++
			time.Sleep(10 * time.Millisecond)
			err = mm.Unlock(context.Background(), lockName)
			if err != nil {
				t.Error(err)
				return
			}
		}(i)
	}
	wg.Wait()

	if acquiredLock != maxWaiters+1 {
		t.Errorf("expected to acquire lock %d times but was %d", maxWaiters+1, acquiredLock)
	}
	if acquiredLock+int(maxWaitersErrs) != maxI {
		t.Errorf("expected acquire lock count + max waiters failures to be maxI(%d) but was %d", maxI, acquiredLock+maxWaiters)
	}
	if int(ma.called) != 1 {
		t.Errorf("expected 1 alloation but was %d", ma.called)
	}
	if mm.len() != 0 {
		t.Errorf("expected map len to be 0 but was %d", mm.len())
	}
}

func TestMutexMap_LockTimeout(t *testing.T) {
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	lockName := "testLock"
	lockIsHeld := sync.WaitGroup{}
	lockIsHeld.Add(1)
	secondDone := sync.WaitGroup{}
	secondDone.Add(1)
	bothDone := sync.WaitGroup{}
	bothDone.Add(2)
	go func() {
		defer bothDone.Done()
		_, err := mm.Lock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
		lockIsHeld.Done()
		secondDone.Wait()
		err = mm.Unlock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
	}()
	go func() {
		defer bothDone.Done()
		defer secondDone.Done()
		lockIsHeld.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		lockCtx, err := mm.Lock(ctx, lockName)
		if lockCtx != nil {
			t.Error("returned lock context should have been nil")
		}
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded error but was %v", err)
		}
	}()

	bothDone.Wait()

	// now should be able to acquire after a waiting lock timeout
	_, err := mm.Lock(context.Background(), lockName)
	if err != nil {
		t.Error(err)
		return
	}
	err = mm.Unlock(context.Background(), lockName)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMutexMap_AllocationFailure(t *testing.T) {
	ma := &testMutexAllocator{
		failOnAllocationCallNum: 1,
	}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	lockName := "testLock"
	lockCtx, err := mm.Lock(context.Background(), lockName)
	if err != errFailedAllocation {
		t.Errorf("expected to fail to allocate but was %v", err)
	}
	if lockCtx != nil {
		t.Error("returned lock context should have been nil")
	}
	// now lock after a failure to make sure things are still good
	_, err = mm.Lock(context.Background(), lockName)
	if err != nil {
		t.Error(err)
		return
	}
	err = mm.Unlock(context.Background(), lockName)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMutexMap_LockFailure(t *testing.T) {
	ma := &testMutexAllocator{
		failOnLockCallNum: 2,
	}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	lockName := "testLock"

	lockIsHeld := sync.WaitGroup{}
	lockIsHeld.Add(1)
	oneDone := sync.WaitGroup{}
	oneDone.Add(1)
	bothDone := sync.WaitGroup{}
	bothDone.Add(2)
	go func() {
		defer bothDone.Done()
		defer oneDone.Done()
		_, err := mm.Lock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
		lockIsHeld.Done()
		// sleep for a brief time so that second routine will be waiting on local lock acquisition
		time.Sleep(500 * time.Millisecond)
		err = mm.Unlock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
	}()
	go func() {
		defer bothDone.Done()
		lockIsHeld.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		lockCtx, err := mm.Lock(ctx, lockName)
		if lockCtx != nil {
			t.Error("returned lock context should have been nil")
		}
		if err != errLockFailure {
			t.Errorf("expected to fail to lock but was %v", err)
		}
		// wait for other routine to unlock then make sure we can lock again
		oneDone.Wait()
		_, err = mm.Lock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
		err = mm.Unlock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	bothDone.Wait()
}

func TestMutexMap_UnlockFailure(t *testing.T) {
	ma := &testMutexAllocator{
		failOnUnlockCallNum: 2,
	}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	lockName := "testLock"

	lockIsHeld := sync.WaitGroup{}
	lockIsHeld.Add(1)
	oneDone := sync.WaitGroup{}
	oneDone.Add(1)
	allDone := sync.WaitGroup{}
	allDone.Add(3)
	go func() {
		defer allDone.Done()
		defer oneDone.Done()
		_, err := mm.Lock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
		lockIsHeld.Done()
		// sleep for a brief time so that second routine will be waiting on local lock acquisition
		time.Sleep(500 * time.Millisecond)
		err = mm.Unlock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	lockUnlock := func(e *error) {
		defer allDone.Done()
		lockIsHeld.Wait()
		_, err := mm.Lock(context.Background(), lockName)
		if err != nil {
			t.Error(err)
			return
		}
		err = mm.Unlock(context.Background(), lockName)
		*e = err
	}
	var unlockErr1 error
	var unlockErr2 error
	go lockUnlock(&unlockErr1)
	go lockUnlock(&unlockErr2)

	allDone.Wait()

	if unlockErr1 != nil && unlockErr1 != errUnlockFailure {
		t.Errorf("expected error to be nil or unlock failure but was %v", unlockErr1)
	}
	if unlockErr2 != nil && unlockErr2 != errUnlockFailure {
		t.Errorf("expected error to be nil or unlock failure but was %v", unlockErr2)
	}
	if unlockErr1 == nil && unlockErr2 == nil {
		t.Errorf("expected one of errs to be not nil but was 1: %v, 2: %v", unlockErr1, unlockErr2)
	}
	if unlockErr1 == errUnlockFailure && unlockErr2 == errUnlockFailure {
		t.Errorf("expected only one of errs to be unlock failure but was 1: %v, 2: %v", unlockErr1, unlockErr2)
	}
}

func TestMutexMap_UnlockNotLocked(t *testing.T) {
	ma := &testMutexAllocator{}
	mm := NewMutexMap(nil, withMutexAllocator(ma.mutexAllocator))
	lockName := "testLock"
	err := mm.Unlock(context.Background(), lockName)
	var e dbmerr.NotLockedError
	if !errors.As(err, &e) {
		t.Errorf("expected error to be not locked but was %v", err)
	}
}
