package dbmutex

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"

	"github.com/dynata/go-dbmutex/dbmerr"
)

// This interface is mainly used to simply testing.
type mutexOperations interface {
	Lock(ctx context.Context) (context.Context, error)
	Unlock(ctx context.Context) error
}

type mutexMapOptions struct {
	maxLocalWaiters int32
	allocator       mutexAllocator
}

// MutexMapOption is used to customize MutexMap behaviour.
type MutexMapOption func(options *mutexMapOptions)

// WithMaxLocalWaiters sets the maximum number of local waiters. "Local" emphasises that the waiters
// are local to this MutexMap. Waiters are not counted across different MutexMaps and/or processes.
func WithMaxLocalWaiters(max int32) MutexMapOption {
	return func(o *mutexMapOptions) {
		o.maxLocalWaiters = max
	}
}

func withMutexAllocator(allocator mutexAllocator) MutexMapOption {
	return func(o *mutexMapOptions) {
		o.allocator = allocator
	}
}

// mutexAllocator is used to decouple creation of underlying Mutex objects during testing.
type mutexAllocator func(ctx context.Context, db *sql.DB, options ...MutexOption) (mutexOperations, error)

func dbMutexAllocator(ctx context.Context, db *sql.DB, options ...MutexOption) (mutexOperations, error) {
	return New(ctx, db, options...)
}

// MutexMap implements a map of named mutexes. It's primary purpose is to prevent additional database calls
// that are needed for locking Mutex objects. If multiple goroutines in the same process
// use the same MutexMap for locking, only one underlying Mutex will be used to interact with the database.
// Additional lockers will wait via in-process synchronization. If you don't care about the additional database
// resource consumption or have low volume locking needs, you might instead use Mutex directly. Internally,
// a map is used to hold reference counted Mutex objects. Once named Mutex objects are not referenced,
// they are removed from the map so the map size does not grow beyond the number of locked Mutexes.
type MutexMap struct {
	options        *mutexMapOptions
	db             *sql.DB
	lock           sync.Mutex
	countedMutexes map[string]*countedMutex
}

// countedMutex objects are held in MutexMap. They keep hold counters and the underlying Mutex.
type countedMutex struct {
	mutex       mutexOperations
	timeoutLock chan struct{}
	waiters     int32
	references  int32
}

func (cm *countedMutex) incrementWaiters() {
	atomic.AddInt32(&cm.waiters, 1)
}

func (cm *countedMutex) decrementWaiters() {
	atomic.AddInt32(&cm.waiters, -1)
}

func (cm *countedMutex) numWaiters() int32 {
	return atomic.LoadInt32(&cm.waiters)
}

// lock locks the underlying Mutex after first acquiring the private lock.
func (cm *countedMutex) lock(
	ctx context.Context,
	db *sql.DB,
	allocator mutexAllocator,
) (context.Context, error) {
	select {
	case cm.timeoutLock <- struct{}{}:
		// Lock acquired. Will unlock when unlock is called.
	case <-ctx.Done():
		// timeout
		return nil, ctx.Err()
	}
	if cm.mutex == nil {
		m, err := allocator(ctx, db)
		if err != nil {
			// unlock our local lock
			<-cm.timeoutLock
			return nil, err
		}
		cm.mutex = m
	}
	lockCtx, err := cm.mutex.Lock(ctx)
	if err != nil {
		// unlock our local lock
		<-cm.timeoutLock
	}
	return lockCtx, err
}

func (cm *countedMutex) unlock(ctx context.Context) error {
	err := cm.mutex.Unlock(ctx)
	// Unlock local lock even if the underlying implementation fails during unlock. The assumption
	// is that underlying implementation should be able to handle a subsequent Lock call and provide
	// mutual exclusion.
	<-cm.timeoutLock
	return err
}

// NewMutexMap allocates a new MutexMap. The passed db will be used for all database operations. options can be
// used to customize behaviour.
func NewMutexMap(db *sql.DB, options ...MutexMapOption) *MutexMap {
	mmo := &mutexMapOptions{}
	for _, o := range options {
		o(mmo)
	}
	if mmo.maxLocalWaiters < 0 {
		mmo.maxLocalWaiters = 0
	}
	if mmo.allocator == nil {
		mmo.allocator = dbMutexAllocator
	}
	return &MutexMap{
		options:        mmo,
		db:             db,
		countedMutexes: make(map[string]*countedMutex),
	}
}

// Lock locks named Mutex. If not already available for the given name, an underlying
// Mutex will be allocated and kept for later use. In order to lock with a timeout pass ctx that
// has a deadline.  The returned context can be used to detect if the lock is lost.
func (mm *MutexMap) Lock(ctx context.Context, name string) (context.Context, error) {
	cm := mm.acquireReference(name, true, true)
	defer cm.decrementWaiters()
	if mm.options.maxLocalWaiters > 0 && cm.numWaiters() > mm.options.maxLocalWaiters {
		mm.releaseReference(name)
		return nil, dbmerr.NewMaxWaitersExceededError(int(mm.options.maxLocalWaiters), name)
	}
	lockExpirationCtx, err := cm.lock(ctx, mm.db, mm.options.allocator)
	if err != nil {
		// remove lock if it's not used any longer because we had an error
		mm.releaseReference(name)
	}
	return lockExpirationCtx, err
}

// Unlock unlocks the named Mutex. Once no more references (including waiting lockers) are held for
// the given name, the underlying Mutex is removed from an internal map.  So, it is likely that
// new Mutex objects are frequently allocated and released.
func (mm *MutexMap) Unlock(ctx context.Context, name string) error {
	cm := mm.acquireReference(name, false, false)
	if cm == nil {
		return dbmerr.NewNotLockedError("n/a", name)
	}
	// release reference we just acquired
	defer mm.releaseReference(name)
	// Release reference held from original locking since we are now unlocked. Release reference
	// even if there is a failure because we assume that the underlying implementation can
	// provide mutual exclusion even after an unlock failure.
	defer mm.releaseReference(name)
	return cm.unlock(ctx)
}

// acquireReference locks the map and obtains a reference to the countedMutex.
func (mm *MutexMap) acquireReference(name string, incrementWaiters bool, allocateMissing bool) *countedMutex {
	mm.lock.Lock()
	defer mm.lock.Unlock()
	cm, exists := mm.countedMutexes[name]
	if !exists {
		if !allocateMissing {
			return nil
		}
		cm = &countedMutex{
			timeoutLock: make(chan struct{}, 1),
		}
		mm.countedMutexes[name] = cm
	}
	cm.references++

	if incrementWaiters {
		cm.incrementWaiters()
	}
	return cm
}

// releaseReference locks the map, acquires the named countedMutex and removes it from the map
// if the reference count drops to zero.
func (mm *MutexMap) releaseReference(name string) {
	mm.lock.Lock()
	cm, exists := mm.countedMutexes[name]
	if !exists {
		return
	}
	cm.references--
	if cm.references == 0 {
		delete(mm.countedMutexes, name)
	}
	mm.lock.Unlock()
}

// helper for testing
func (mm *MutexMap) len() int {
	mm.lock.Lock()
	defer mm.lock.Unlock()
	return len(mm.countedMutexes)
}
