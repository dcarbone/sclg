package sclg

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// TimedCacheItem represents one entry in a timed cache
type TimedCacheItem struct {
	id      uint64
	key     string
	data    interface{}
	defunct chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

// timedCacheComparator is used when a new cache request is seen where there is already key for that item defined. If
// your implementation returns true, it is assumed the two items are equivalent and therefore no update is necessary
type TimedCacheItemEquivalencyFunc func(key string, current, new interface{}) bool

// defaultTimedCacheEquivalencyFunc will always allow the incoming item to overwrite the existing one.
func defaultTimedCacheEquivalencyFunc(_ string, _, _ interface{}) bool {
	return false
}

var (
	// this id will be incremented each time a new cache item is stored in any existing cache
	timedCacheItemID uint64
	// this pool is shared across all caches
	timedCacheItemPool sync.Pool
)

func init() {
	timedCacheItemPool = sync.Pool{New: func() interface{} { return new(TimedCacheItem) }}
}

type TimedCacheConfig struct {
	// Log [optional]
	//
	// Optionally specify a logger to get debug-level logging
	Log interface{ Printf(string, ...interface{}) }

	// Comparator [optional]
	//
	// Optionally define a comparison func that will be called when determining if an existing cached item should be
	// overwritten when calling the LoadOrStore methods
	Comparator TimedCacheItemEquivalencyFunc
}

// DefaultTimedCacheConfig will return a functional default configuration instance to you
func DefaultTimedCacheConfig() *TimedCacheConfig {
	c := new(TimedCacheConfig)
	c.Comparator = defaultTimedCacheEquivalencyFunc
	return c
}

// TimedCacheConfigMutator defines a func structure that may be provided when building a client to modify the provided config
type TimedCacheConfigMutator func(*TimedCacheConfig)

// TimedCache represents a specific-purpose cache
type TimedCache struct {
	mu    sync.RWMutex
	l     interface{ Printf(string, ...interface{}) }
	wg    *sync.WaitGroup
	cmp   TimedCacheItemEquivalencyFunc
	items map[string]*TimedCacheItem
}

// NewTimedCache will return to you a function cache instance
func NewTimedCache(conf *TimedCacheConfig, mutators ...TimedCacheConfigMutator) *TimedCache {
	tc := new(TimedCache)

	c := processConfig(conf, mutators...)
	tc.l = c.Log
	tc.wg = new(sync.WaitGroup)
	tc.cmp = c.Comparator
	tc.items = make(map[string]*TimedCacheItem)

	return tc
}

func processConfig(inc *TimedCacheConfig, mutators ...TimedCacheConfigMutator) *TimedCacheConfig {
	act := DefaultTimedCacheConfig()
	if inc == nil {
		inc = DefaultTimedCacheConfig()
	}
	for _, fn := range mutators {
		fn(inc)
	}
	act.Log = inc.Log
	if inc.Comparator != nil {
		act.Comparator = inc.Comparator
	}
	return act
}

func (tc *TimedCache) log(f string, v ...interface{}) {
	if tc.l != nil {
		tc.l.Printf(f, v...)
	}
}

func (tc *TimedCache) expireHandler(tci *TimedCacheItem) {
	// wait for context to expire
	<-tci.ctx.Done()
	tc.log("Context for item %q has expired with reason: %s", tci.id, tci.ctx.Err())

	// lock cache and determine if the item needs to be deleted or if it has already been superseded
	tc.mu.Lock()
	if curr, ok := tc.items[tci.key]; ok && curr.id == tci.id {
		tc.log("Deleting item %q from cache", tci.key)
		delete(tc.items, tci.key)
	}
	tc.mu.Unlock()

	// close internal signal chan
	close(tci.defunct)

	// decrement cache wg
	tc.wg.Done()

	// inform the masses
	tc.log("Cache item %d is being recycled", tci.id)

	// zero out
	tci.id = 0
	tci.key = ""
	tci.data = nil
	tci.defunct = nil
	tci.ctx = nil
	tci.cancel = nil

	// finally put back in the pool
	timedCacheItemPool.Put(tci)
}

func (tc *TimedCache) doStore(ctx context.Context, key string, data interface{}) {
	// fetch item from pool and populate
	tci := timedCacheItemPool.Get().(*TimedCacheItem)
	tci.id = atomic.AddUint64(&timedCacheItemID, 1)
	tci.key = key
	tci.data = data
	tci.defunct = make(chan struct{})
	tci.ctx, tci.cancel = context.WithCancel(ctx)

	// add / replace entry
	tc.items[key] = tci

	// increment wait group
	tc.wg.Add(1)

	if d, ok := tci.ctx.Deadline(); ok {
		tc.log("Storing item %q until %s", tci.id, d)
	} else {
		tc.log("Storing item %q until death")
	}

	// spin up expiration routine
	go tc.expireHandler(tci)
}

func (tc *TimedCache) doLoad(key string) (*TimedCacheItem, bool) {
	tci, ok := tc.items[key]
	return tci, ok
}

// Store will immediately place the provided key into the cache, overwriting any existing entries
func (tc *TimedCache) Store(ctx context.Context, key string, data interface{}) {
	tc.mu.Lock()
	tc.doStore(ctx, key, data)
	tc.mu.Unlock()
}

// Load will attempt to retrieve the associated data for the provided key, if found
func (tc *TimedCache) Load(key string) (interface{}, bool) {
	var (
		tci  *TimedCacheItem
		data interface{}
		ok   bool
	)
	tc.mu.RLock()
	if tci, ok = tc.doLoad(key); ok {
		data = tci.data
	}
	tc.mu.RUnlock()
	return data, ok
}

// LoadOrStore must attempt to store the provided data at the provided key.  If the key already exists, it will call
// the provided equivalency comparison func to determine if a new cache item should be created, or if the existing
// item is sufficient.
func (tc *TimedCache) LoadOrStore(ctx context.Context, key string, data interface{}) (interface{}, bool) {
	// lock immediately
	tc.mu.Lock()

	// test if there is an existing entry that needs to be overwritten
	if curr, ok := tc.doLoad(key); ok {
		if tc.cmp(key, curr.data, data) {
			tc.log("Incoming cache request for %q does not differ, using existing cached item", key)
			v := curr.data
			tc.mu.Unlock()
			// return false as we are returning existing data
			return v, false
		}
		tc.log("Incoming cache request for %q differs, expiring existing entry", key)
		curr.cancel()
	}

	// store data
	tc.doStore(ctx, key, data)

	// unlock
	tc.mu.Unlock()

	// return true as we used the new incoming data
	return data, true
}

// Len must return a count of the number of items currently in the cache
func (tc *TimedCache) Len() int {
	tc.mu.RLock()
	l := len(tc.items)
	tc.mu.RUnlock()
	return l
}

// List must return a map of the keys and their current expiration currently being stored in this cache
func (tc *TimedCache) List() map[string]time.Time {
	tc.mu.RLock()
	items := make(map[string]time.Time, len(tc.items))
	for k, tci := range tc.items {
		items[k], _ = tci.ctx.Deadline()
	}
	tc.mu.RUnlock()
	return items
}

// Remove must attempt to remove a given key from the cache.  It must block until the item has been removed, and it must
// return false if no item with that key was found
func (tc *TimedCache) Remove(key string) bool {
	var (
		tci *TimedCacheItem
		ok  bool
	)
	tc.mu.RLock()
	if tci, ok = tc.items[key]; ok {
		tci.cancel()
		done := tci.defunct
		tc.mu.RUnlock()
		<-done
	} else {
		tc.mu.RUnlock()
	}
	return ok
}

// Flush must immediately invalidate all items in the cache, returning a count of the number of items flushed
func (tc *TimedCache) Flush() int {
	var (
		tci *TimedCacheItem
		cnt int
	)
	tc.mu.RLock()
	for _, tci = range tc.items {
		cnt++
		tci.cancel()
	}
	tc.mu.RUnlock()
	tc.wg.Wait()
	return cnt
}
