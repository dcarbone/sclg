package sclg

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// this id will be incremented each time a new cache item is stored in any existing cache
	timedCacheItemID uint64
	// this pool is shared across all caches
	timedCacheItemPool sync.Pool
)

// timedCacheItem represents one entry in a timed cache
type timedCacheItem struct {
	mu   sync.Mutex
	id   uint64
	key  interface{}
	data interface{}
	dl   time.Time
	exp  chan struct{}
	expd bool
}

func (tci *timedCacheItem) expire() {
	tci.mu.Lock()
	if !tci.expd {
		tci.expd = true
		close(tci.exp)
	}
	tci.mu.Unlock()
}

func (tci *timedCacheItem) expired() bool {
	tci.mu.Lock()
	e := tci.expd
	tci.mu.Unlock()
	return e
}

func (tci *timedCacheItem) wait() <-chan struct{} {
	return tci.exp
}

func init() {
	timedCacheItemPool = sync.Pool{
		New: func() interface{} { return new(timedCacheItem) }}
}

func recycle(tci *timedCacheItem) {
	// zero out
	tci.id = 0
	tci.key = ""
	tci.data = nil
	tci.dl = time.Time{}
	tci.exp = nil

	// finally put back in the pool
	timedCacheItemPool.Put(tci)
}

// PurgeFunc is used to selectively purge multiple items from the cache.
//
// - if "removeKey" is true, the key may be removed if it is still present by the time the call is made
// - if "nextKey" is false, once the current key has been handled the call will end
type PurgeFunc func(key interface{}, ttl time.Time) (removeKey bool, nextKey bool)

// TimedCacheEvent
//
// These are events that represent significant events in the lifecycle of an item within this cache.  More will be
// added as they are needed
type TimedCacheEvent int

const (
	TimedCacheEventStored TimedCacheEvent = iota
	TimedCacheEventRemoved
)

// String must return a printable human-understandable descriptor of the event
func (ev TimedCacheEvent) String() string {
	switch ev {
	case TimedCacheEventStored:
		return "stored"
	case TimedCacheEventRemoved:
		return "removed"

	default:
		return fmt.Sprintf("UKNOWN (%d)", ev)
	}
}

// TimedCacheItemEquivalencyFunc is used when a new cache request is seen where there is already key for that item defined. If
// your implementation returns true, it is assumed the two items are equivalent and therefore no update is necessary
type TimedCacheItemEquivalencyFunc func(key, current, new interface{}) bool

// TimedCacheEventCallback is called any time a significant event happens to a particular item in the cache
type TimedCacheEventCallback func(ev TimedCacheEvent, key interface{}, message string)

// defaultTimedCacheEquivalencyFunc will always allow the incoming item to overwrite the existing one.
func defaultTimedCacheEquivalencyFunc(_, _, _ interface{}) bool {
	return false
}

// TimedCacheConfig is used to define a given TimedCache instance
type TimedCacheConfig struct {
	// Log [optional]
	//
	// Optionally specify a logger to get debug-level logging
	Log *log.Logger

	// Comparator [optional]
	//
	// Optionally define a comparison func that will be called when determining if an existing cached item should be
	// overwritten when calling the LoadOrStore methods
	Comparator TimedCacheItemEquivalencyFunc

	// StoredEventCallback [optional]
	//
	// If defined, this will be called each time an entry has been persisted, after persistence has happened
	StoredEventCallback TimedCacheEventCallback

	// RemovedEventCallback [optional]
	//
	// If defined, this will be called each time an entry has been removed from the cache, after it has been removed.
	RemovedEventCallback TimedCacheEventCallback
}

// DefaultTimedCacheConfig will return a functional default configuration instance to you
func DefaultTimedCacheConfig() *TimedCacheConfig {
	c := new(TimedCacheConfig)
	c.Comparator = defaultTimedCacheEquivalencyFunc
	return c
}

// TimedCacheConfigMutator defines a func structure that may be provided when building a client to modify the provided
// config
type TimedCacheConfigMutator func(*TimedCacheConfig)

// TimedCache represents a specific-purpose cache
type TimedCache struct {
	mu    sync.RWMutex
	log   *log.Logger
	wg    *sync.WaitGroup
	cmp   TimedCacheItemEquivalencyFunc
	items map[interface{}]*timedCacheItem

	sl TimedCacheEventCallback
	rl TimedCacheEventCallback
}

// NewTimedCache will return to you a function cache instance
func NewTimedCache(conf *TimedCacheConfig, mutators ...TimedCacheConfigMutator) *TimedCache {
	tc := new(TimedCache)

	c := processConfig(conf, mutators...)
	tc.log = c.Log
	tc.wg = new(sync.WaitGroup)
	tc.cmp = c.Comparator
	tc.items = make(map[interface{}]*timedCacheItem)

	tc.sl = c.StoredEventCallback
	tc.rl = c.RemovedEventCallback

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
	if inc.Log != nil {
		act.Log = inc.Log
	} else {
		act.Log = log.New(ioutil.Discard, "", 0)
	}
	if inc.Comparator != nil {
		act.Comparator = inc.Comparator
	}
	if inc.StoredEventCallback != nil {
		act.StoredEventCallback = inc.StoredEventCallback
	}
	if inc.RemovedEventCallback != nil {
		act.RemovedEventCallback = inc.RemovedEventCallback
	}
	return act
}

func (tc *TimedCache) expireHandler(tci *timedCacheItem) {
	// wait for context to expire
	if !tci.dl.IsZero() {
		go func() {
			timer := time.NewTimer(tci.dl.Sub(time.Now()))
			<-timer.C
			tci.expire()
		}()
	}

	<-tci.wait()

	// build message for log and event listener
	key := tci.key
	msg := fmt.Sprintf("Item %v (%d) has expired", tci.key, tci.id)

	// tell the world
	tc.log.Printf(msg)

	// lock cache and determine if the item needs to be deleted or if it has already been superseded
	tc.mu.Lock()

	if curr, ok := tc.items[tci.key]; ok {
		if curr.id == tci.id {
			tc.log.Printf("Deleting item %v (%d) from cache", tci.key, tci.id)
			delete(tc.items, tci.key)
		} else {
			tc.log.Printf("Item %v (%d) has been superseded by %d", tci.key, tci.id, curr.id)
		}
	}

	tc.mu.Unlock()

	// decrement cache wg
	tc.wg.Done()

	// inform the masses
	tc.log.Printf("Cache item %d is being recycled", tci.id)

	// send off to recycle bin
	recycle(tci)

	// call listener, if defined
	if tc.rl != nil {
		tc.rl(TimedCacheEventRemoved, key, msg)
	}
}

func (tc *TimedCache) doLoad(key interface{}) (*timedCacheItem, bool) {
	if tci, ok := tc.items[key]; ok && !tci.expired() {
		return tci, ok
	}
	return nil, false
}

func (tc *TimedCache) doStore(deadline time.Time, key, data interface{}, force bool) (*timedCacheItem, bool) {
	// test if there is an existing entry
	if curr, ok := tc.doLoad(key); ok {
		// if this is not a forced overwrite, call equality comparison func to see if a replace needs to be done.
		if !force && tc.cmp(key, curr.data, data) {
			tc.log.Printf("Incoming cache request for %q does not differ, using existing cached item", key)
			// return current item and false as we are returning existing data
			return curr, false
		}
		tc.log.Printf("Incoming cache request for %q differs, expiring existing entry", key)
		curr.expire()
	}

	// fetch item from pool and populate
	tci := timedCacheItemPool.Get().(*timedCacheItem)
	tci.key = key
	tci.data = data
	tci.dl = deadline
	tci.id = atomic.AddUint64(&timedCacheItemID, 1)
	tci.exp = make(chan struct{})
	tci.expd = false

	// add / replace entry
	tc.items[key] = tci

	// increment wait group
	tc.wg.Add(1)

	// build msg for event
	var msg string
	if deadline.IsZero() {
		msg = fmt.Sprintf("Item %q (%d) has been persisted until replaced", key, tci.id)
	} else {
		msg = fmt.Sprintf("Item %q (%d) has been persisted until %s", key, tci.id, deadline)
	}

	// spin up expiration routine
	go tc.expireHandler(tci)

	// tell the world
	tc.log.Printf(msg)

	// call listener, if defined
	if tc.sl != nil {
		tc.sl(TimedCacheEventStored, key, msg)
	}

	return tci, true
}

// StoreUntil will immediately place the provided key into the cache until the provided deadline is breached
func (tc *TimedCache) StoreUntil(deadline time.Time, key, data interface{}) {
	tc.mu.Lock()
	tc.doStore(deadline, key, data, true)
	tc.mu.Unlock()
}

// StoreFor will immediately place the provided key into the cache for the specified duration
func (tc *TimedCache) StoreFor(ttl time.Duration, key, data interface{}) {
	tc.StoreUntil(time.Now().Add(ttl), key, data)
}

// Store will immediately place the provided key into the cache with an infinite ttl
func (tc *TimedCache) Store(key, data interface{}) {
	tc.StoreUntil(time.Time{}, key, data)
}

// Load will attempt to retrieve the associated data for the provided key, if found
func (tc *TimedCache) Load(key interface{}) (interface{}, bool) {
	var (
		tci  *timedCacheItem
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

// LoadOrStoreUntil attempts to store the provided data at the provided key.  If the key already exists, it will call
// the provided equivalency comparison func to determine if a new cache item should be created, or if the existing
// item is sufficient.
func (tc *TimedCache) LoadOrStoreUntil(deadline time.Time, key, data interface{}) (interface{}, bool) {
	var (
		tci *timedCacheItem
		act interface{}
		ok  bool
	)
	tc.mu.Lock()
	// call doStore without force, indicating we may need to return a value other than the one provided
	if tci, ok = tc.doStore(deadline, key, data, false); ok {
		act = data
	} else {
		act = tci.data
	}
	tc.mu.Unlock()
	return act, ok

}

// LoadOrStoreFor executes a LoadOrStoreUntil call with a deadline of now + ttl
func (tc *TimedCache) LoadOrStoreFor(ttl time.Duration, key, data interface{}) (interface{}, bool) {
	return tc.LoadOrStoreUntil(time.Now().Add(ttl), key, data)
}

// LoadOrStore executes a LoadOrStoreUntil call with an infinite context ttl
func (tc *TimedCache) LoadOrStore(key, data interface{}) (interface{}, bool) {
	return tc.LoadOrStoreUntil(time.Time{}, key, data)
}

// Len must return a count of the number of items currently in the cache
func (tc *TimedCache) Len() int {
	tc.mu.RLock()
	l := len(tc.items)
	tc.mu.RUnlock()
	return l
}

// List must return a map of the keys and their current expiration currently being stored in this cache
func (tc *TimedCache) List() map[interface{}]time.Time {
	tc.mu.RLock()
	items := make(map[interface{}]time.Time, len(tc.items))
	for k, tci := range tc.items {
		items[k] = tci.dl
	}
	tc.mu.RUnlock()
	return items
}

// Purge allows you to selectively remove multiple keys based on the result of the provided func.  The return value
// will only count those keys _actually_ removed, not just requested.  It is entirely possible that by the time your
// func returns for the key to have already expired by some other means.
func (tc *TimedCache) Purge(fn PurgeFunc) int {
	var (
		r, c bool
		cnt  int
	)
	if fn == nil {
		return 0
	}
	for key, t := range tc.List() {
		r, c = fn(key, t)
		if r {
			if tc.Remove(key) {
				cnt++
			}
		}
		if !c {
			break
		}
	}
	return cnt
}

// Remove must attempt to remove a given key from the cache.  It must block until the item has been removed, and it must
// return false if no item with that key was found
func (tc *TimedCache) Remove(key interface{}) bool {
	var (
		tci *timedCacheItem
		ok  bool
	)
	tc.mu.RLock()
	if tci, ok = tc.items[key]; ok {
		done := tci.wait()
		tci.expire()
		tc.mu.RUnlock()
		<-done
	} else {
		tc.mu.RUnlock()
	}
	return ok
}

// Delete retains API compatibility with sync.Map
func (tc *TimedCache) Delete(key interface{}) {
	tc.Remove(key)
}

// Flush must immediately invalidate all items in the cache, returning a count of the number of items flushed
func (tc *TimedCache) Flush() int {
	var (
		tci *timedCacheItem
		cnt int
	)
	tc.mu.RLock()
	for _, tci = range tc.items {
		cnt++
		tc.log.Printf("Expiring %v...", tci.key)
		tci.expire()
	}
	tc.mu.RUnlock()
	tc.wg.Wait()
	return cnt
}
