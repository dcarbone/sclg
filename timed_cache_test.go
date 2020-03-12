package sclg_test

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dcarbone/sclg/v2"
)

const (
	defaultMax  = 100
	defaultMax2 = defaultMax * 2
)

func kvp(max int) (string, int) {
	v := rand.Intn(max)
	return strconv.Itoa(v), v
}

func newBasicTimedCache(mu ...sclg.TimedCacheConfigMutator) *sclg.TimedCache {
	mu = append(mu, func(config *sclg.TimedCacheConfig) {
		config.Log = log.New(os.Stdout, "--> ", log.LstdFlags)
	})
	return sclg.NewTimedCache(nil, mu...)
}

func TestTimedCache(t *testing.T) {
	t.Run("store-load", func(t *testing.T) {
		tc := newBasicTimedCache()
		defer tc.Flush()
		k, v := kvp(defaultMax)
		tc.Store(k, v)
		if lv, ok := tc.Load(k); !ok {
			t.Logf("Key %q expected to be %d, saw %v", k, v, lv)
			t.Fail()
		} else if i, ok := lv.(int); !ok {
			t.Logf("Key %q value changed from %T (%d) to %T (%v)", k, v, v, lv, lv)
			t.Fail()
		} else if i != v {
			t.Logf("Key %q value changed from %d to %d", k, v, i)
			t.Fail()
		}
	})

	t.Run("load-empty", func(t *testing.T) {
		tc := newBasicTimedCache()
		defer tc.Flush()
		if v, ok := tc.Load("nope"); ok {
			t.Logf("Key %q shouldn't exist", "nope")
			t.Fail()
		} else if v != nil {
			t.Logf("v should be empty with undefined key, saw %v", v)
			t.Fail()
		}
	})

	t.Run("load-overwrite", func(t *testing.T) {
		tc := newBasicTimedCache()
		defer tc.Flush()
		k, v1 := kvp(defaultMax)
		v2 := v1 + 1
		tc.Store(k, v1)
		tc.Store(k, v2)
		if v, ok := tc.Load(k); !ok {
			t.Logf("Key %q was not found after subsequent store call", k)
			t.Fail()
		} else if v.(int) != v2 {
			t.Logf("Key %q was not set to updated value %d: %v", k, v2, v)
			t.Fail()
		}
	})

	t.Run("store-remove-event-callback", func(t *testing.T) {
		cfg := sclg.DefaultTimedCacheConfig()
		cfg.Log = log.New(os.Stdout, "--> ", log.LstdFlags)
		var (
			called  = make(chan struct{}, 1)
			ev      sclg.TimedCacheEvent
			key     interface{}
			message string
		)
		defer func() {
			close(called)
			if len(called) > 0 {
				<-called
			}
		}()
		f := func(_ev sclg.TimedCacheEvent, _key interface{}, _message string) {
			ev = _ev
			key = _key
			message = _message
			called <- struct{}{}
		}

		cfg.StoredEventCallback = f
		cfg.RemovedEventCallback = f

		tc := sclg.NewTimedCache(cfg)
		k, v := kvp(defaultMax)
		tc.Store(k, v)
		defer tc.Flush()
		select {
		case <-called:
			if ev != sclg.TimedCacheEventStored {
				t.Logf("unexpected ev: %s", ev)
				t.Fail()
			} else {
				t.Logf("store - message was: %s", message)
			}
			if key != k {
				t.Logf("key %s was not expected value of %s", key, k)
				t.Fail()
			}
		case <-time.After(time.Second):
			t.Logf("store function was not called")
			t.FailNow()
		}

		t.Logf("Removing key %q...", k)

		// now remove
		tc.Remove(k)
		select {
		case <-called:
			if ev != sclg.TimedCacheEventRemoved {
				t.Logf("espected removed event, got %s", ev)
				t.Fail()
			} else {
				t.Logf("remove - message was: %s", message)
			}
		case <-time.After(time.Second):
			t.Logf("remove callback was not called")
			t.FailNow()
		}
	})

	t.Run("concurrent-read-write", func(t *testing.T) {
		// TODO: this seems...simplistic.
		rand.Seed(time.Now().UnixNano())
		var (
			storeCnt       uint64
			hitCnt         uint64
			missCnt        uint64
			overwrittenCnt uint64
			reusedCnt      uint64

			routines     = 1000
			routineLoops = 50
			wg           = new(sync.WaitGroup)
		)

		// new empty cache
		tc := sclg.NewTimedCache(nil, func(c *sclg.TimedCacheConfig) {
			c.Comparator = func(_, current, new interface{}) bool {
				return rand.Intn(2)%2 == 0
			}
		})
		defer tc.Flush()

		// spin up a few routines
		for n := 0; n < routines; n++ {
			wg.Add(1)
			go func() {
				var (
					k      interface{}
					v      int
					ok     bool
					stored bool
				)
				for i := 0; i < routineLoops; i++ {
					k, v = kvp(defaultMax)
					switch rand.Intn(3) {
					case 0:
						tc.Store(k, v)
						atomic.AddUint64(&storeCnt, 1)
					case 1:
						if _, ok = tc.Load(k); !ok {
							atomic.AddUint64(&missCnt, 1)
						} else {
							atomic.AddUint64(&hitCnt, 1)
						}
					case 2:
						if _, stored = tc.LoadOrStore(k, v); stored {
							atomic.AddUint64(&overwrittenCnt, 1)
						} else {
							atomic.AddUint64(&reusedCnt, 1)
						}
					}
				}
				wg.Done()
			}()
		}

		wg.Wait()

		t.Logf("cache - items: %d", tc.Len())
		t.Logf(
			"calls - store: %d; load: %d (hit %f%%); loadOrStore: %d (overwritten %f%%)",
			storeCnt,
			hitCnt+missCnt,
			(float32(hitCnt)/float32(hitCnt+missCnt))*100,
			overwrittenCnt+reusedCnt,
			(float32(overwrittenCnt)/float32(overwrittenCnt+reusedCnt))*100,
		)
	})
}
