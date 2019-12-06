package sclg_test

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dcarbone/sclg"
)

const (
	defaultMax  = 100
	defaultMax2 = defaultMax * 2
)

func kvp(max int) (string, int) {
	v := rand.Intn(max)
	return strconv.Itoa(v), v
}

func TestTimedCache(t *testing.T) {
	t.Run("store-load", func(t *testing.T) {
		tc := sclg.NewTimedCache(nil)
		defer tc.Flush()
		k, v := kvp(defaultMax)
		tc.Store(context.Background(), k, v)
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
		tc := sclg.NewTimedCache(nil)
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
		tc := sclg.NewTimedCache(nil)
		defer tc.Flush()
		k, v1 := kvp(defaultMax)
		v2 := v1 + 1
		tc.Store(context.Background(), k, v1)
		tc.Store(context.Background(), k, v2)
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
		var (
			called  = make(chan struct{}, 1)
			ev      sclg.TimedCacheEvent
			key     string
			message string
		)
		defer func() {
			close(called)
			if len(called) > 0 {
				<-called
			}
		}()
		f := func(_ev sclg.TimedCacheEvent, _key, _message string) {
			ev = _ev
			key = _key
			message = _message
			called <- struct{}{}
		}

		cfg.StoredEventCallback = f
		cfg.RemovedEventCallback = f

		tc := sclg.NewTimedCache(cfg)
		k, v := kvp(defaultMax)
		tc.Store(context.Background(), k, v)
		defer tc.Flush()
		select {
		case <-called:
			if ev != sclg.TimedCacheEventStored {
				t.Logf("unexpected ev: %s", ev)
				t.Fail()
			} else {
				t.Logf("message was %s", message)
			}
			if key != k {
				t.Logf("key %s was not expected value of %s", key, k)
				t.Fail()
			}
		case <-time.After(time.Second):
			t.Logf("store function was not called")
			t.FailNow()
		}
		// now remove
		tc.Remove(k)
		select {
		case <-called:
			if ev != sclg.TimedCacheEventRemoved {
				t.Logf("espected removed event, got %s", ev)
				t.Fail()
			} else {
				t.Logf("message was %s", message)
			}
		case <-time.After(time.Second):
			t.Logf("remove callback was not called")
			t.FailNow()
		}
	})

	t.Run("concurrent-read-write", func(t *testing.T) {
		rand.Seed(time.Now().UnixNano())
		var (
			zero, one, two uint64
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
			c.Comparator = func(_ string, current, new interface{}) bool {
				return rand.Intn(2)%2 == 0
			}
		})
		defer tc.Flush()

		// spin up a few routines
		for n := 0; n < routines; n++ {
			wg.Add(1)
			go func() {
				var (
					k      string
					v      int
					ok     bool
					stored bool
				)
				for i := 0; i < routineLoops; i++ {
					k, v = kvp(defaultMax)
					switch rand.Intn(3) {
					case 0:
						tc.Store(context.Background(), k, v)
						atomic.AddUint64(&zero, 1)
						atomic.AddUint64(&storeCnt, 1)
					case 1:
						atomic.AddUint64(&one, 1)
						if _, ok = tc.Load(k); !ok {
							atomic.AddUint64(&missCnt, 1)
						} else {
							atomic.AddUint64(&hitCnt, 1)
						}
					case 2:
						atomic.AddUint64(&two, 1)
						if _, stored = tc.LoadOrStore(context.Background(), k, v); stored {
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

		t.Logf("zero: %d; one: %d; two: %d", zero, one, two)
		t.Logf("stored: %d; hits: %d; misses: %d; overwritten: %d; reused %d", storeCnt, hitCnt, missCnt, overwrittenCnt, reusedCnt)
	})
}
