package sclg_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/dcarbone/sclg"
)

type testLogger struct {
	t *testing.T
}

func (tl *testLogger) Printf(f string, v ...interface{}) {
	tl.t.Logf(f, v...)
}

func kvp() (string, int) {
	v := rand.Intn(100)
	return strconv.Itoa(v), v
}

func newTC(t *testing.T, cfg *sclg.TimedCacheConfig) *sclg.TimedCache {
	return sclg.NewTimedCache(cfg, func(c *sclg.TimedCacheConfig) { c.Log = &testLogger{t} })
}

func TestTimedCache(t *testing.T) {
	t.Run("store-load", func(t *testing.T) {
		tc := newTC(t, nil)
		k, v := kvp()
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
		tc := newTC(t, nil)
		if v, ok := tc.Load("nope"); ok {
			t.Logf("Key %q shouldn't exist", "nope")
			t.Fail()
		} else if v != nil {
			t.Logf("v should be empty with undefined key, saw %v", v)
			t.Fail()
		}
	})

	t.Run("load-overwrite", func(t *testing.T) {
		tc := newTC(t, nil)
		k, v1 := kvp()
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

		tc := newTC(t, cfg)
		k, v := kvp()
		tc.Store(context.Background(), k, v)
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
}
