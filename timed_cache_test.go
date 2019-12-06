package sclg_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

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

func TestTimedCache(t *testing.T) {
	t.Run("store-load", func(t *testing.T) {
		tc := sclg.NewTimedCache(nil)
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
		tc := sclg.NewTimedCache(nil)
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
		f := func(_ev sclg.TimedCacheEvent, _key, _message string) {
			ev = _ev
			key = _key
			message = _message
			called <- struct{}{}
		}

		cfg.StoredEventCallback = f
		cfg.RemovedEventCallback = f

		tc := sclg.NewTimedCache(cfg)
		k, v := kvp()
		tc.Store(context.Background(), k, v)
		select {
		case <-called:
		default:
			t.Logf("store function was not called")
			t.Fail()
		}
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
		// now remove
		tc.Remove(k)
		select {
		case <-called:
		default:
			t.Logf("remove callback was not called")
			t.Fail()
		}
		if ev != sclg.TimedCacheEventRemoved {
			t.Logf("espected removed event, got %s", ev)
			t.Fail()
		} else {
			t.Logf("message was %s", message)
		}
	})
}
