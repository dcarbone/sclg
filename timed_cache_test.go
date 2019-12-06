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
}
