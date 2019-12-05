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

func TestTimedCache(t *testing.T) {
	// TODO: do better...

	tc := sclg.NewTimedCache(nil)

	t.Run("load-or-store", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < 100; i++ {
			go func(i int) {
				v := rand.Intn(100)
				k := strconv.Itoa(v)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				v2, _ := tc.LoadOrStore(ctx, k, v)
				if v2i, ok := v2.(int); !ok {
					t.Logf("Routine %d: Expected LoadOrStore to return int, saw %T", i, v2)
					t.Fail()
				} else if v2i != v {
					t.Logf("Routine %d: Expected LoadOrStore value to return %d, saw %d", i, v, v2i)
					t.Fail()
				}
			}(i)
		}
	})

	t.Run("load", func(t *testing.T) {
		t.Parallel()
		for i := 0; i < 100; i++ {
			go func(i int) {
				k := rand.Intn(100)
				if v, ok := tc.Load(strconv.Itoa(k)); ok {
					if vi, ok := v.(int); !ok {
						t.Logf("Routine %d: Expected key %q to be int, saw %T", i, k, v)
						t.Fail()
					} else if vi != k {
						t.Logf("Routine %d: Expected key %q to have value %d, saw %d", i, k, k, vi)
						t.Fail()
					}
				}
			}(i)
		}
	})
}
