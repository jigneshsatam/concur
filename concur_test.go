package concur_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jigneshsatam/concur"
)

func TestProcess_TableDriven(t *testing.T) {
	type empty struct{}

	tests := []struct {
		name          string
		inputItems    []int
		options       concur.Options
		workerSetup   func(t *testing.T) concur.WorkerFunc[int, any]
		verifyResults func(t *testing.T, results []concur.Result[any])
	}{
		{
			name:       "Default workers fallback to 4",
			inputItems: []int{1, 2, 3, 4, 5},
			options:    concur.Options{Workers: 0, StopOnError: false},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				return func(ctx context.Context, item int) (any, error) {
					return item * 2, nil
				}
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) != 5 {
					t.Errorf("expected 5 results, got %d", len(results))
				}
				expectedSums := map[int]bool{2: true, 4: true, 6: true, 8: true, 10: true}
				for _, res := range results {
					if res.Err != nil {
						t.Errorf("unexpected error: %v", res.Err)
					}
					val := res.Value.(int)
					if !expectedSums[val] {
						t.Errorf("unexpected value returned: %d", val)
					}
				}
			},
		},
		{
			name:       "Stop on error halts early",
			inputItems: []int{1, 2, 3, 4, 5},
			options:    concur.Options{Workers: 1, StopOnError: true},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				return func(ctx context.Context, item int) (any, error) {
					if item == 3 {
						return nil, errors.New("halt pipeline")
					}
					return item, nil
				}
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				hasError := false
				for _, res := range results {
					if res.Err != nil {
						hasError = true
					}
				}
				if !hasError {
					t.Error("expected pipeline to record an error, but none was found")
				}
				if len(results) > 3 {
					t.Errorf("expected early stop, but processed %d items", len(results))
				}
			},
		},
		{
			name:       "Continue on error handles all elements",
			inputItems: []int{1, 2, 3, 4},
			options:    concur.Options{Workers: 2, StopOnError: false},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				return func(ctx context.Context, item int) (any, error) {
					if item%2 == 0 {
						return nil, errors.New("even number failure")
					}
					return item, nil
				}
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) != 4 {
					t.Errorf("expected exactly 4 elements handled, got %d", len(results))
				}
				errCount, successCount := 0, 0
				for _, res := range results {
					if res.Err != nil {
						errCount++
					} else {
						successCount++
					}
				}
				if errCount != 2 || successCount != 2 {
					t.Errorf("expected 2 errors and 2 successes, got %d errors and %d successes", errCount, successCount)
				}
			},
		},
		{
			name:       "Panic recovery catches panic and treats as standard error",
			inputItems: []int{1},
			options:    concur.Options{Workers: 1, StopOnError: false},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				return func(ctx context.Context, item int) (any, error) {
					panic("critical worker explosion")
				}
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) != 1 {
					t.Fatalf("expected 1 result from recovery test, got %d", len(results))
				}
				res := results[0]
				if res.Err == nil {
					t.Fatal("expected error from panic recovery, got nil")
				}
				if !strings.Contains(res.Err.Error(), "worker panicked: critical worker explosion") {
					t.Errorf("unexpected panic error format: %v", res.Err)
				}
			},
		},
		{
			name:       "Verify concurrency pooling worker limits",
			inputItems: make([]int, 50),
			options:    concur.Options{Workers: 3, StopOnError: false},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				var activeWorkers int32
				var maxWorkers int32

				return func(ctx context.Context, item int) (any, error) {
					current := atomic.AddInt32(&activeWorkers, 1)
					defer atomic.AddInt32(&activeWorkers, -1)

					for {
						max := atomic.LoadInt32(&maxWorkers)
						if current <= max || atomic.CompareAndSwapInt32(&maxWorkers, max, current) {
							break
						}
					}
					time.Sleep(1 * time.Millisecond)

					if atomic.LoadInt32(&maxWorkers) > 3 {
						t.Errorf("concurrency violation! Max concurrent workers detected: %d", maxWorkers)
					}
					return item, nil
				}
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) != 50 {
					t.Errorf("expected 50 elements drained, got %d", len(results))
				}
			},
		},
		// --- FIXES FOR THE 12 CLOSURE PATTERNS AS SERIALLY INDEXED BOUNDS ---
		{
			name:       "Closure Pattern 1: func(ctx, In) (Out, error)",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(c context.Context, i int) (string, error) { return fmt.Sprintf("val-%d", i), nil }
				return func(ctx context.Context, item int) (any, error) { return impl(ctx, item) }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "val-10" {
					t.Error("Pattern 1 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 2: func(ctx, In) Out",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(c context.Context, i int) string { return fmt.Sprintf("val-%d", i) }
				return func(ctx context.Context, item int) (any, error) { return impl(ctx, item), nil }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "val-10" || results[0].Err != nil {
					t.Error("Pattern 2 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 3: func(ctx, In) error",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(c context.Context, i int) error { return errors.New("p3-err") }
				return func(ctx context.Context, item int) (any, error) { return empty{}, impl(ctx, item) }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Err == nil || results[0].Err.Error() != "p3-err" {
					t.Error("Pattern 3 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 4: func(ctx, In)",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(c context.Context, i int) {}
				return func(ctx context.Context, item int) (any, error) { impl(ctx, item); return empty{}, nil }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Err != nil {
					t.Error("Pattern 4 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 5: func(In) (Out, error)",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(i int) (string, error) { return "ok", nil }
				return func(ctx context.Context, item int) (any, error) { return impl(item) }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "ok" {
					t.Error("Pattern 5 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 6: func(In) Out",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(i int) string { return "ok" }
				return func(ctx context.Context, item int) (any, error) { return impl(item), nil }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "ok" {
					t.Error("Pattern 6 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 7: func(In) error",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(i int) error { return errors.New("err") }
				return func(ctx context.Context, item int) (any, error) { return empty{}, impl(item) }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Err == nil {
					t.Error("Pattern 7 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 8: func(In)",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func(i int) {}
				return func(ctx context.Context, item int) (any, error) { impl(item); return empty{}, nil }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Err != nil {
					t.Error("Pattern 8 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 9: func() (Out, error)",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func() (string, error) { return "gen", nil }
				return func(ctx context.Context, _ int) (any, error) { return impl() }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "gen" {
					t.Error("Pattern 9 failed")
				}
			},
		},
		{
			name:       "Closure Pattern 10: func() Out",
			inputItems: []int{10},
			options:    concur.Options{Workers: 1},
			workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
				impl := func() string { return "gen" }
				return func(ctx context.Context, _ int) (any, error) { return impl(), nil }
			},
			verifyResults: func(t *testing.T, results []concur.Result[any]) {
				if len(results) == 0 || results[0].Value.(string) != "gen" {
					t.Error("Pattern 10 failed")
				}
			}}, {name: "Closure Pattern 11: func() error", inputItems: []int{10}, options: concur.Options{Workers: 1}, workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
			impl := func() error { return errors.New("err") }
			return func(ctx context.Context, _ int) (any, error) { return empty{}, impl() }
		}, verifyResults: func(t *testing.T, results []concur.Result[any]) {
			if len(results) == 0 || results[0].Err == nil {
				t.Error("Pattern 11 failed")
			}
		}}, {name: "Closure Pattern 12: func()", inputItems: []int{10}, options: concur.Options{Workers: 1}, workerSetup: func(t *testing.T) concur.WorkerFunc[int, any] {
			impl := func() {}
			return func(ctx context.Context, _ int) (any, error) { impl(); return empty{}, nil }
		}, verifyResults: func(t *testing.T, results []concur.Result[any]) {
			if len(results) == 0 || results[0].Err != nil {
				t.Error("Pattern 12 failed")
			}
		}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inChan := make(chan int, len(tt.inputItems))
			for _, item := range tt.inputItems {
				inChan <- item
			}
			close(inChan)
			workerFn := tt.workerSetup(t)
			outChan := concur.Process(context.Background(), inChan, tt.options, workerFn)
			var results []concur.Result[any]
			for res := range outChan {
				results = append(results, res)
			}
			tt.verifyResults(t, results)
		})
	}
}
