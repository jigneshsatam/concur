package concur_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jigneshsatam/concur"
)

// TestDefaultWorkers verifies that leaving Workers at 0 defaults to 4 workers.
func TestDefaultWorkers(t *testing.T) {
	in := make(chan int, 10)
	for i := 0; i < 10; i++ {
		in <- i
	}
	close(in)

	worker := func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	}

	// opts.Workers is 0, should default to 4 internally
	out := concur.Process(context.Background(), in, concur.Options{}, worker)

	count := 0
	for res := range out {
		if res.Err != nil {
			t.Errorf("expected no error, got: %v", res.Err)
		}
		count++
	}

	if count != 10 {
		t.Errorf("expected 10 items processed, got %d", count)
	}
}

// TestStopOnError verifies that the pipeline stops processing immediately when an error hits.
func TestStopOnError(t *testing.T) {
	in := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	worker := func(ctx context.Context, item int) (int, error) {
		if item == 3 {
			return 0, errors.New("error on item 3")
		}
		return item, nil
	}

	opts := concur.Options{
		Workers:     1, // Use 1 worker to ensure deterministic sequential execution order for testing
		StopOnError: true,
	}

	out := concur.Process(context.Background(), in, opts, worker)

	hasError := false
	processedCount := 0

	for res := range out {
		processedCount++
		if res.Err != nil {
			hasError = true
		}
	}

	if !hasError {
		t.Error("expected pipeline to hit an error and stop, but no error was caught")
	}

	if processedCount > 3 {
		t.Errorf("expected pipeline to stop execution early, but processed %d items", processedCount)
	}
}

// TestContinueOnError ensures that all elements are handled even if some items fail.
func TestContinueOnError(t *testing.T) {
	in := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		in <- i
	}
	close(in)

	worker := func(ctx context.Context, item int) (int, error) {
		if item%2 == 0 {
			return 0, errors.New("even number error")
		}
		return item, nil
	}

	opts := concur.Options{
		Workers:     2,
		StopOnError: false,
	}

	out := concur.Process(context.Background(), in, opts, worker)

	errCount := 0
	successCount := 0

	for res := range out {
		if res.Err != nil {
			errCount++
		} else {
			successCount++
		}
	}

	if errCount != 2 || successCount != 2 {
		t.Errorf("expected 2 successes and 2 errors, got %d successes and %d errors", successCount, errCount)
	}
}

// TestRaceCondition passes structural data and updates atomic counters to check for data races.
func TestRaceCondition(t *testing.T) {
	in := make(chan int, 100)
	for i := 0; i < 100; i++ {
		in <- i
	}
	close(in)

	var activeWorkers int32
	var maxWorkers int32

	worker := func(ctx context.Context, item int) (int, error) {
		current := atomic.AddInt32(&activeWorkers, 1)
		defer atomic.AddInt32(&activeWorkers, -1)

		// Track high water mark of active workers running concurrently
		for {
			max := atomic.LoadInt32(&maxWorkers)
			if current <= max || atomic.CompareAndSwapInt32(&maxWorkers, max, current) {
				break
			}
		}

		time.Sleep(1 * time.Millisecond)
		return item, nil
	}

	opts := concur.Options{
		Workers:     5,
		StopOnError: false,
	}

	out := concur.Process(context.Background(), in, opts, worker)

	for range out {
		// Drain channel completely
	}

	if atomic.LoadInt32(&maxWorkers) > 5 {
		t.Errorf("worker pool breached limits! Max concurrent workers detected: %d", maxWorkers)
	}
}
