package concur

import (
	"context"
	"testing"
)

// TestFromSlice validates that slices are cleanly streaming into read-only channels.
func TestFromSlice(t *testing.T) {
	ctx := context.Background()
	input := []int{10, 20, 30, 40, 50}

	ch := FromSlice(ctx, input, len(input))

	var output []int
	for item := range ch {
		output = append(output, item)
	}

	if len(output) != len(input) {
		t.Fatalf("expected length %d, got %d", len(input), len(output))
	}

	for i, v := range output {
		if v != input[i] {
			t.Errorf("at index %d: expected %d, got %d", i, input[i], v)
		}
	}
}

// TestFromSlice_Cancellation ensures the generator breaks cleanly and prevents goroutine leaks if context is closed.
func TestFromSlice_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	input := []int{1, 2, 3, 4, 5}

	// Buffer size 0 means writing will block immediately until a receiver reads.
	ch := FromSlice(ctx, input, 0)

	// Cancel context immediately before iterating
	cancel()

	// Read from channel; it should exit its worker loop and close cleanly without deadlock
	count := 0
	for range ch {
		count++
	}

	if count == len(input) {
		t.Error("expected channel to short-circuit due to context cancellation, but all items streamed through")
	}
}

// TestProcessSlice validates core fanned-out processing of a direct slice layout.
func TestProcessSlice(t *testing.T) {
	ctx := context.Background()
	input := []string{"go", "generics", "concurrency"}
	opts := Options{Workers: 2, StopOnError: false}

	resChan := ProcessSlice(ctx, input, opts, func(ctx context.Context, item string) (int, error) {
		return len(item), nil
	})

	results := make(map[int]bool)
	for res := range resChan {
		if res.Err != nil {
			t.Errorf("unexpected error inside processor pipeline: %v", res.Err)
		}
		results[res.Value] = true
	}

	expectedLengths := []int{2, 8, 11}
	for _, expected := range expectedLengths {
		if !results[expected] {
			t.Errorf("missing expected transformation length: %d", expected)
		}
	}
}

// TestProcessSlice_Empty handles the empty input boundary case natively.
func TestProcessSlice_Empty(t *testing.T) {
	ctx := context.Background()
	var input []int
	opts := Options{Workers: 4}

	resChan := ProcessSlice(ctx, input, opts, func(ctx context.Context, item int) (int, error) {
		return item * 2, nil
	})

	count := 0
	for range resChan {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 items from empty array processing, got %d", count)
	}
}
