package concur_test

import (
	"context"
	"testing"
	"time"

	"github.com/jigneshsatam/concur"
)

// simulateWork mimics a real-world blocking task like an HTTP request or I/O operation.
func simulateWork(ctx context.Context, item int) (int, error) {
	time.Sleep(1 * time.Millisecond) // Simulated processing latency
	return item * 2, nil
}

// BenchmarkSequential measures performance processing items one after another on a single thread.
func BenchmarkSequential(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Process 20 items sequentially per benchmark iteration
		for j := 0; j < 20; j++ {
			_, _ = simulateWork(context.Background(), j)
		}
	}
}

// BenchmarkConcurPool measures performance using your concur library with parallel workers.
func BenchmarkConcurPool(b *testing.B) {
	ctx := context.Background()
	opts := concur.Options{
		Workers:     4, // Leverage a pool of 4 parallel workers
		StopOnError: false,
	}

	b.ResetTimer() // Exclude setup time from the benchmark metric calculation

	for i := 0; i < b.N; i++ {
		// Stop the timer during channel setup overhead
		b.StopTimer()
		in := make(chan int, 20)
		for j := 0; j < 20; j++ {
			in <- j
		}
		close(in)
		b.StartTimer()

		// Run the concur fanned-out execution loop
		out := concur.Process(ctx, in, opts, simulateWork)

		// Drain the fanned-in channel entirely to finish execution
		for range out {
		}
	}
}

// ============================================================================
// Slice Processing Benchmarks
// ============================================================================

// mockHeavyWork simulates an I/O bound or CPU-heavy processing task (e.g., API calls, DB writes)
func mockHeavyWork(val int) int {
	time.Sleep(1 * time.Millisecond) // Artificial micro-delay to simulate real-world workloads
	return val * 2
}

// BenchmarkSequentialSlice Processing runs a standard single-threaded loop for baseline comparison
func BenchmarkSequentialSlice(b *testing.B) {
	input := make([]int, 100)
	for i := 0; i < 100; i++ {
		input[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		output := make([]int, len(input))
		for idx, item := range input {
			output[idx] = mockHeavyWork(item)
		}
	}
}

// BenchmarkConcurProcessSlice tracks the performance of your new automated slice-to-channel concurrent pipeline
func BenchmarkConcurProcessSlice(b *testing.B) {
	ctx := context.Background()
	input := make([]int, 100)
	for i := 0; i < 100; i++ {
		input[i] = i
	}

	opts := concur.Options{
		Workers:     10, // Leverages 10 concurrent routines to distribute the 100 items
		StopOnError: false,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resChan := concur.ProcessSlice(ctx, input, opts, func(ctx context.Context, item int) (int, error) {
			return mockHeavyWork(item), nil
		})

		// Drain the fanned-in pipeline completely to ensure accuracy in benchmark timing
		for range resChan {
		}
	}
}
