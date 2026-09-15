package concur

import (
	"context"
	"fmt"
	"sync"
)

// Result wraps the fanned-in worker output data and its error state.
type Result[Out any] struct {
	Value Out
	Err   error
}

// Options configures the operational pipeline behavior.
type Options struct {
	// Workers sets the maximum size of the execution goroutine pool.
	// If left at 0, it defaults to 4 workers.
	Workers int

	// If true, the first error (or panic) halts the entire pipeline instantly.
	// If false, errored items pass through, and processing continues.
	StopOnError bool
}

// WorkerFunc accepts a context and an input item, returning a result and an error.
type WorkerFunc[In any, Out any] func(ctx context.Context, item In) (Out, error)

// Process runs a pool of workers to concurrently process data using the provided options.
func Process[In any, Out any](
	ctx context.Context,
	in <-chan In,
	opts Options,
	workerFn WorkerFunc[In, Out],
) <-chan Result[Out] {
	// Create a cancelable child context triggered if StopOnError is true
	pipelineCtx, cancelPipeline := context.WithCancel(ctx)

	// Fallback safely to a baseline default pool size of 4 workers
	if opts.Workers <= 0 {
		opts.Workers = 4
	}

	// Buffer the output channel by the worker count to guarantee short-circuiting without deadlocks
	out := make(chan Result[Out], opts.Workers)
	var wg sync.WaitGroup

	// 1. FAN-OUT: Distribute pool workloads to multiple worker goroutines
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-pipelineCtx.Done():
					return // Gracefully exit if pipeline is canceled elsewhere
				case item, ok := <-in:
					if !ok {
						return // Input queue completely exhausted
					}

					// Execute business logic with panic recovery shielding
					var val Out
					var err error

					func() {
						defer func() {
							if r := recover(); r != nil {
								// Convert the panic interface into a clean standard error
								err = fmt.Errorf("worker panicked: %v", r)
							}
						}()
						val, err = workerFn(pipelineCtx, item)
					}()

					res := Result[Out]{Value: val, Err: err}

					select {
					case <-pipelineCtx.Done():
						return
					case out <- res: // Send the result to fanned-in output
						// If an error or panic happened and StopOnError is true, halt the world
						if err != nil && opts.StopOnError {
							cancelPipeline()
							return
						}
					}
				}
			}
		}()
	}

	// 2. FAN-IN: Wait for all workers to shut down, clean up context, then close output
	go func() {
		wg.Wait()
		cancelPipeline()
		close(out)
	}()

	return out
}

// FromSlice safely converts a slice into a read-only channel asynchronously.
// The channel closes automatically when all items are sent or if the context is cancelled.
func FromSlice[T any](ctx context.Context, slice []T, bufferSize int) <-chan T {
	out := make(chan T, bufferSize)

	go func() {
		defer close(out)
		for _, item := range slice {
			select {
			case <-ctx.Done():
				return
			case out <- item:
			}
		}
	}()

	return out
}

// ProcessSlice accepts a raw slice, wraps it using FromSlice, and feeds it
// directly into the core Process runner.
func ProcessSlice[In any, Out any](
	ctx context.Context,
	inputSlice []In,
	opts Options,
	workerFunc func(context.Context, In) (Out, error),
) <-chan Result[Out] {

	// If the slice is completely empty, short-circuit immediately
	// to avoid spinning up unnecessary background threads.
	if len(inputSlice) == 0 {
		out := make(chan Result[Out])
		close(out)
		return out
	}

	// Clean code reuse: Convert the slice to an asynchronous channel stream.
	// We set the buffer size equal to the slice length for optimal throughput.
	inputChan := FromSlice(ctx, inputSlice, len(inputSlice))

	// Pass the converted channel directly into your core Process logic!
	return Process(ctx, inputChan, opts, workerFunc)
}
