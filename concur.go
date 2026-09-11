package concur

import (
	"context"
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

	// If true, the first error halts the entire pipeline instantly.
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
	out := make(chan Result[Out])
	var wg sync.WaitGroup

	// Idiomatic Go Defaults: If the user provides 0 or negative numbers,
	// fallback safely to a baseline default pool size of 4 workers.
	if opts.Workers <= 0 {
		opts.Workers = 4
	}

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

					// Execute business logic safely
					val, err := workerFn(pipelineCtx, item)
					res := Result[Out]{Value: val, Err: err}

					select {
					case <-pipelineCtx.Done():
						return
					case out <- res:
						// If an error happened and StopOnError is true, halt the world
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
