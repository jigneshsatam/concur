/*
Package concur is a lightweight, type-safe, production-ready Go library that
implements the Fan-Out / Fan-In concurrency pattern using Go Generics.

It distributes resource-heavy workloads across a controlled pool of parallel
workers and multiplexes their results back into a single fanned-in stream,
completely preventing unbounded goroutine leaks and memory spikes.

# Core Concepts

The pipeline relies on a few fundamental paradigms to guarantee safe executions:

  - Bounded Scaling: Enforces a strict, maximum worker pool size to protect system memory.
  - Short-Circuit Error Propagation: Halts the entire pipeline instantly upon encountering
    the first item failure if configured via options.
  - Automated Panic Recovery: Built-in catch routines shield your infrastructure by translating
    unexpected worker runtime panics into standardized, manageable Go errors.

# Usage with Closure Adaptation

Because the primary Process function utilizes a highly comprehensive function signature,
users can leverage standard Go anonymous closures at the call-site to adapt any custom
worker function format (e.g., functions without a context parameter, functions that don't
return errors, or pure background tasks) completely bypassing messy bracketed type syntax.

A simple example using inline closure adaptation:

	inputChan := make(chan int, 3)
	// ... populate and close channel ...

	opts := concur.Options{Workers: 4, StopOnError: false}
	results := concur.Process(ctx, inputChan, opts, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("Processed: %d", item), nil
	})

	for res := range results {
		if res.Err != nil {
			log.Printf("Worker task failed: %v", res.Err)
			continue
		}
		fmt.Println(res.Value)
	}
*/
package concur
