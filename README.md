# concur 🏎️

`concur` is a lightweight, type-safe, production-ready Go library that implements the **Fan-Out / Fan-In** concurrency pattern using Go Generics. 

It allows you to distribute workloads across a controlled pool of parallel workers and multiplex their results back into a single stream, preventing unbounded goroutine leaks and memory spikes.

## ✨ Features

- **Strict Type Safety:** Built using Go Generics—no slow `interface{}` reflections or dynamic runtime type-casting.
- **Bounded Scaling:** Maintains a strict, maximum worker pool size to shield system resources.
- **Configurable Error Strategies:** Choose whether an internal item failure gracefully cancels the entire process (`StopOnError: true`) or silently logs and continues (`StopOnError: false`).
- **Context-Aware:** Native structural support for `context.Context` cancellation and timeouts.
