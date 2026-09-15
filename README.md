# concur 🏎️

[![Go Reference](https://pkg.go.dev/badge/github.com/jigneshsatam/concur.svg)](https://pkg.go.dev/github.com/jigneshsatam/concur)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/jigneshsatam/concur)
[![Build Pipeline Status 🏎️](https://github.com/jigneshsatam/concur/actions/workflows/go.yml/badge.svg)](https://github.com/jigneshsatam/concur/actions/workflows/go.yml)
[![Linter Status](https://github.com/jigneshsatam/concur/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/jigneshsatam/concur/actions)
[![GitHub License](https://img.shields.io/github/license/jigneshsatam/concur)](https://github.com/jigneshsatam/concur/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/jigneshsatam/concur)](https://github.com/jigneshsatam/concur/releases)

`concur` is a lightweight, type-safe, production-ready Go library that implements the **Fan-Out / Fan-In** concurrency pattern using Go Generics.

It distributes resource-heavy workloads across a controlled pool of parallel workers and multiplexes their results back into a single fanned-in stream, completely preventing unbounded goroutine leaks, data races, and memory spikes.

---

## ✨ Features

- **Strict Type Safety:** Built using Go Generics—no slow `interface{}` reflections or dynamic runtime type-casting.
- **Bounded Scaling:** Enforces a strict, maximum worker pool size to protect system resources.
- **Configurable Error Strategies:** Choose whether an error or panic halts the entire pipeline instantly (`StopOnError: true`) or lets processing continue (`StopOnError: false`).
- **Context-Aware:** Native support for `context.Context` cancellation and timeouts.
- **Automated Panic Recovery:** Built-in catch routines capture user function panics, seamlessly translating them into manageable Go errors.
- **Zero Signature Friction:** Accepts a single robust API format. Users can wrap **any function signature** using standard Go closures without writing tedious wrapper boilerplate.

---

## 💎 Why Use `concur`? (The Pros)

Implementing manual fan-out/fan-in pipelines requires writing complex boilerplates using channels, `sync.WaitGroup`, context tracking, and select blocks. `concur` abstracts this safely.

- **🛡️ Shielded Memory Allocation (Bounded Scaling):** Instead of spinning up an arbitrary number of goroutines that could trigger out-of-memory (OOM) crashes under high loads, it enforces a strict maximum worker pool boundary.
- **⚡ Native Type Safety (Zero Reflection):** Built fully using Go Generics. It enforces static compile-time type check safety without resorting to slow runtime `interface{}` type casting or reflection.
- **🛑 Advanced Error Interception:** You can toggle the `StopOnError` mode. If a single item fails, it immediately triggers an internal structural context cancellation to stop all other active workers, avoiding wasted compute.
- **🫧 Zero Goroutine Leaks:** The engine guarantees that all worker sub-routines cleanly exit and internal contexts are entirely torn down, even if down-stream channels stop reading data early.
- **🧩 Adaptable to "Any Function Signature":** By packing complex arguments into single custom input/output structs or leveraging direct function closures, you can map any business function signature cleanly into the pipeline.

---

## 🛠️ Installation

```bash
go get github.com/jigneshsatam/concur
```

---

## 🏎️ Core Usage Example

Here is how to map a slice of custom structural inputs across a pool of parallel workers while using inline closure adaptation and continuing past isolated items errors.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jigneshsatam/concur"
)

// Wrap your multiple function input arguments
type ProcessInput struct {
	TaskID   int
	TargetIP string
}

// Wrap your multiple function return values
type ProcessOutput struct {
	BytesWritten int
	IsVerified   bool
}

func main() {
	ctx := context.Background()

	inputArray := []ProcessInput {
		ProcessInput{TaskID: 1, TargetIP: "192.168.1.5"},
		ProcessInput{TaskID: 2, TargetIP: "10.0.0.1"}, // Malformed error item
		ProcessInput{TaskID: 3, TargetIP: "192.168.1.9"},
	}

	// 1. Configure behavior (0 workers automatically defaults to 4)
	opts := concur.Options{
		Workers:     4,
		StopOnError: false, // Continue executing the queue if an item errors out
	}

	// 2. Fire up the fanned-out pool using a Closure Adapter
	resultsStream := concur.ProcessSlice(ctx, inputArray, opts, func(ctx context.Context, item ProcessInput) (ProcessOutput, error) {
		if item.TargetIP == "10.0.0.1" {
			return ProcessOutput{}, errors.New("network routing rejection")
		}
		return ProcessOutput{BytesWritten: 128, IsVerified: true}, nil
	})

	// 4. Consume fanned-in aggregated results natively
	for res := range resultsStream {
		if res.Err != nil {
			fmt.Printf("[❌ Error/Panic Caught]: %v\n", res.Err)
			continue
		}
		fmt.Printf("[✅ Success] Bytes: %d | Validated: %t\n", res.Value.BytesWritten, res.Value.IsVerified)
	}
}
```

## 📦 Slice Utilities (No Channel Boilerplate)

Instead of manually instantiating input channels, pushing elements, and handling channel closures, `concur` provides native generic utilities to process raw Go slices instantly.

### ⚡ Direct Processing: `ProcessSlice`
Pass a static array or slice straight into the pipeline. `concur` handles the underlying channel lifecycle entirely under the hood.

```go
inputs := []string{"apple", "banana", "cherry"}

// Processes slice values immediately using concurrent worker pools
results := concur.ProcessSlice(ctx, inputs, opts, func(ctx context.Context, item string) (int, error) {
    return len(item), nil
})
```

### 🌊 Stream Conversion: `FromSlice`
Convert an existing static slice into an isolated, context-aware read-only channel asynchronously. This is excellent when you need a stream that plays nicely with context cancellations.

```go
items := []int{10, 20, 30}

// Returns a <-chan int that closes when the slice drains or if ctx cancels
inputChan := concur.FromSlice(ctx, items, len(items))

// Feed it to your core process architecture
results := concur.Process(ctx, inputChan, opts, workerFunc)
```


---

## 📂 Repository Structure & Examples

The repository is structured as a step-by-step learning path, ranging from basic introduction programs to advanced multi-stage data stream systems:

examples/  
|── 01_basic/  
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── main.go           # Pure onboarding (Squaring numbers in parallel)  
|── 02_variations/  
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── main.go           # Master reference layout for all 12 combinations  
|── 03_advanced_pipeline/  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── main.go           # Advanced multi-stage streaming pipeline 🔥  

You can execute any of these scenarios locally from the root of your workspace:

# Run the basic introduction program
```go
go run ./examples/01_basic/main.go
```

# Run the 12-signature syntax cheat-sheet reference block
```go
go run ./examples/02_variations/main.go
```

# Run the complex multi-stage streaming pipeline
```go
go run ./examples/03_advanced_pipeline/main.go
```

---

## 🧩 Adapting All 12 Signature Variations

Because `concur` uses Go's type-inference engine, **you never need to write messy bracketed types like `[int, string]`**. You can adapt all 12 combinations of inputs and outputs directly at the call-site using standard Go anonymous closures:

### Group 1: Signatures with Context & Item `(ctx, item)`

#### 1. Full Match: `func(ctx, In) (Out, error)`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (string, error) {
	return myFunc(ctx, item)
})
```

#### 2. No Error: `func(ctx, In) Out`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (string, error) {
	return myFunc(ctx, item), nil
})
```

#### 3. Side-Effect Only with Error: `func(ctx, In) error`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (struct{}, error) {
	return struct{}{}, myFunc(ctx, item)
})
```

#### 4. Pure Side-Effect: `func(ctx, In)`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (struct{}, error) {
	myFunc(ctx, item)
	return struct{}{}, nil
})
```

---

### Group 2: Signatures with Item Only `(item)`

#### 5. Standard Go Return: `func(In) (Out, error)`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (string, error) {
	return myFunc(item)
})
```

#### 6. Pure Transformation: `func(In) Out`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (string, error) {
	return myFunc(item), nil
})
```

#### 7. Side-Effect with Error: `func(In) error`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (struct{}, error) {
	return struct{}{}, myFunc(item)
})
```

#### 8. Local Side-Effect: `func(In)`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (struct{}, error) {
	myFunc(item)
	return struct{}{}, nil
})
```

---

### Group 3: Standalone Background Tasks `()`
*(Note: When using these, the channel stream acts strictly as a worker queue counter to trigger background operations).*

#### 9. Task with Result and Error: `func() (Out, error)`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, _ int) (string, error) {
	return myFunc()
})
```

#### 10. Task with Result Only: `func() Out`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, _ int) (string, error) {
	return myFunc(), nil
})
```

#### 11. Task with Error Only: `func() error`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, _ int) (struct{}, error) {
	return struct{}{}, myFunc()
})
```

#### 12. Pure Standalone Job: `func()`
```go
out := concur.Process(ctx, in, opts, func(ctx context.Context, _ int) (struct{}{}, error) {
	myFunc()
	return struct{}{}, nil
})
```

---

## ⚡ Panic Safety Guardrails

Unexpected runtime panics inside your closure workloads will **not** bring down your entire Go application server infrastructure.

`concur` wraps executions in an inner recovery loop. If a closure panics, the worker captures the payload, normalizes it into a standard Go error structure format (`worker panicked: <reason>`), and pushes it down the fanned-in result stream:

```go
opts := concur.Options{StopOnError: false} // Keep other workers alive if one panics

out := concur.Process(ctx, in, opts, func(ctx context.Context, item int) (int, error) {
	if item == 42 {
		panic("malformed byte payload crash!")
	}
	return item * 2, nil
})

for res := range out {
	if res.Err != nil {
		// Output: "worker panicked: malformed byte payload crash!"
		fmt.Println("Handled cleanly:", res.Err)
	}
}
```

---

## 🧪 Testing

To run unit tests along with Go's automated **Data Race Detector**:

```bash
go test -v -race ./...
```

## 📊 Benchmarking

To benchmark the parallel processing gains of `concur` against a traditional sequential execution loop, run:

```bash
go test -bench=. -benchmem
```
