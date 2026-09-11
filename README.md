# concur 🏎️

`concur` is a lightweight, type-safe, production-ready Go library that implements the **Fan-Out / Fan-In** concurrency pattern using Go Generics.

It distributes resource-heavy workloads across a controlled pool of parallel workers and multiplexes their results back into a single fanned-in stream, completely preventing unbounded goroutine leaks and memory spikes.

---

## ✨ Features

- **Strict Type Safety:** Built using Go Generics—no slow `interface{}` reflections or dynamic runtime type-casting.
- **Bounded Scaling:** Maintains a strict, maximum worker pool size to shield system resources.
- **Configurable Error Strategies:** Choose whether an internal item failure gracefully cancels the entire process (`StopOnError: true`) or silently logs and continues (`StopOnError: false`).
- **Context-Aware:** Native structural support for `context.Context` cancellation and timeouts.


## 💎 Why Use `concur`? (The Pros)

Implementing manual fan-out/fan-in pipelines requires writing complex boilerplates using channels, `sync.WaitGroup`, context tracking, and select blocks. `concur` abstracts this safely.

- **🛡️ Shielded Memory Allocation (Bounded Scaling):** Instead of spinning up an arbitrary number of goroutines that could trigger out-of-memory (OOM) crashes under high loads, it enforces a strict maximum worker pool boundary.
- **⚡ Native Type Safety (Zero Reflection):** Built fully using Go Generics. It enforces static compile-time type check safety without resorting to slow runtime `interface{}` type casting or reflection.
- **🛑 Advanced Error Interception:** You can toggle the `StopOnError` mode. If a single item fails, it immediately triggers an internal structural context cancellation to stop all other active workers, avoiding wasted compute.
- **🫧 Zero Goroutine Leaks:** The engine guarantees that all worker sub-routines cleanly exit and internal contexts are entirely torn down, even if down-stream channels stop reading data early.
- **🧩 Adaptable to "Any Function Signature":** By packing complex arguments into single custom input/output structs, you can process custom business functions with any number of parameters.

---

## 🛠️ Installation

```bash
go get github.com/jigneshsatam/concur
```

---

## 🏎️ Usage Example

Here is how to map **3 custom input parameters** into **3 custom output parameters** using a pool that automatically defaults to **4 parallel workers** while opting to continue on isolated item errors.

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
	Payload  []byte
}

// Wrap your multiple function return values
type ProcessOutput struct {
	BytesWritten int
	Duration     time.Duration
	IsVerified   bool
}

func main() {
	ctx := context.Background()

	// 1. Populate the work stream queue
	inputChan := make(chan ProcessInput, 3)
	inputChan <- ProcessInput{TaskID: 1, TargetIP: "192.168.1.5", Payload: []byte("ping")}
	inputChan <- ProcessInput{TaskID: 2, TargetIP: "10.0.0.1", Payload: []byte("malformed")} // Error item
	inputChan <- ProcessInput{TaskID: 3, TargetIP: "192.168.1.9", Payload: []byte("secure-auth")}
	close(inputChan)

	// 2. Define the execution worker function
	networkWorker := func(ctx context.Context, req ProcessInput) (ProcessOutput, error) {
		if req.TargetIP == "10.0.0.1" {
			return ProcessOutput{}, errors.New("network routing table rejection")
		}
		time.Sleep(50 * time.Millisecond) // Simulating network lag
		return ProcessOutput{BytesWritten: len(req.Payload), Duration: 50 * time.Millisecond, IsVerified: true}, nil
	}

	// 3. Configure behavior.
	// Leaving Workers at 0 automatically defaults to 4 workers.
	opts := concur.Options{
		Workers:     0,
		StopOnError: false, // Continue executing the queue if an item errors out
	}

	// 4. Fire up the fanned-out processing pool
	resultsStream := concur.Process(ctx, inputChan, opts, networkWorker)

	// 5. Consume fanned-in aggregated results natively
	for res := range resultsStream {
		if res.Err != nil {
			fmt.Printf("[❌ Error Caught]: %v. Moving to next queue item...\n", res.Err)
			continue
		}

		out := res.Value
		fmt.Printf("[✅ Success] Task Written: %d bytes | Latency: %v | Validated: %t\n",
			out.BytesWritten, out.Duration, out.IsVerified)
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
