# 01. Basic Parallel Processing 🟢

This example demonstrates parallelizing workloads using the `concur` package via two methods: traditional input channels and the new `ProcessSlice` utility.

## 🎯 Objective
Distribute sequential integers across **3 parallel pool workers** to concurrently calculate their squares and aggregate the results into a single stream.

## 🔀 Two Ways to Process Data

### Approach A: The Traditional Way (`concur.Process`)
Manually manages a standard Go input channel, ideal for dynamic streaming inputs from external sources like message queues.

### Approach B: The Modern Way (`concur.ProcessSlice`)
Passes a static slice directly into the pipeline while `concur` automates channel management and teardown, eliminating boilerplate.

## 🛠️ Design Patterns Covered
- **Decoupled Architecture:** Business operations remain independent functions for easy unit testing.
- **Closure Adaptation:** Wraps standard functions into the pipeline signature using anonymous closures.
- **Automated Lifecycle Draining:** Safely reads results from the output stream until internal channels close.

## 🏃 Execution
Run the example via:
```bash
go run ./examples/01_basic/main.go
```
