# 01. Basic Parallel Processing 🟢

This example demonstrates the simplest, most intuitive way to get up and running with the `concur` package. 

## 🎯 Objective
Take a streaming queue of sequential integers, distribute them across **4 parallel pool workers** to calculate their squares concurrently, and cleanly aggregate the results into a single fanned-in channel stream.

## 🛠️ Design Patterns Covered
- **Decoupled Architecture:** The core business operation (`SquareNumber`) is written as a completely independent, pure function. This ensures it remains trivial to unit-test without needing to wrap it in mock channels or pipeline infrastructure.
- **Closure Adaptation:** Shows how to wrap an item-only function (`func(int) int`) into the comprehensive pipeline signature seamlessly at the call-site using standard Go anonymous closures.
- **Native Draining:** Demonstrates reading values cleanly from the fanned-in output stream until the internal channels automate closure safely.

## 🏃 Execution
Run this example from the root directory of the repository:
```bash
go run ./examples/01_basic/main.go
```
