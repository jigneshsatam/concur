# 03. Advanced Multi-Stage Streaming Pipeline 🔥

This directory showcases a complex, industrial-grade data engineering pipeline. It addresses real-world streaming challenges where data must flow sequentially through multiple transformation filters at different concurrency scales.

## 🎯 Objective
Ingest raw paragraphs of unstructured text rows, break them down into tokens in Stage 1, filter out execution drops dynamically, calculate unique word counts in Stage 2, and emit unified analytical calculations without bottlenecks or deadlocks.

## 🛠️ Design Patterns Covered
- **Chained Concurrency Processors:** Connects two entirely separate `concur.Process` instances. Stage 1's fanned-in output serves as the streaming source data for Stage 2.
- **Asymmetric Rate Control:** 
  - **Stage 1 (Tokenization):** Scales aggressively using **4 parallel workers** because parsing string arrays is lightweight.
  - **Stage 2 (Deduplication Analytics):** Intentionally slows scaling down to **2 parallel workers** to simulate protecting a downstream rate-limited microservice or database connection pool.
- **The Decoupled Stream Bridge:** Utilizes an asynchronous background thread loop acting as a buffer gate between stages. It inspects incoming tokens, logs/drops isolated runtime anomalies, and smoothly forwards clean values forward.
- **Leak-Proof Context Engineering:** Every stage listens explicitly to `<-ctx.Done()`. If a system cancellation occurs or the final downstream consumer drops out early, all background sub-routines stop execution instantly and close channels properly to prevent permanent goroutine memory leaks.

## 🏃 Execution
Run this example from the root directory of the repository:
```bash
go run ./examples/03_advanced_pipeline/main.go
```
