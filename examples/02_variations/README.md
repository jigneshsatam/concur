# 02. Architectural Closure Variations 🔵

This directory serves as a comprehensive **API Cheat Sheet** and syntax reference matrix for developers integrating pre-existing or third-party business logic into `concur`.

## 🎯 Objective
Demonstrate how Go's native type-inference engine handles **all 12 mathematical combinations** of function parameters and return values without requiring messy bracketed types like `concur.Process[int, string](...)`.

## 🛠️ Design Patterns Covered
Using standard inline Go closures at the call-site, this master reference file steps through:
- **Group 1: Full Signatures `(ctx, item)`**
  - Full match handling (`func(ctx, In) (Out, error)`)
  - Suppressing error channels when missing (`func(ctx, In) Out`)
  - Running context-aware side-effects (`func(ctx, In) error` and `func(ctx, In)`)
- **Group 2: Input Only Signatures `(item)`**
  - Standard Go function adaptations ignoring pipeline contexts (`func(In) (Out, error)`)
  - Pure value mapping/transformation blocks (`func(In) Out`)
  - Local tracking or logging side-effects (`func(In) error` and `func(In)`)
- **Group 3: Standalone Background Tasks `()`**
  - Triggering completely independent cron-like operations or background network telemetry generation loops where the input channel acts strictly as an execution counter token (`func() (Out, error)`, `func() Out`, `func() error`, and `func()`).

## 🏃 Execution
Run this example from the root directory of the repository:
```bash
go run ./examples/02_variations/main.go
```
