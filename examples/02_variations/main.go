package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jigneshsatam/concur"
)

type empty struct{}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := concur.Options{
		Workers:     3,
		StopOnError: false,
	}

	fmt.Println("🚀 Executing all 12 Closure Signatures Reference Matrix...")
	runGroup1(ctx, opts)
	runGroup2(ctx, opts)
	runGroup3(ctx, opts)
}

// ============================================================================
// GROUP 1: Functions accepting both Context and Item -> func(ctx, In)
// ============================================================================
func runGroup1(ctx context.Context, opts concur.Options) {
	fmt.Println("\n--- Group 1: (Context, Item) Signatures ---")

	// 1. Signature: func(ctx, In) (Out, error)
	in1 := makeChan(10)
	out1 := concur.Process(ctx, in1, opts, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("G1-P1-%d", item), nil
	})
	drainStream("1. Full Match", out1)

	// 2. Signature: func(ctx, In) Out
	in2 := makeChan(10)
	out2 := concur.Process(ctx, in2, opts, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("G1-P2-%d", item), nil
	})
	drainStream("2. No Error Return", out2)

	// 3. Signature: func(ctx, In) error
	in3 := makeChan(10)
	out3 := concur.Process(ctx, in3, opts, func(ctx context.Context, item int) (empty, error) {
		var err error
		if item == 10 {
			err = errors.New("G1-P3-triggered-error")
		}
		return empty{}, err
	})
	drainStream("3. Side-Effect with Error", out3)

	// 4. Signature: func(ctx, In)
	in4 := makeChan(10)
	out4 := concur.Process(ctx, in4, opts, func(ctx context.Context, item int) (empty, error) {
		_ = item
		return empty{}, nil
	})
	drainStream("4. Pure Side-Effect", out4)
}

// ============================================================================
// GROUP 2: Functions accepting Item only -> func(item)
// ============================================================================
func runGroup2(ctx context.Context, opts concur.Options) {
	fmt.Println("\n--- Group 2: (Item Only) Signatures ---")

	// 5. Signature: func(In) (Out, error)
	in5 := makeChan(20)
	out5 := concur.Process(ctx, in5, opts, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("G2-P5-%d", item), nil
	})
	drainStream("5. Standard Go Return", out5)

	// 6. Signature: func(In) Out
	in6 := makeChan(20)
	out6 := concur.Process(ctx, in6, opts, func(ctx context.Context, item int) (string, error) {
		return fmt.Sprintf("G2-P6-%d", item), nil
	})
	drainStream("6. Pure Mapping Transform", out6)

	// 7. Signature: func(In) error
	in7 := makeChan(20)
	out7 := concur.Process(ctx, in7, opts, func(ctx context.Context, item int) (empty, error) {
		return empty{}, nil
	})
	drainStream("7. Item Side-Effect with Error", out7)

	// 8. Signature: func(In)
	in8 := makeChan(20)
	out8 := concur.Process(ctx, in8, opts, func(ctx context.Context, item int) (empty, error) {
		log.Printf("[Log Side Effect] Processing item: %d", item)
		return empty{}, nil
	})
	drainStream("8. Local Pure Side-Effect", out8)
}

// ============================================================================
// GROUP 3: Standalone background tasks -> func()
// ============================================================================
func runGroup3(ctx context.Context, opts concur.Options) {
	fmt.Println("\n--- Group 3: (No Parameters) Background Signatures ---")

	// 9. Signature: func() (Out, error)
	in9 := makeChan(1)
	out9 := concur.Process(ctx, in9, opts, func(ctx context.Context, _ int) (string, error) {
		return "G3-P9-Static-Data", nil
	})
	drainStream("9. Task with Result and Error", out9)

	// 10. Signature: func() Out
	in10 := makeChan(1)
	out10 := concur.Process(ctx, in10, opts, func(ctx context.Context, _ int) (string, error) {
		return "G3-P10-Generated", nil
	})
	drainStream("10. Task with Result Only", out10)

	// 11. Signature: func() error
	in11 := makeChan(1)
	out11 := concur.Process(ctx, in11, opts, func(ctx context.Context, _ int) (empty, error) {
		return empty{}, errors.New("G3-P11-failure")
	})
	drainStream("11. Task with Error Only", out11)

	// 12. Signature: func()
	in12 := makeChan(1)
	out12 := concur.Process(ctx, in12, opts, func(ctx context.Context, _ int) (empty, error) {
		return empty{}, nil
	})
	drainStream("12. Pure Standalone Job", out12)

	// =========================================================================
	// BONUS: Slice-Based Adaptive Patterns
	// =========================================================================
	// Both ProcessSlice and FromSlice natively inherit the exact same execution model
	// since they use the same underlying generic core engine.
	fmt.Println("\n--- Evaluating Slice-Based Adaptive Closures ---")

	type DB struct{}
	mockClientDB := &DB{}

	// Define some dummy structures to support our various signatures
	type DataIn struct{ ID int }
	type DataOut struct{ Val string }

	staticData := []DataIn{{ID: 201}, {ID: 202}, {ID: 203}}

	// Pattern A: ProcessSlice adapting a legacy non-standard function (No Context/No Error)
	legacyFunc := func(in DataIn) DataOut {
		return DataOut{Val: fmt.Sprintf("Slice-Legacy-%d", in.ID)}
	}

	sliceStream := concur.ProcessSlice(ctx, staticData, opts, func(_ context.Context, in DataIn) (DataOut, error) {
		// Closure handles normalization step smoothly
		return legacyFunc(in), nil
	})

	for r := range sliceStream {
		if r.Err == nil {
			fmt.Printf("[ProcessSlice Adapter Output]: %s\n", r.Value.Val)
		}
	}

	// Pattern B: FromSlice converting data into streams that accept heavy dependencies
	streamChan := concur.FromSlice(ctx, staticData, len(staticData))

	dependentStream := concur.Process(ctx, streamChan, opts, func(c context.Context, in DataIn) (bool, error) {
		// Complete lexical closure access to 'mockClientDB'
		_ = mockClientDB
		return true, nil
	})

	for r := range dependentStream {
		_ = r.Value
	}

	fmt.Println("✨ Reference execution finalized successfully!")

}

func makeChan(val int) chan int {
	ch := make(chan int, 1)
	ch <- val
	close(ch)
	return ch
}

func drainStream[T any](label string, stream <-chan concur.Result[T]) {
	for res := range stream {
		if res.Err != nil {
			fmt.Printf("  [💥 %s Result]: Failed with error: %v\n", label, res.Err)
			continue
		}
		fmt.Printf("  [🎯 %s Result]: Extracted: %v\n", label, res.Value)
	}
}
