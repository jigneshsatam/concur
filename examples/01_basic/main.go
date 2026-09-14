package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jigneshsatam/concur"
)

// SquareNumber is a simple, decoupled pure function
func SquareNumber(n int) int {
	return n * n
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Create a simple data stream
	numChan := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		numChan <- i
	}
	close(numChan)

	// 2. Configure 4 parallel workers
	opts := concur.Options{
		Workers:     4,
		StopOnError: false,
	}

	fmt.Println("🌟 [Basic Example] Squaring Numbers in Parallel...")

	// 3. Fire up the fanned-out processing pipeline
	squareStream := concur.Process(ctx, numChan, opts, func(ctx context.Context, item int) (int, error) {
		result := SquareNumber(item) // Separated pure business function
		return result, nil           // Wrapped into the pipeline closure layout
	})

	// 4. Drain fanned-in results natively
	for res := range squareStream {
		fmt.Printf("   👉 Input Item squared: %d\n", res.Value)
	}

	fmt.Println("🏁 Basic concurrent execution complete!")
}
