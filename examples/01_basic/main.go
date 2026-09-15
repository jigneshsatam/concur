package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jigneshsatam/concur"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	opts := concur.Options{Workers: 3, StopOnError: true}
	numbers := []int{1, 2, 3, 4, 5}

	// ---------------------------------------------------------
	// APPROACH A: The Traditional Way (Channels)
	// ---------------------------------------------------------
	fmt.Println("--- Approach A: Standard Input Channels ---")

	inChan := make(chan int, len(numbers))
	for _, n := range numbers {
		inChan <- n
	}
	close(inChan)

	resChanA := concur.Process(ctx, inChan, opts, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})

	for res := range resChanA {
		if res.Err == nil {
			fmt.Printf("Channel Result: %d\n", res.Value)
		}
	}

	// ---------------------------------------------------------
	// APPROACH B: The Modern Way (New ProcessSlice Feature)
	// ---------------------------------------------------------
	fmt.Println("\n--- Approach B: Zero-Boilerplate ProcessSlice ---")

	// No channel declaration, no manual seeding, no manual closing!
	resChanB := concur.ProcessSlice(ctx, numbers, opts, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})

	for res := range resChanB {
		if res.Err == nil {
			fmt.Printf("Slice Result: %d\n", res.Value)
		}
	}
}
