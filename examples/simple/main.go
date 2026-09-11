package main

import (
	"context"
	"fmt"

	"github.com/jigneshsatam/concur"
)

func main() {
	// 1. Create a channel and fill it with simple numbers
	inputChan := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		inputChan <- i
	}
	close(inputChan) // Always close the channel to let workers know no more data is coming

	// 2. Define a simple worker function
	squareWorker := func(ctx context.Context, num int) (int, error) {
		return num * num, nil
	}

	// 3. Configure options (leaving Workers at 0 triggers the default of 4 workers)
	opts := concur.Options{
		Workers:     0,
		StopOnError: false,
	}

	// 4. Start the concurrent process pipeline
	resultsStream := concur.Process(context.Background(), inputChan, opts, squareWorker)

	// 5. Read the fanned-in aggregated results
	for res := range resultsStream {
		if res.Err != nil {
			fmt.Printf("Error processing item: %v\n", res.Err)
			continue
		}
		fmt.Printf("Result: %d\n", res.Value)
	}
}
