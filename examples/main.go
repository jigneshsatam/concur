package main

import (
	"context"
	"errors"
	"fmt"

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

	inputArray := []ProcessInput{
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
