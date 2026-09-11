package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jigneshsatam/concur"
)

// 1. Pack the 3 input parameters into a structured object
type RequestData struct {
	TaskID   int
	TargetIP string
	Payload  []byte
}

// 2. Pack the 3 output results into a structured object
type ResponseData struct {
	BytesWritten int
	Duration     time.Duration
	IsVerified   bool
}

func main() {
	ctx := context.Background()

	// 3. Populate our input channel queue
	inputChan := make(chan RequestData, 4)
	inputChan <- RequestData{TaskID: 1, TargetIP: "192.168.1.5", Payload: []byte("ping")}
	inputChan <- RequestData{TaskID: 2, TargetIP: "10.0.0.1", Payload: []byte("malformed")} // Will simulate an error
	inputChan <- RequestData{TaskID: 3, TargetIP: "192.168.1.9", Payload: []byte("secure-auth")}
	inputChan <- RequestData{TaskID: 4, TargetIP: "192.168.1.12", Payload: []byte("health-check")}
	close(inputChan) // Close channel to signal to the workers there is no more incoming data

	// 4. Implement our custom WorkerFunc handling the structural wraps
	networkWorker := func(ctx context.Context, req RequestData) (ResponseData, error) {
		if req.TargetIP == "10.0.0.1" {
			return ResponseData{}, errors.New("network routing table rejection")
		}

		// Simulate network processing delay
		time.Sleep(50 * time.Millisecond)

		return ResponseData{
			BytesWritten: len(req.Payload),
			Duration:     50 * time.Millisecond,
			IsVerified:   true,
		}, nil
	}

	// 5. Instantiating options. Leaving Workers at 0 means the library
	// automatically applies the internal default of 4 workers.
	opts := concur.Options{
		Workers:     0,     // Automatically defaults to 4 inside the library
		StopOnError: false, // Continue streaming other results if one fails
	}

	startTime := time.Now()

	// 6. Execute the process pipeline with the configuration options
	resultsStream := concur.Process(ctx, inputChan, opts, networkWorker)

	// 7. Consume fanned-in aggregated results
	for res := range resultsStream {
		if res.Err != nil {
			fmt.Printf("[❌ Error]: %v (Skipping and moving to next task...)\n", res.Err)
			continue
		}

		// Unpack our 3 outputs safely
		out := res.Value
		fmt.Printf("[✅ Success] Bytes Scaled: %d | Latency: %v | Verified: %t\n",
			out.BytesWritten, out.Duration, out.IsVerified)
	}

	fmt.Printf("\nPipeline fully finished in: %v\n", time.Since(startTime))
}
