package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jigneshsatam/concur"
)

// Stage 1 structures: Raw text ingestion
type RawText struct {
	ID        int
	Paragraph string
}

type TokenizedText struct {
	ID    int
	Words []string
}

// Stage 2 structures: Analytics generation
type WordCountResult struct {
	ID        int
	UniqueQty int
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Setup Data Feed
	rawTextSlice := []RawText{
		RawText{ID: 1, Paragraph: "Go concurrency pipelines are fast and type-safe."},
		RawText{ID: 2, Paragraph: "Fan-out distributes workloads cleanly across pools."},
		RawText{ID: 3, Paragraph: "Avoid memory leaks by closing your channels safely."},
	}

	// ============================================================================
	// STAGE 1: Parallel Ingestion & Tokenization (4 Workers)
	// ============================================================================
	stage1Opts := concur.Options{Workers: 4, StopOnError: true}

	stage1Stream := concur.ProcessSlice(ctx, rawTextSlice, stage1Opts, func(ctx context.Context, item RawText) (TokenizedText, error) {
		// Simulate computation/string parsing latency
		time.Sleep(10 * time.Millisecond)

		words := strings.Fields(strings.ToLower(item.Paragraph))
		return TokenizedText{ID: item.ID, Words: words}, nil
	})

	// ============================================================================
	// STREAM BRIDGE: Decouples Stage 1 and Stage 2 safely
	// ============================================================================
	stage2InputChan := make(chan TokenizedText, 4)

	go func() {
		defer close(stage2InputChan)
		for res := range stage1Stream {
			if res.Err != nil {
				fmt.Printf("[Pipeline Alert] Skipping item due to Stage 1 Error: %v\n", res.Err)
				continue
			}

			// Forward clean output value straight into Stage 2's feed
			select {
			case <-ctx.Done():
				return
			case stage2InputChan <- res.Value:
			}
		}
	}()

	// ============================================================================
	// STAGE 2: Parallel Analytics / Heavy Calculations (2 Workers)
	// ============================================================================
	stage2Opts := concur.Options{Workers: 2, StopOnError: false}

	finalOutputStream := concur.Process(ctx, stage2InputChan, stage2Opts, func(ctx context.Context, item TokenizedText) (WordCountResult, error) {
		// Simulate database lookup / analytics tracking overhead
		time.Sleep(20 * time.Millisecond)

		// Deduplicate and count unique words
		uniqueMap := make(map[string]bool)
		for _, w := range item.Words {
			// Strip common punctuation marks
			w = strings.Trim(w, ".,!?;:")
			if w != "" {
				uniqueMap[w] = true
			}
		}

		return WordCountResult{ID: item.ID, UniqueQty: len(uniqueMap)}, nil
	})

	// ============================================================================
	// CONSUMER: Process final streaming calculations cleanly
	// ============================================================================
	fmt.Println("🚀 Streaming Multi-Stage Concurrency Pipeline started...")

	for finalResult := range finalOutputStream {
		if finalResult.Err != nil {
			fmt.Printf("[❌ Stage 2 Failure]: %v\n", finalResult.Err)
			continue
		}

		res := finalResult.Value
		fmt.Printf("[✅ Pipeline Complete] Job ID: %d | Found %d unique processed terms.\n", res.ID, res.UniqueQty)
	}

	fmt.Println("🏁 All streaming stages completed successfully without leaks!")
}
