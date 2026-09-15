package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jigneshsatam/concur"
)

type Order struct {
	ID     string
	Amount float64
}

type SyncReport struct {
	OrderID   string
	Success   bool
	Timestamp time.Time
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Simulating rows pulled statically from a database
	dbOrders := []Order{
		{ID: "ORD-991", Amount: 45.50},
		{ID: "ORD-992", Amount: 120.00},
		{ID: "ORD-993", Amount: 12.99},
	}

	opts := concur.Options{
		Workers:     2,
		StopOnError: false, // Keep syncing remaining orders even if one drops out
	}

	fmt.Println("🔥 Starting Multi-Stage Advanced Pipeline 🔥")

	// STAGE 1: Convert your static database slice into a reactive, context-aware stream channel
	orderStream := concur.FromSlice(ctx, dbOrders, len(dbOrders))

	// STAGE 2: Distribute the stream across concurrent network integration workers
	reportPipeline := concur.Process(ctx, orderStream, opts, func(ctx context.Context, ord Order) (SyncReport, error) {
		// Simulate hitting an external billing API endpoint (e.g., Stripe/PayPal)
		time.Sleep(100 * time.Millisecond)

		if ord.Amount > 100 {
			return SyncReport{OrderID: ord.ID, Success: false}, fmt.Errorf("order %s flagged: requires high-value manager review", ord.ID)
		}

		return SyncReport{OrderID: ord.ID, Success: true, Timestamp: time.Now()}, nil
	})

	// STAGE 3: Final Fan-In Collection Consumer Loop
	var compiledTotal float64
	for report := range reportPipeline {
		if report.Err != nil {
			fmt.Printf("[⚠️ Sync Failure]: %v\n", report.Err)
			continue
		}

		fmt.Printf("[🚀 Sync Success]: Order %s pushed to tracking engine at %s\n",
			report.Value.OrderID, report.Value.Timestamp.Format("15:04:05"))
		compiledTotal++
	}

	fmt.Printf("\nPipeline Completed. Successfully synchronized %g orders.\n", compiledTotal)
}
