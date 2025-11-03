// Package pipeline provides the core pipeline infrastructure and metrics
// instrumentation for measuring stage performance. This file defines the
// StageMetrics struct and the emitMetrics method, which handles asynchronous
// metric collection and persistence to ClickHouse.
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

//
// =======================
//  METRICS STRUCTURE
// =======================
//

// StageMetrics represents a standardized record of performance statistics for
// a single stage execution within the pipeline. Each metric corresponds to one
// batch being processed by one worker at one stage.
//
// These metrics are emitted asynchronously and stored in ClickHouse for
// monitoring and analysis in Grafana.
type StageMetrics struct {
	StageName   string        `json:"stage_name"`   // Name of the stage (e.g., Map, Filter, Reduce)
	BatchID     int           `json:"batch_id"`     // Batch identifier within the pipeline run
	InputCount  int           `json:"input_count"`  // Number of items entering this stage
	OutputCount int           `json:"output_count"` // Number of items produced by this stage
	Duration    time.Duration `json:"duration"`     // Processing duration for this stage and batch
	WorkerID    int           `json:"worker_id"`    // Worker goroutine that handled this batch
	Timestamp   time.Time     `json:"timestamp"`    // Time when this stage execution occurred
}

//
// =======================
//  EMIT METRICS FUNCTION
// =======================
//

// emitMetrics asynchronously listens on a channel of StageMetrics values and
// writes them in batches to a ClickHouse database.
//
// It connects to ClickHouse via TCP, ensures the table exists, prepares a
// batched INSERT statement, and appends each incoming metric to the batch.
func (p *Pipeline[T]) emitMetrics(metricsChan <-chan StageMetrics) {
	log.Println("Starting emitMetrics... waiting for metrics to arrive.")

	ctx := context.Background()

	// Establish a TCP connection to ClickHouse (✅ changed: use Docker service name)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"}, // ✅ use docker service name instead of localhost
		Auth: clickhouse.Auth{
			Database: "mable",
			Username: "default",
			Password: "mypass",
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		log.Printf("Failed to connect to ClickHouse: %v", err)
		return
	}
	defer func() {
		log.Println("Closing ClickHouse connection.")
		_ = conn.Close()
	}()

	// ✅ NEW BLOCK: Ensure table exists before inserting
	if err := conn.Exec(ctx, `CREATE DATABASE IF NOT EXISTS mable`); err != nil {
		log.Printf("Failed to ensure database exists: %v", err)
	}

	// Ensure the table exists
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pipeline_metrics (
			stage_name String,
			batch_id UInt32,
			input_count UInt32,
			output_count UInt32,
			duration_ms Float64,
			worker_id UInt32,
			timestamp DateTime
		) ENGINE = MergeTree()
		ORDER BY (stage_name, batch_id, timestamp)
	`); err != nil {
		log.Printf("Failed to ensure pipeline_metrics table exists: %v", err)
	} else {
		log.Println("✅ ClickHouse table ensured: mable.pipeline_metrics")
	}

	// Prepare an insert statement for the pipeline_metrics table
	batch, err := conn.PrepareBatch(ctx, `
        INSERT INTO mable.pipeline_metrics 
        (stage_name, batch_id, input_count, output_count, duration_ms, worker_id, timestamp)
        VALUES
    `)
	if err != nil {
		log.Printf("Failed to prepare batch insert: %v", err)
		return
	}

	count := 0

	// Process metrics as they arrive on the channel
	for m := range metricsChan {
		count++
		log.Printf("Metric #%d: Stage=%s | Batch=%d | In=%d | Out=%d | Duration=%v | Worker=%d\n",
			count, m.StageName, m.BatchID, m.InputCount, m.OutputCount, m.Duration, m.WorkerID)

		// Append metric record to ClickHouse batch
		if err := batch.Append(
			m.StageName,
			uint32(m.BatchID),
			uint32(m.InputCount),
			uint32(m.OutputCount),
			float64(m.Duration.Milliseconds()),
			uint32(m.WorkerID),
			m.Timestamp,
		); err != nil {
			log.Printf("Failed to append metric: %v", err)
		}
	}

	fmt.Printf("Total metrics collected: %d\n", count)

	// No metrics were sent
	if count == 0 {
		log.Println("No metrics received. metricsChan may be empty or closed early.")
		return
	}

	// Commit the batch to ClickHouse
	if err := batch.Send(); err != nil {
		log.Printf("Failed to send metrics batch: %v", err)
	} else {
		log.Println("✅ Metrics successfully sent to ClickHouse.")
	}
}
