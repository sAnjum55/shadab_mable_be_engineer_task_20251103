package pipeline

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type StageMetrics struct {
	StageName   string        `json:"stage_name"`   // Name of the stage (Map, Filter, Collect, etc.)
	BatchID     int           `json:"batch_id"`     // Which batch this metric belongs to
	InputCount  int           `json:"input_count"`  // How many items entered this stage
	OutputCount int           `json:"output_count"` // How many items came out of this stage
	Duration    time.Duration `json:"duration"`     // How long the stage took
	WorkerID    int           `json:"worker_id"`    // Which worker processed this batch
	Timestamp   time.Time     `json:"timestamp"`    // When this stage executed
}

func (p *Pipeline[T]) emitMetrics(metricsChan <-chan StageMetrics) {
	fmt.Println("⚙️  Starting emitMetrics... waiting for metrics to arrive.")

	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"}, // native TCP port (not HTTP)
		Auth: clickhouse.Auth{
			Database: "mable",
			Username: "default",
			Password: "mypass",
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		log.Fatalf("❌ Failed to connect to ClickHouse: %v", err)
	}
	defer func() {
		fmt.Println("📴 Closing ClickHouse connection.")
		_ = conn.Close()
	}()

	batch, err := conn.PrepareBatch(ctx, `
        INSERT INTO mable.pipeline_metrics 
        (stage_name, batch_id, input_count, output_count, duration_ms, worker_id, timestamp)
        VALUES
    `)
	if err != nil {
		log.Fatalf("❌ Failed to prepare batch insert: %v", err)
	}

	count := 0
	for m := range metricsChan {
		count++
		fmt.Printf("📊 Metric #%d: Stage=%s | Batch=%d | In=%d | Out=%d | Duration=%v | Worker=%d\n",
			count, m.StageName, m.BatchID, m.InputCount, m.OutputCount, m.Duration, m.WorkerID)

		if err := batch.Append(
			m.StageName,
			uint32(m.BatchID),
			uint32(m.InputCount),
			uint32(m.OutputCount),
			float64(m.Duration.Milliseconds()),
			uint32(m.WorkerID),
			m.Timestamp,
		); err != nil {
			log.Printf("⚠️ Failed to append metric: %v", err)
		}
	}

	fmt.Printf("📦 Total metrics collected: %d\n", count)

	if count == 0 {
		fmt.Println("⚠️  No metrics received. metricsChan may be empty or closed early.")
		return
	}

	if err := batch.Send(); err != nil {
		log.Printf("❌ Failed to send metrics batch: %v", err)
	} else {
		fmt.Println("✅ Metrics successfully sent to ClickHouse!")
	}
}
