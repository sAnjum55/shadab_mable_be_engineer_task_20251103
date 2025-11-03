package pipeline

import (
	"fmt"
	"time"
)

// RunAllBenchmarks executes all embedded pipeline benchmarks.
// This is reusable by main.go and test files.
func RunAllBenchmarks() {
	fmt.Println("🏗 Running embedded benchmarks...")

	runIntPipelineBenchmark()
	runStructPipelineBenchmark()
	runMableEventBenchmark()

	fmt.Println("✅ All benchmarks completed successfully.")
}

// Benchmark #1 — Integer Pipeline
func runIntPipelineBenchmark() {
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		start := time.Now()

		data := make([]int, size)
		for i := range data {
			data[i] = i
		}

		p := New[int]().
			WithBatchSize(100).
			WithWorkers(4).
			Map(func(x int) int { return x * 2 }).
			Filter(func(x int) bool { return x%3 == 0 }).
			Collect(func(batch []int) { _ = batch })

		p.Run(data)
		fmt.Printf("📊 BenchmarkPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}

// Benchmark #2 — Custom Struct Pipeline
func runStructPipelineBenchmark() {
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		start := time.Now()

		data := make([]TestStruct, size)
		for i := range data {
			data[i] = TestStruct{
				ID:        i,
				Name:      fmt.Sprintf("Item_%d", i),
				Value:     float64(i) * 1.23,
				Active:    i%2 == 0,
				Category:  "default",
				Timestamp: time.Now().Unix(),
				Count:     i % 100,
				Tag:       "bench",
				Ratio:     0.5,
				Flag:      i%3 == 0,
			}
		}

		p := New[TestStruct]().
			WithBatchSize(1000).
			WithWorkers(8).
			Map(func(e TestStruct) TestStruct {
				e.Value *= 2
				return e
			}).
			Filter(func(e TestStruct) bool {
				return e.Value > 1000
			}).
			Collect(func(batch []TestStruct) { _ = len(batch) })

		p.Run(data)
		fmt.Printf("📊 BenchmarkTestStructPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}

// Benchmark #3 — Mable Event Pipeline
func runMableEventBenchmark() {
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		start := time.Now()

		data := make([]MableEvent, size)
		for i := range data {
			data[i] = MableEvent{
				EID: fmt.Sprintf("event_%d", i),
				EN:  "Order Completed",
				TS:  time.Now().Unix(),
			}
		}

		p := New[MableEvent]().
			WithBatchSize(1000).
			WithWorkers(8).
			Map(func(e MableEvent) MableEvent {
				e.CreatedAt = time.Now()
				e.ESD.Cart.Total.Price *= 1.1
				return e
			}).
			Filter(func(e MableEvent) bool {
				return e.ESD.Cart.Total.Price > 500
			}).
			Collect(func(batch []MableEvent) { _ = len(batch) })

		p.Run(data)
		fmt.Printf("📊 BenchmarkMableEventPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}
