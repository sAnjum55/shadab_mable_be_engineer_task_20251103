package pipeline

import (
	"fmt"
	"math/rand"
	"runtime"
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

// ======================================================
// Benchmark #1 — Integer Pipeline
// Map → Filter → Generate → Reduce → Collect
// ======================================================
func runIntPipelineBenchmark() {
	numCores := runtime.NumCPU()
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}

	fmt.Printf("🧠 Using %d CPU cores for integer benchmark (batch size = %d)\n", numCores, numCores)

	for _, size := range sizes {
		start := time.Now()

		data := make([]int, size)
		for i := range data {
			data[i] = rand.Intn(1000)
		}

		p := New[int]().
			WithBatchSize(numCores).
			WithWorkers(numCores).
			Map(func(x int) int { return x * 2 }).
			Filter(func(x int) bool { return x%3 == 0 }).
			Generate(func(x int) []int { return []int{x, x + 1, x + 2} }).
			Reduce(func(batch []int) any {
				sum := 0
				for _, v := range batch {
					sum += v
				}
				return sum
			}).
			Collect(func(batch []int) { _ = len(batch) })

		p.Run(data)
		fmt.Printf("📊 BenchmarkPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}

// ==========================================================
// Benchmark #2 — Custom Struct Pipeline
// Map → Filter → Generate → Reduce → Collect
// ==========================================================
func runStructPipelineBenchmark() {
	numCores := runtime.NumCPU()
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}

	fmt.Printf("🧠 Using %d CPU cores for struct benchmark (batch size = %d)\n", numCores, numCores)

	for _, size := range sizes {
		start := time.Now()

		data := make([]TestStruct, size)
		for i := range data {
			data[i] = TestStruct{
				ID:        i,
				Name:      fmt.Sprintf("Item_%d", i),
				Value:     rand.Float64() * 5000,
				Active:    i%2 == 0,
				Category:  []string{"alpha", "beta"}[i%2],
				Timestamp: time.Now().Unix(),
			}
		}

		p := New[TestStruct]().
			WithBatchSize(numCores).
			WithWorkers(numCores).
			Map(func(e TestStruct) TestStruct {
				e.Value += 10
				return e
			}).
			Filter(func(e TestStruct) bool { return e.Value > 1000 }).
			Generate(func(e TestStruct) []TestStruct {
				return []TestStruct{e, {ID: e.ID + 1, Name: e.Name + "_dup", Value: e.Value * 0.95}}
			}).
			Reduce(func(batch []TestStruct) any {
				var total float64
				for _, e := range batch {
					total += e.Value
				}
				return map[string]any{
					"total_value": total,
					"count":       len(batch),
				}
			}).
			Collect(func(batch []TestStruct) {
				_ = len(batch)
			})

		p.Run(data)
		fmt.Printf("📊 BenchmarkTestStructPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}

// ===============================================================
// Benchmark #3 — Mable Event Pipeline
// Map → Generate → Filter → Reduce → Collect
// ===============================================================
func runMableEventBenchmark() {
	numCores := runtime.NumCPU()
	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}

	fmt.Printf("🧠 Using %d CPU cores for Mable Event benchmark (batch size = %d)\n", numCores, numCores)

	for _, size := range sizes {
		start := time.Now()

		data := make([]MableEvent, size)
		for i := range data {
			data[i] = MableEvent{
				EID: fmt.Sprintf("event_%d", i),
				EN:  []string{"Add To Cart", "Order Completed"}[i%2],
				TS:  time.Now().Unix(),
			}
		}

		p := New[MableEvent]().
			WithBatchSize(numCores).
			WithWorkers(numCores).
			Map(func(e MableEvent) MableEvent {
				e.CreatedAt = time.Now()
				e.ESD.Cart.Total.Price = rand.Float64() * 1000
				return e
			}).
			Generate(func(e MableEvent) []MableEvent {
				return []MableEvent{
					e,
					{EID: e.EID + "_audit", EN: "Audit Event", TS: e.TS, CreatedAt: e.CreatedAt},
				}
			}).
			Filter(func(e MableEvent) bool {
				return e.ESD.Cart.Total.Price > 500
			}).
			Reduce(func(batch []MableEvent) any {
				var maxEvent MableEvent
				maxPrice := 0.0
				for _, e := range batch {
					if e.ESD.Cart.Total.Price > maxPrice {
						maxPrice = e.ESD.Cart.Total.Price
						maxEvent = e
					}
				}
				return map[string]any{
					"event": maxEvent.EID,
					"price": maxPrice,
				}
			}).
			Collect(func(batch []MableEvent) { _ = len(batch) })

		p.Run(data)
		fmt.Printf("📊 BenchmarkMableEventPipeline | Size=%d | Duration=%s\n", size, time.Since(start))
	}
}
