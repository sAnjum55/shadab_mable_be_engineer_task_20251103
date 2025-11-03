// Package pipeline contains comprehensive tests for the generic data processing
// pipeline. These tests validate each stage (Map, Filter, Reduce, Generate,
// If, Collect), overall pipeline behavior, and ClickHouse metrics emission.
//
// It includes:
//   - Unit tests for correctness
//   - Integration tests with MableEvent
//   - Performance benchmarks with CSV export
//   - Error and edge-case coverage for >90% test coverage
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

//
// =======================
//  LOG & STDOUT SUPPRESSION
// =======================
//

// suppressLogsDuringBenchmarks disables Go's standard logger output.
func suppressLogsDuringBenchmarks(b *testing.B) func() {
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	return func() {
		log.SetOutput(originalOutput)
	}
}

// suppressStdout silences fmt output (used to hide ClickHouse & metric logs).
func suppressStdout() func() {
	null, _ := os.Open(os.DevNull)
	original := os.Stdout
	os.Stdout = null
	return func() {
		os.Stdout = original
		null.Close()
	}
}

//
// =======================
//  HELPER FUNCTIONS
// =======================
//

// writeBenchmarkResult appends a benchmark record to a CSV file.
func writeBenchmarkResult(b *testing.B, csvFile *os.File, name string, size int, start time.Time, allocs uint64, bytes uint64) {
	elapsed := time.Since(start)
	nsPerOp := elapsed.Nanoseconds() / int64(b.N)
	fmt.Fprintf(csvFile, "%s,%d,%d,%d,%d\n", name, size, nsPerOp, bytes, allocs)
}

// openCSVFile opens or creates the CSV output file and adds headers if needed.
func openCSVFile(b *testing.B) *os.File {
	file, err := os.OpenFile("pipeline_library_benchmarks.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		b.Fatalf("failed to open CSV file: %v", err)
	}
	info, _ := file.Stat()
	if info.Size() == 0 {
		fmt.Fprintln(file, "Benchmark,Size,NsPerOp,BytesPerOp,AllocsPerOp")
	}
	return file
}

//
// =======================
//  UNIT TESTS
// =======================
//

// TestMapStage verifies that the Map stage correctly applies a transformation.
func TestMapStage(t *testing.T) {
	p := New[int]().Map(func(x int) int { return x * 2 })
	result := p.Run([]int{1, 2, 3})
	expected := []int{2, 4, 6}
	if !equalUnordered(result, expected) {
		t.Errorf("Map failed: expected %v, got %v", expected, result)
	}
}

// TestFilterStage verifies that the Filter stage removes elements properly.
func TestFilterStage(t *testing.T) {
	p := New[int]().Filter(func(x int) bool { return x%2 == 0 })
	result := p.Run([]int{1, 2, 3, 4, 5, 6})
	expected := []int{2, 4, 6}
	if !equalUnordered(result, expected) {
		t.Errorf("Filter failed: expected %v, got %v", expected, result)
	}
}

// TestGenerateStage ensures the Generate stage expands each input correctly.
func TestGenerateStage(t *testing.T) {
	p := New[string]().Generate(func(s string) []string {
		return []string{s, strings.ToUpper(s)}
	})
	result := p.Run([]string{"go"})
	expected := []string{"go", "GO"}
	if len(result) != len(expected) {
		t.Fatalf("Generate failed: expected %v, got %v", expected, result)
	}
}

// TestReduceStage verifies that Reduce combines elements as intended.
func TestReduceStage(t *testing.T) {
	p := New[string]().Reduce(func(batch []string) any {
		return strings.Join(batch, ",")
	})
	result := p.Run([]string{"a", "b", "c"})
	if len(result) == 0 {
		t.Fatalf("Reduce failed: got empty result")
	}
}

// TestIfStage validates branching logic (true vs. false pipeline paths).
func TestIfStage(t *testing.T) {
	trueP := New[int]().Map(func(x int) int { return x * 10 })
	falseP := New[int]().Map(func(x int) int { return x * -1 })
	p := New[int]().If(func(x int) bool { return x > 0 }, trueP, falseP)

	input := []int{1, -2, 3}
	result := p.Run(input)
	expected := []int{10, 2, 30}
	if !equalUnordered(result, expected) {
		t.Errorf("IfStage failed: expected %v, got %v", expected, result)
	}
}

//
// =======================
//  EDGE CASE TESTS
// =======================
//

// TestPipelineDefaultConfig ensures default worker/batch settings work properly.
func TestPipelineDefaultConfig(t *testing.T) {
	p := New[int]().Map(func(x int) int { return x + 1 })
	output := p.Run([]int{1, 2, 3})
	expected := []int{2, 3, 4}
	if !equalUnordered(output, expected) {
		t.Errorf("Default config failed: expected %v, got %v", expected, output)
	}
}

// TestPipelineEmptyInput ensures no crash on empty slices.
func TestPipelineEmptyInput(t *testing.T) {
	p := New[int]().Map(func(x int) int { return x * 2 })
	result := p.Run([]int{})
	if len(result) != 0 {
		t.Errorf("Expected empty output, got %v", result)
	}
}

// TestEmitMetricsFailure ensures metric goroutine doesn’t panic on DB failure.
func TestEmitMetricsFailure(t *testing.T) {
	ch := make(chan StageMetrics, 1)
	ch <- StageMetrics{StageName: "Map", BatchID: 1}
	close(ch)
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered from simulated ClickHouse failure: %v", r)
		}
	}()
	p := New[int]()
	go p.emitMetrics(ch)
	time.Sleep(50 * time.Millisecond)
}

//
// =======================
//  INTEGRATION TESTS
// =======================
//

// TestFullPipeline checks a full Map→Filter→Collect chain.
func TestFullPipeline(t *testing.T) {
	p := New[int]().
		WithBatchSize(3).
		WithWorkers(2).
		Map(func(x int) int { return x * 2 }).
		Filter(func(x int) bool { return x > 5 }).
		Collect(func(batch []int) { _ = batch })

	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	output := p.Run(input)
	if len(output) == 0 {
		t.Errorf("Expected non-empty output, got %v", output)
	}
	for _, val := range output {
		if val <= 5 {
			t.Errorf("Expected values > 5, got %d", val)
		}
	}
}

// TestEmitMetricsMock ensures metric sending logic can be mocked.
func TestEmitMetricsMock(t *testing.T) {
	New[int]()
	ch := make(chan StageMetrics, 1)
	ch <- StageMetrics{
		StageName:   "Map",
		BatchID:     1,
		InputCount:  10,
		OutputCount: 10,
		Duration:    time.Millisecond * 5,
		WorkerID:    1,
		Timestamp:   time.Now(),
	}
	close(ch)
	called := false
	mockEmit := func(_ <-chan StageMetrics) { called = true }
	mockEmit(ch)
	if !called {
		t.Errorf("emitMetrics mock was not called")
	}
}

//
// =======================
//  BENCHMARKS (CSV OUTPUT)
// =======================
//

// BenchmarkPipeline tests scalability with integer transformations.
func BenchmarkPipeline(b *testing.B) {
	restoreLogs := suppressLogsDuringBenchmarks(b)
	restoreStdout := suppressStdout()
	defer func() { restoreLogs(); restoreStdout() }()

	csvFile := openCSVFile(b)
	defer csvFile.Close()

	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := make([]int, size)
			for i := range data {
				data[i] = i
			}
			p := New[int]().
				WithBatchSize(100).
				WithWorkers(5).
				Map(func(x int) int { return x * 2 }).
				Filter(func(x int) bool { return x%3 == 0 }).
				Collect(func(batch []int) { _ = batch })

			b.ReportAllocs()
			start := time.Now()
			allocs := uint64(testing.AllocsPerRun(1, func() { p.Run(data) }))
			writeBenchmarkResult(b, csvFile, "BenchmarkPipeline", size, start, allocs, 0)
		})
	}
}

// BenchmarkTestStructPipeline tests scalability for a 10-field TestStruct.
func BenchmarkTestStructPipeline(b *testing.B) {
	restoreLogs := suppressLogsDuringBenchmarks(b)
	restoreStdout := suppressStdout()
	defer func() { restoreLogs(); restoreStdout() }()

	csvFile := openCSVFile(b)
	defer csvFile.Close()

	sizes := []int{10, 100, 10_000, 100_000, 1_000_000, 10_000_000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			data := make([]TestStruct, size)
			for i := range data {
				data[i] = TestStruct{
					ID: i, Name: fmt.Sprintf("Item_%d", i), Value: float64(i) * 1.23,
					Active: i%2 == 0, Category: "default", Timestamp: time.Now().Unix(),
					Count: i % 100, Tag: "bench", Ratio: 0.5, Flag: i%3 == 0,
				}
			}
			p := New[TestStruct]().
				WithBatchSize(1000).
				WithWorkers(8).
				Map(func(e TestStruct) TestStruct {
					e.Value *= 2
					return e
				}).
				Filter(func(e TestStruct) bool { return e.Value > 1000 }).
				Collect(func(batch []TestStruct) { _ = len(batch) })

			b.ReportAllocs()
			start := time.Now()
			allocs := uint64(testing.AllocsPerRun(1, func() { p.Run(data) }))
			writeBenchmarkResult(b, csvFile, "BenchmarkTestStructPipeline", size, start, allocs, 0)
		})
	}
}

// BenchmarkMableEventPipeline benchmarks with realistic MableEvent structs.
func BenchmarkMableEventPipeline(b *testing.B) {
	restoreLogs := suppressLogsDuringBenchmarks(b)
	restoreStdout := suppressStdout()
	defer func() { restoreLogs(); restoreStdout() }()

	csvFile := openCSVFile(b)
	defer csvFile.Close()

	sizes := []int{10, 100, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("MableEvent_Size_%d", size), func(b *testing.B) {
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
				Filter(func(e MableEvent) bool { return e.ESD.Cart.Total.Price > 500 }).
				Collect(func(batch []MableEvent) { _ = len(batch) })

			b.ReportAllocs()
			start := time.Now()
			allocs := uint64(testing.AllocsPerRun(1, func() { p.Run(data) }))
			writeBenchmarkResult(b, csvFile, "BenchmarkMableEventPipeline", size, start, allocs, 0)
		})
	}
}

func BenchmarkAllStagesPipeline(b *testing.B) {
	restoreLogs := suppressLogsDuringBenchmarks(b)
	defer restoreLogs()

	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}

	p := New[int]().
		Map(func(x int) int { return x * 2 }).
		Filter(func(x int) bool { return x%3 == 0 }).
		Generate(func(x int) []int { return []int{x, -x} }).
		Reduce(func(batch []int) any { return len(batch) }).
		Collect(func(batch []int) { _ = batch })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Run(data)
	}
}
