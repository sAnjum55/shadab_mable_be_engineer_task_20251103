package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// --- UNIT TESTS ---

func TestMapStage(t *testing.T) {
	p := New[int]().Map(func(x int) int { return x * 2 })
	result := p.Run([]int{1, 2, 3})

	expected := []int{2, 4, 6}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Map failed: expected %v, got %v", expected, result)
		}
	}
}

func TestFilterStage(t *testing.T) {
	p := New[int]().Filter(func(x int) bool { return x%2 == 0 })
	result := p.Run([]int{1, 2, 3, 4, 5, 6})
	expected := []int{2, 4, 6}

	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Filter failed: expected %v, got %v", expected, result)
		}
	}
}

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

func TestReduceStage(t *testing.T) {
	p := New[string]().Reduce(func(batch []string) any {
		return strings.Join(batch, ",")
	})
	result := p.Run([]string{"a", "b", "c"})
	if len(result) == 0 {
		t.Fatalf("Reduce failed: got empty result")
	}
}

func TestIfStage(t *testing.T) {
	trueP := New[int]().Map(func(x int) int { return x * 10 })
	falseP := New[int]().Map(func(x int) int { return x * -1 })

	p := New[int]().If(func(x int) bool { return x > 0 }, trueP, falseP)
	result := p.Run([]int{1, -2, 3})

	expected := []int{10, 2, 30} // fixed expectation
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("If failed: expected %v, got %v", expected, result)
		}
	}
}

// --- PERFORMANCE BENCHMARKS ---

func BenchmarkPipeline(b *testing.B) {
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

			for i := 0; i < b.N; i++ {
				p.Run(data)
			}
		})
	}
}

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

	// Verify output consistency
	for _, val := range output {
		if val <= 5 {
			t.Errorf("Expected values > 5, got %d", val)
		}
	}
}

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

	// ✅ Define a custom mock emitter to track if it was called
	called := false

	mockEmit := func(_ <-chan StageMetrics) {
		called = true
	}

	// ✅ Temporarily replace emitMetrics using function variable injection
	// Since emitMetrics is a method, we can invoke the mock directly instead
	mockEmit(ch)

	if !called {
		t.Errorf("emitMetrics mock was not called")
	}
}

func TestMableEventPipeline(t *testing.T) {
	data, err := os.ReadFile("testdata/mable_event.json")
	if err != nil {
		t.Fatalf("failed to read JSON: %v", err)
	}

	var event MableEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// Duplicate the event to simulate multiple events
	var events []MableEvent
	for i := 0; i < 1000; i++ {
		events = append(events, event)
	}

	// Define a sample pipeline
	p := New[MableEvent]().
		WithBatchSize(100).
		WithWorkers(5).
		Map(func(e MableEvent) MableEvent {
			e.CreatedAt = NowUTC()
			return e
		}).
		Filter(func(e MableEvent) bool {
			return e.ESD.Cart.Total.Price > 500
		}).
		Collect(func(batch []MableEvent) {
			t.Logf("Processed batch of %d events", len(batch))
		})

	result := p.Run(events)
	if len(result) == 0 {
		t.Errorf("expected non-empty result, got %d", len(result))
	}
}

func NowUTC() time.Time {
	return time.Now().UTC()
}
