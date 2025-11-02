package pipeline

import (
	"fmt"
	"time"
)

// ---------- MAP ----------
type MapStage[T any] struct {
	fn func(T) T
}

func (m MapStage[T]) Name() string { return "Map" }

func (m MapStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	out := make([]T, len(data))
	for i, v := range data {
		out[i] = m.fn(v)
	}
	duration := time.Since(start)
	return out, StageMetrics{
		StageName:   "Map",
		InputCount:  len(data),
		OutputCount: len(out),
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

// ---------- FILTER ----------
type FilterStage[T any] struct {
	fn func(T) bool
}

func (f FilterStage[T]) Name() string { return "Filter" }

func (f FilterStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	var out []T
	for _, v := range data {
		if f.fn(v) {
			out = append(out, v)
		}
	}
	duration := time.Since(start)
	return out, StageMetrics{
		StageName:   "Filter",
		InputCount:  len(data),
		OutputCount: len(out),
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

// ---------- COLLECT ----------
type CollectStage[T any] struct {
	fn func([]T)
}

func (c CollectStage[T]) Name() string { return "Collect" }

func (c CollectStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	c.fn(data)
	duration := time.Since(start)
	return data, StageMetrics{
		StageName:   "Collect",
		InputCount:  len(data),
		OutputCount: len(data),
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

// ---------- REDUCE ----------
type ReduceStage[T any, R any] struct {
	fn func([]T) R
}

func (r ReduceStage[T, R]) Name() string { return "Reduce" }

func (r ReduceStage[T, R]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	result := r.fn(data)
	fmt.Println("Reduce result:", result)
	duration := time.Since(start)
	return data, StageMetrics{
		StageName:   "Reduce",
		InputCount:  len(data),
		OutputCount: 1,
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

// ---------- GENERATE ----------
type GenerateStage[T any] struct {
	fn func(T) []T
}

func (g GenerateStage[T]) Name() string { return "Generate" }

func (g GenerateStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	var result []T
	for _, v := range data {
		result = append(result, g.fn(v)...)
	}
	duration := time.Since(start)
	return result, StageMetrics{
		StageName:   "Generate",
		InputCount:  len(data),
		OutputCount: len(result),
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

// ---------- IF ----------
type IfStage[T any] struct {
	cond   func(T) bool
	trueP  *Pipeline[T]
	falseP *Pipeline[T]
}

func (i IfStage[T]) Name() string { return "If" }

func (i IfStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	var result []T

	for _, item := range data {
		if i.cond(item) {
			out := i.trueP.Run([]T{item}) // ✅ run true pipeline
			result = append(result, out...)
		} else {
			out := i.falseP.Run([]T{item}) // ✅ run false pipeline
			result = append(result, out...)
		}
	}

	duration := time.Since(start)
	return result, StageMetrics{
		StageName:   "If",
		InputCount:  len(data),
		OutputCount: len(result),
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}
