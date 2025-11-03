// Package pipeline defines the individual stages that make up a data processing
// pipeline. Each stage implements a common interface with a Name() and Process()
// method, enabling the pipeline to process arbitrary data types generically.
//
// The supported stages include:
//   - Map: Transforms each element in the batch.
//   - Filter: Removes elements that do not match a condition.
//   - Collect: Performs side effects (e.g., saving, printing).
//   - Reduce: Combines a batch into a single aggregated result.
//   - Generate: Expands each input into multiple derived outputs.
//   - If: Routes elements conditionally through different sub-pipelines.
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

import (
	"log"
	"time"
)

//
// =======================
//  MAP STAGE
// =======================
//

// MapStage represents a pipeline stage that applies a transformation function
// to each element in the input batch. It outputs a new batch of the same size
// where every element has been transformed by the provided function.
type MapStage[T any] struct {
	fn func(T) T
}

// Name returns the stage identifier.
func (m MapStage[T]) Name() string { return "Map" }

// Process executes the map operation on the provided data batch. It measures
// the processing duration and returns both the transformed data and
// performance metrics for this stage.
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

//
// =======================
//  FILTER STAGE
// =======================
//

// FilterStage represents a pipeline stage that filters out elements from the
// input batch that do not satisfy a boolean predicate function. Only elements
// for which the predicate returns true are retained.
type FilterStage[T any] struct {
	fn func(T) bool
}

// Name returns the stage identifier.
func (f FilterStage[T]) Name() string { return "Filter" }

// Process applies the filter predicate to the input data. Elements for which
// the predicate returns true are included in the output. Metrics such as
// processing duration and item counts are also returned.
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

//
// =======================
//  COLLECT STAGE
// =======================
//

// CollectStage represents a terminal stage in the pipeline that performs a
// side-effecting operation on the processed data batch. It is typically used
// for logging, persisting, or displaying the data. The data itself is not
// modified and is returned as-is.
type CollectStage[T any] struct {
	fn func([]T)
}

// Name returns the stage identifier.
func (c CollectStage[T]) Name() string { return "Collect" }

// Process invokes the collection function on the input data and records
// performance metrics. The data is returned unmodified for possible downstream
// stages.
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

//
// =======================
//  REDUCE STAGE
// =======================
//

// ReduceStage represents a pipeline stage that aggregates a batch of data into
// a single result using a user-provided reduction function. The reduced value
// is printed for observability, but the original batch is returned unchanged
// for downstream compatibility.
type ReduceStage[T any, R any] struct {
	fn func([]T) R
}

// Name returns the stage identifier.
func (r ReduceStage[T, R]) Name() string { return "Reduce" }

// Process applies the reduction function to the input data, logs the result,
// and returns the original batch alongside execution metrics.
func (r ReduceStage[T, R]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	result := r.fn(data)
	log.Println("Reduce result:", result)
	duration := time.Since(start)

	return data, StageMetrics{
		StageName:   "Reduce",
		InputCount:  len(data),
		OutputCount: 1,
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}

//
// =======================
//  GENERATE STAGE
// =======================
//

// GenerateStage represents a pipeline stage that produces multiple new
// elements from each input element. It is useful for data expansion or
// enrichment operations (e.g., duplicating or creating derived values).
type GenerateStage[T any] struct {
	fn func(T) []T
}

// Name returns the stage identifier.
func (g GenerateStage[T]) Name() string { return "Generate" }

// Process applies the generation function to each input item, aggregates the
// expanded results, and returns them along with metrics.
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

//
// =======================
//  IF STAGE
// =======================
//

// IfStage represents a conditional branching stage that routes each input
// element to one of two sub-pipelines based on a boolean condition. This allows
// for complex data flows where elements follow different processing paths.
type IfStage[T any] struct {
	cond   func(T) bool
	trueP  *Pipeline[T]
	falseP *Pipeline[T]
}

// Name returns the stage identifier.
func (i IfStage[T]) Name() string { return "If" }

// Process evaluates the conditional function for each input element and routes
// the element through either the "true" or "false" sub-pipeline. Each branch
// is cloned per item to avoid shared-state issues in concurrent execution.
func (i IfStage[T]) Process(data []T) ([]T, StageMetrics) {
	start := time.Now()
	var result []T

	for _, item := range data {
		var out []T

		if i.cond(item) {
			// Clone the true pipeline before running
			trueClone := *i.trueP
			out = trueClone.Run([]T{item})
		} else {
			// Clone the false pipeline before running
			falseClone := *i.falseP
			out = falseClone.Run([]T{item})
		}

		result = append(result, out...)
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
