// Package pipeline defines the core components of a generic, concurrent data
// processing pipeline. Each pipeline is composed of multiple stages, where
// every stage implements the Stage interface defined in this file.
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

// Stage defines the common interface that every pipeline stage must implement.
// It provides a consistent structure for how data flows through the pipeline.
//
// The Stage interface is generic — parameterized by type T — meaning it can
// operate on any kind of data (integers, strings, structs, etc.) without
// rewriting logic for each type.
//
// Each stage is responsible for transforming, filtering, generating, or
// aggregating data elements in a batch.
//
// Example implementations include:
//   - MapStage[T]      — applies a function to each element
//   - FilterStage[T]   — removes elements based on a condition
//   - GenerateStage[T] — expands each input into multiple outputs
//   - ReduceStage[T,R] — aggregates a batch into a single value
//   - IfStage[T]       — conditionally routes elements through sub-pipelines
//   - CollectStage[T]  — sends final results to a sink (e.g., logs or DB)
//
// The interface has two methods:
//
//	Process(data []T) ([]T, StageMetrics)
//	    Processes a batch of input elements and returns the transformed batch
//	    along with metrics describing execution details (time taken, counts, etc.)
//
//	Name() string
//	    Returns the display name of the stage (e.g., "Map", "Filter") used in logs
//	    and observability metrics.
type Stage[T any] interface {
	// Process runs the stage logic on a slice of input elements and returns
	// a slice of processed outputs along with collected stage metrics.
	Process([]T) ([]T, StageMetrics)

	// Name returns the name of the stage — useful for debugging, logging,
	// and metrics visualization in ClickHouse and Grafana.
	Name() string
}
