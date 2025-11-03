// Package pipeline implements a generic and extensible data processing pipeline
// framework in Go. A Pipeline consists of multiple composable stages (e.g. Map,
// Filter, Generate, Reduce, If, Collect), each of which processes batches of
// data elements concurrently using a configurable number of worker goroutines.
//
// The design emphasizes:
//   - Type safety via Go generics.
//   - Modularity through stage interfaces.
//   - Configurability of batch sizes and concurrency levels.
//   - Extensibility for future stage types.
//   - Observability through performance metrics.
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

import (
	"runtime"
	"sync"
	"time"
)

//
// =======================
//  PIPELINE STRUCTURE
// =======================
//

// Pipeline represents a generic, concurrent data processing pipeline.
// It operates on elements of type T and executes a sequence of stages.
// Each stage performs a specific operation (e.g., Map, Filter, Reduce)
// on batches of input data.
//
// The pipeline is designed to be modular, type-safe, and parallelizable.
// Developers can configure batch sizes, worker counts, and add custom stages.
type Pipeline[T any] struct {
	stages      []Stage[T] // ordered list of stages to execute
	batchSize   int        // number of elements per batch
	workerCount int        // number of concurrent workers
}

// New creates and returns a new, empty Pipeline for elements of type T.
func New[T any]() *Pipeline[T] {
	return &Pipeline[T]{}
}

// AddStage appends a processing stage to the pipeline.
// Each stage must implement the Stage[T] interface.
func (p *Pipeline[T]) AddStage(stage Stage[T]) {
	p.stages = append(p.stages, stage)
}

// autoTune dynamically adjusts pipeline hyperparameters such as
// batch size and worker count at runtime, based on system resources
// and input data size.
//
// This implements the assignment’s optional “dynamic hyperparameters” feature.
//
// Logic:
//   - The number of CPU cores determines the default worker count.
//   - The total data size is divided evenly across workers, so the
//     number of batches ≈ number of CPU cores.
//   - This ensures efficient parallelism and load balancing.
//
// Example:
//
//	On an 8-core system processing 80,000 items:
//	  → workerCount = 8
//	  → batchSize = 10,000
//
// This approach automatically scales the pipeline to available
// compute capacity without requiring manual configuration.
func (p *Pipeline[T]) autoTune(data []T) {
	cpuCount := runtime.NumCPU()

	// Automatically assign worker count if not set manually
	if p.workerCount == 0 {
		p.workerCount = cpuCount
	}

	totalItems := len(data)
	if totalItems == 0 {
		p.batchSize = 1
		return
	}

	// Compute batch size so total batches ≈ worker count
	batchSize := totalItems / p.workerCount
	if batchSize < 1 {
		batchSize = 1
	}

	p.batchSize = batchSize
}

//
// =======================
//  PIPELINE EXECUTION
// =======================
//

// Run executes the configured pipeline over the provided input data.
// It automatically tunes batch size and worker count based on input size
// and system CPU capacity (via autoTune), splits the input into batches,
// processes each batch through all stages concurrently, and collects results.
//
// The function also emits per-stage metrics to ClickHouse for observability.
func (p *Pipeline[T]) Run(data []T) []T {
	// Automatically determine optimal batch size and workers
	p.autoTune(data)

	// Split input data into batches based on tuned batch size
	batches := chunk(data, p.batchSize)
	results := make(chan []T, len(batches))
	metricsChan := make(chan StageMetrics, len(batches)*len(p.stages))

	var wg sync.WaitGroup
	workerPool := make(chan struct{}, p.workerCount)
	done := make(chan struct{}) // signals when metrics emission completes

	// Launch metrics emitter in the background
	go func() {
		p.emitMetrics(metricsChan)
		close(done)
	}()

	// Launch worker goroutines to process batches concurrently
	for batchID, batch := range batches {
		wg.Add(1)
		workerPool <- struct{}{}

		go func(b []T, id int) {
			defer wg.Done()
			defer func() { <-workerPool }()

			result := append([]T(nil), b...)
			for _, stage := range p.stages {
				var m StageMetrics
				start := time.Now()

				// Process the batch through the current stage
				result, m = stage.Process(result)

				// Record metrics
				m.BatchID = id
				m.WorkerID = id
				m.Duration = time.Since(start)
				m.Timestamp = time.Now()
				metricsChan <- m
			}

			results <- result
		}(batch, batchID)
	}

	// Close channels after all workers finish
	go func() {
		wg.Wait()
		close(results)
		close(metricsChan)
	}()

	// Merge results from all workers
	var final []T
	for r := range results {
		final = append(final, r...)
	}

	// Wait for metrics emitter to finish before returning
	<-done

	return final
}

//
// =======================
//  STAGE BUILDERS
// =======================
//

// Map adds a MapStage to the pipeline. The provided function transforms
// each element of the batch and produces an output of the same type.
func (p *Pipeline[T]) Map(fn func(T) T) *Pipeline[T] {
	p.AddStage(MapStage[T]{fn})
	return p
}

// Filter adds a FilterStage to the pipeline. The provided function
// acts as a predicate that determines which elements are retained.
func (p *Pipeline[T]) Filter(fn func(T) bool) *Pipeline[T] {
	p.AddStage(FilterStage[T]{fn})
	return p
}

// Collect adds a CollectStage to the pipeline. The provided function is invoked
// for each batch to perform side effects such as logging, saving, or reporting.
// The data itself is not modified.
func (p *Pipeline[T]) Collect(fn func([]T)) *Pipeline[T] {
	p.AddStage(CollectStage[T]{fn})
	return p
}

// Reduce adds a ReduceStage to the pipeline. The provided function aggregates
// all elements in a batch into a single result. The batch itself is returned
// unmodified to preserve pipeline continuity.
func (p *Pipeline[T]) Reduce(fn func([]T) any) *Pipeline[T] {
	p.AddStage(ReduceStage[T, any]{fn: fn})
	return p
}

// Generate adds a GenerateStage to the pipeline. The provided function produces
// multiple new elements for each input element, enabling data expansion.
func (p *Pipeline[T]) Generate(fn func(T) []T) *Pipeline[T] {
	p.AddStage(GenerateStage[T]{fn: fn})
	return p
}

// If adds an IfStage to the pipeline. The condition function determines whether
// each element should be processed through the true or false sub-pipeline.
// This allows branching logic within a single pipeline.
func (p *Pipeline[T]) If(cond func(T) bool, trueP, falseP *Pipeline[T]) *Pipeline[T] {
	p.AddStage(IfStage[T]{cond, trueP, falseP})
	return p
}

//
// =======================
//  CONFIGURATION HELPERS
// =======================
//

// WithBatchSize sets the batch size for the pipeline.
// A batch defines how many elements are processed together.
func (p *Pipeline[T]) WithBatchSize(size int) *Pipeline[T] {
	p.batchSize = size
	return p
}

// WithWorkers sets the number of concurrent workers (goroutines) that process
// batches in parallel. This controls the level of parallelism.
func (p *Pipeline[T]) WithWorkers(count int) *Pipeline[T] {
	p.workerCount = count
	return p
}

//
// =======================
//  INTERNAL HELPERS
// =======================
//

// chunk divides the input data slice into smaller batches of the specified size.
// The last batch may be smaller if the data length is not divisible by size.
func chunk[T any](data []T, size int) [][]T {
	var batches [][]T
	for size < len(data) {
		data, batches = data[size:], append(batches, data[0:size:size])
	}
	batches = append(batches, data)
	return batches
}
