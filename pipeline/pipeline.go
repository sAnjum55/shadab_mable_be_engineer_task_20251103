package pipeline

import (
	"sync"
	"time"
)

type Pipeline[T any] struct {
	stages      []Stage[T]
	batchSize   int
	workerCount int
}

func New[T any]() *Pipeline[T] {
	return &Pipeline[T]{}
}

func (p *Pipeline[T]) AddStage(stage Stage[T]) {
	p.stages = append(p.stages, stage)
}

func (p *Pipeline[T]) Run(data []T) []T {
	if p.batchSize <= 0 {
		p.batchSize = len(data)
	}
	if p.workerCount <= 0 {
		p.workerCount = 1
	}

	batches := chunk(data, p.batchSize)
	results := make(chan []T, len(batches))
	metricsChan := make(chan StageMetrics, len(batches)*len(p.stages))

	var wg sync.WaitGroup
	workerPool := make(chan struct{}, p.workerCount)

	// ✅ Create a channel to signal when metrics emitter is done
	done := make(chan struct{})

	// ✅ Launch metrics emitter (in background)
	go func() {
		p.emitMetrics(metricsChan)
		close(done) // Signal completion
	}()

	// ✅ Start worker goroutines
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
				result, m = stage.Process(result)
				m.BatchID = id
				m.WorkerID = id
				m.Duration = time.Since(start)
				m.Timestamp = time.Now()
				metricsChan <- m // Emit metrics for every stage
			}

			results <- result
		}(batch, batchID)
	}

	// ✅ Close results & metrics after all workers finish
	go func() {
		wg.Wait()
		close(results)
		close(metricsChan)
	}()

	// ✅ Merge results
	var final []T
	for r := range results {
		final = append(final, r...)
	}

	// ✅ Wait for metrics emitter to finish before returning
	<-done

	return final
}

func (p *Pipeline[T]) Map(fn func(T) T) *Pipeline[T] {
	p.AddStage(MapStage[T]{fn})
	return p
}

func (p *Pipeline[T]) Filter(fn func(T) bool) *Pipeline[T] {
	p.AddStage(FilterStage[T]{fn})
	return p
}

func (p *Pipeline[T]) Collect(fn func([]T)) *Pipeline[T] {
	p.AddStage(CollectStage[T]{fn})
	return p
}

func (p *Pipeline[T]) WithBatchSize(size int) *Pipeline[T] {
	p.batchSize = size
	return p
}

func (p *Pipeline[T]) WithWorkers(count int) *Pipeline[T] {
	p.workerCount = count
	return p
}

func chunk[T any](data []T, size int) [][]T {
	var batches [][]T
	for size < len(data) {
		data, batches = data[size:], append(batches, data[0:size:size])
	}
	batches = append(batches, data)
	return batches
}

// Reduce (generic output)
func (p *Pipeline[T]) Reduce(fn func([]T) any) *Pipeline[T] {
	p.AddStage(ReduceStage[T, any]{fn: fn})
	return p
}

// Generate
func (p *Pipeline[T]) Generate(fn func(T) []T) *Pipeline[T] {
	p.AddStage(GenerateStage[T]{fn: fn})
	return p
}

// If
func (p *Pipeline[T]) If(cond func(T) bool, trueP, falseP *Pipeline[T]) *Pipeline[T] {
	p.AddStage(IfStage[T]{cond, trueP, falseP})
	return p
}
