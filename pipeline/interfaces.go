package pipeline

type Stage[T any] interface {
	Process([]T) ([]T, StageMetrics)
	Name() string
}
