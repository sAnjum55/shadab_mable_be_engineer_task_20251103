package pipeline

// TestStruct represents a sample data type for benchmarking.
// It includes 10 fields of mixed primitive types to simulate
// a realistic workload.
type TestStruct struct {
	ID        int
	Name      string
	Value     float64
	Active    bool
	Category  string
	Timestamp int64
	Count     int
	Tag       string
	Ratio     float64
	Flag      bool
}
