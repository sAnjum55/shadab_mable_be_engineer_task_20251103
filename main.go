package main

import (
	"flag"
	"fmt"
	"toy-pipeline/pipeline" // ✅ import your module
)

func main() {
	inputFile := flag.String("input", "pipeline/testdata/mable_event.json", "Path to input JSON file")
	outputFile := flag.String("output", "benchmarks.csv", "Path to output CSV file")
	flag.Parse()

	fmt.Println("🚀 Starting Mable Pipeline Benchmarks")
	fmt.Printf("Input file: %s\n", *inputFile)
	fmt.Printf("Output file: %s\n", *outputFile)

	// ✅ Run all benchmarks internally
	fmt.Println("🏗  Running embedded benchmarks...")
	pipeline.RunAllBenchmarks()
	fmt.Println("✅ Benchmarks complete. Results saved to CSV (if enabled).")
}
