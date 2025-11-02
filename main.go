package main

import (
	"fmt"
	"strings"
	"time"
	"toy-pipeline/pipeline"
)

func main() {
	fmt.Println("🚀 Starting Full Pipeline Demo (All Stages)")

	// =====================
	// 1️⃣ String pipeline — demonstrates Generate, Map, Filter, Reduce, Collect
	// =====================
	stringPipeline := pipeline.New[string]().
		WithBatchSize(2).
		WithWorkers(2).
		// Generate stage: duplicates each string and adds an uppercase version
		Generate(func(s string) []string {
			return []string{s, strings.ToUpper(s)}
		}).
		// Map stage: adds a suffix
		Map(func(s string) string {
			time.Sleep(20 * time.Millisecond)
			return s + "_processed"
		}).
		// Filter stage: keep only those containing "A"
		Filter(func(s string) bool {
			return strings.Contains(s, "A")
		}).
		// Reduce stage: concatenate batch into one string
		Reduce(func(batch []string) any {
			return strings.Join(batch, " | ")
		}).
		// Collect stage: output batch result
		Collect(func(batch []string) {
			fmt.Println("🧩 [String Batch Output]:", batch)
		})

	inputStrings := []string{"alpha", "beta", "gamma"}
	fmt.Println("🎬 Input Strings:", inputStrings)

	stringResult := stringPipeline.Run(inputStrings)
	fmt.Println("✅ Final String Pipeline Output:", stringResult)

	// =====================
	// 2️⃣ Integer pipeline with If condition — demonstrates branching
	// =====================
	truePipeline := pipeline.New[int]().
		Map(func(x int) int { return x * 10 }).
		Collect(func(data []int) {
			fmt.Println("✅ True branch executed:", data)
		})

	falsePipeline := pipeline.New[int]().
		Map(func(x int) int { return x * -1 }).
		Collect(func(data []int) {
			fmt.Println("❌ False branch executed:", data)
		})

	intPipeline := pipeline.New[int]().
		WithBatchSize(3).
		WithWorkers(2).
		If(func(x int) bool { return x > 0 }, truePipeline, falsePipeline).
		Collect(func(data []int) {
			fmt.Println("🔹 [IfStage Batch Result]:", data)
		})

	intInput := []int{1, -2, 3, -4, 5}
	fmt.Println("\n🎬 Input Integers:", intInput)

	intResult := intPipeline.Run(intInput)
	fmt.Println("✅ Final If Pipeline Output:", intResult)

	fmt.Println("\n🎉 Pipeline demonstration complete — all 6 stages executed successfully!")
}
