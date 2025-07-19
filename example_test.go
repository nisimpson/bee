package bee_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nisimpson/bee/v2"
)

// This example demonstrates how to create and execute a simple [bee.Task].
func Example_simpleTask() {
	// Create a task that processes a string and returns its length
	task := bee.NewTask(func(ctx context.Context, input string) (int, error) {
		return len(input), nil
	})

	// Execute the task
	result, err := bee.Execute(context.Background(), task, "hello")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("String length: %d\n", result)
	// Output: String length: 5
}

// This example demonstrates how to create a [bee.Task] with retry capabilities.
func Example_retry() {
	// Create a task that might fail
	var attempts int
	task := bee.NewTask(func(ctx context.Context, input string) (string, error) {
		attempts++
		if attempts < 3 {
			return "", fmt.Errorf("attempt %d failed", attempts)
		}
		return fmt.Sprintf("processed: %s", input), nil
	})

	// Create a worker with retry options
	worker := bee.Retry(task,
		bee.WithMaxAttempts(5),
		bee.WithRetry(bee.RetryLinear(time.Millisecond)),
	)

	// Execute the task with retry
	_, err := bee.Execute(context.Background(), worker, "hello")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Result: processed: hello")
	// Output: Result: processed: hello
}

// This example demonstrates how to compose multiple tasks together.
func Example_composeTasks() {
	// Create a task that converts a string to uppercase
	uppercase := bee.NewTask(func(ctx context.Context, input string) (string, error) {
		return strings.ToUpper(input), nil
	})

	// Create a task that adds an exclamation mark
	exclaim := bee.NewTask(func(ctx context.Context, input string) (string, error) {
		return input + "!", nil
	})

	// Compose the tasks; you can chain composition if input and outputs are
	// the same type.
	pipeline := bee.Compose2(uppercase, bee.Compose(exclaim, exclaim, exclaim))

	// Execute the composed task
	result, err := bee.Execute(context.Background(), pipeline, "hello")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Result: %s\n", result)
	// Output: Result: HELLO!!!
}

// This example demonstrates how to use a [bee.TaskStream] to process multiple inputs concurrently.
func Example_stream() {
	// Create a task that processes numbers
	task := bee.NewTask(func(ctx context.Context, n int) (int, error) {
		// Simulate work
		time.Sleep(time.Millisecond * 10)
		return n * 2, nil
	})

	// Create a stream with multiple workers
	stream := bee.Stream(task,
		bee.WithMaxWorkers(3),
		bee.WithMinCapacity(5),
	)

	// Create an input channel
	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	// Process inputs concurrently
	ctx := context.Background()
	output, errs := bee.Execute(ctx, stream, input)

	// Check for errors
	for err := range errs {
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	}

	// Collect and print results
	results := make([]int, 0, 5)
	for result := range output {
		results = append(results, result)
	}

	slices.Sort(results)
	fmt.Printf("Results: %v\n", results)
	// Output: Results: [2 4 6 8 10]
}

// This example demonstrates how to [bee.Pipe2] multiple streams together.
func Example_pipeStreams() {
	// Create a task that doubles numbers
	doubler := bee.NewTask(func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	// Create a task that converts numbers to strings
	toString := bee.NewTask(func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("Value: %02d", n), nil
	})

	// Create streams for each task
	doubleStream := bee.Stream(doubler, bee.WithMaxWorkers(2))
	stringStream := bee.Stream(toString, bee.WithMaxWorkers(2))

	// Pipe the streams together; you can chain multiple pipes if the input
	// and output are the same type.
	pipeline := bee.Pipe2(bee.Pipe(doubleStream, doubleStream, doubleStream), stringStream)

	// Create an input channel
	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	// Process inputs through the pipeline
	ctx := context.Background()
	output, errs := bee.Execute(ctx, pipeline, input)

	// Collect and print results
	results := make([]string, 0, 3)
	for result := range output {
		results = append(results, result)
	}

	// Check for errors
	for err := range errs {
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	}

	// Sort results for consistent output
	slices.Sort(results)
	fmt.Println("Results:")
	for _, r := range results {
		fmt.Println(r)
	}
	// Output:
	// Results:
	// Value: 08
	// Value: 16
	// Value: 24
}

// This example demonstrates how to track [bee.Progress] of a [bee.Task].
func Example_trackProgress() {
	// Create a task that reports progress
	task := bee.NewTask(func(ctx context.Context, items []int) ([]int, error) {
		results := make([]int, 0, len(items))

		for _, item := range items {
			// Process the item
			results = append(results, item*2)
		}

		return results, nil
	})

	// Execute the task with a slice of items
	items := []int{1, 2, 3}
	_, err := bee.Execute(context.Background(), task, items)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	fmt.Println("Task completed")
	// Output: Task completed
}

// This example demonstrates how to cancel a [bee.Task] using context cancellation.
func Example_cancelTask() {
	// Create a task that simulates long-running work
	task := bee.NewTask(func(ctx context.Context, input string) (string, error) {
		// Simulate work that takes time
		select {
		case <-time.After(time.Second):
			return "completed", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay
	go func() {
		time.Sleep(time.Millisecond * 100)
		cancel()
	}()

	// Execute the task with the cancellable context
	result, err := bee.Execute(ctx, task, "test")
	if err != nil {
		fmt.Printf("Task cancelled: %v\n", err)
		return
	}

	fmt.Printf("Result: %s\n", result)
	// Output: Task cancelled: context canceled
}

// This example demonstrates how to cancel a [bee.TaskStream] using context cancellation.
func Example_cancelStream() {
	// Create a task that simulates processing work
	task := bee.NewTask(func(ctx context.Context, n int) (int, error) {
		select {
		case <-time.After(time.Millisecond * 100):
			return n * 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})

	// Create a stream with multiple workers
	stream := bee.Stream(task,
		bee.WithMaxWorkers(3),
		bee.WithMinCapacity(5),
	)

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and populate input channel
	input := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		input <- i
	}
	close(input)

	// Cancel the context after a short delay
	go func() {
		time.Sleep(time.Millisecond * 50)
		cancel()
	}()

	// Process inputs concurrently
	output, errs := bee.Execute(ctx, stream, input)

	// Track results and errors
	var results []int
	var cancelled bool

	// Collect results until cancellation
	for result := range output {
		_ = append(results, result)
	}

	// Check for cancellation error
	for err := range errs {
		if errors.Is(err, context.Canceled) {
			cancelled = true
			break
		}
	}

	fmt.Printf("Stream cancelled: %v\n", cancelled)
	// Output: Stream cancelled: true
}
