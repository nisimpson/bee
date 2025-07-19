# bee

[![Test](https://github.com/nisimpson/bee/actions/workflows/test.yml/badge.svg)](https://github.com/nisimpson/bee/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/nisimpson/bee?status.svg)](http://godoc.org/github.com/nisimpson/bee)
[![Release](https://img.shields.io/github/release/nisimpson/bee.svg)](https://github.com/nisimpson/bee/releases)

A Go package that provides a flexible and type-safe worker pool implementation with retry capabilities. The package allows you to create workers that can process tasks concurrently, with configurable retry behavior and pool size management.

## Installation

```bash
go get github.com/nisimpson/bee/v2
```

## Usage

### Simple Task Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/nisimpson/bee/v2"
)

func main() {
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
}
// Output: String length: 5
```

### Retry Example

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/nisimpson/bee/v2"
)

func main() {
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
    result, err := bee.Execute(context.Background(), worker, "hello")
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("Result: %s\n", result)
}
// Output: Result: processed: hello
```

### Task Composition Example

```go
package main

import (
    "context"
    "fmt"
    "strings"
    "github.com/nisimpson/bee/v2"
)

func main() {
    // Create a task that converts a string to uppercase
    uppercase := bee.NewTask(func(ctx context.Context, input string) (string, error) {
        return strings.ToUpper(input), nil
    })

    // Create a task that adds an exclamation mark
    exclaim := bee.NewTask(func(ctx context.Context, input string) (string, error) {
        return input + "!", nil
    })

    // Compose the tasks
    pipeline := bee.Compose2(uppercase, bee.Compose(exclaim, exclaim, exclaim))

    // Execute the composed task
    result, err := bee.Execute(context.Background(), pipeline, "hello")
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("Result: %s\n", result)
}
// Output: Result: HELLO!!!
```

### Stream Example

```go
package main

import (
    "context"
    "fmt"
    "slices"
    "time"
    "github.com/nisimpson/bee/v2"
)

func main() {
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

    // Collect and sort results
    results := make([]int, 0, 5)
    for result := range output {
        results = append(results, result)
    }
    slices.Sort(results)

    fmt.Printf("Results: %v\n", results)
}
// Output: Results: [2 4 6 8 10]
```

### Stream Pipeline Example

```go
package main

import (
    "context"
    "fmt"
    "slices"
    "github.com/nisimpson/bee/v2"
)

func main() {
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

    // Pipe the streams together
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

    // Collect results
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

    // Sort and print results
    slices.Sort(results)
    fmt.Println("Results:")
    for _, r := range results {
        fmt.Println(r)
    }
}
// Output:
// Results:
// Value: 08
// Value: 16
// Value: 24
```

## Features

- Type-safe task definitions using generics
- Configurable retry behavior with different backoff strategies
- Task composition for building processing pipelines
- Concurrent task processing with worker pools
- Stream processing with automatic worker management
- Stream pipeline composition for complex workflows
- Progress tracking through context
- Context-based cancellation support
- Flexible configuration options

## Configuration Options

### Task Options

- `WithMaxAttempts(n)`: Set maximum number of retry attempts
- `WithRetry(retryer)`: Set retry strategy (Linear or Exponential)

### Stream Options

- `WithMaxWorkers(n)`: Set maximum number of concurrent workers
- `WithMinCapacity(n)`: Set minimum channel buffer capacity

### Retry Strategies

- `RetryLinear(period)`: Use constant backoff between retries
- `RetryExponential(initial, max)`: Use exponential backoff between retries
