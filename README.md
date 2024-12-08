# bee

[![Test](https://github.com/nisimpson/bee/actions/workflows/test.yml/badge.svg)](https://github.com/nisimpson/bee/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/nisimpson/bee?status.svg)](http://godoc.org/github.com/nisimpson/bee)
[![Release](https://img.shields.io/github/release/nisimpson/bee.svg)](https://github.com/nisimpson/bee/releases)

A Go package that provides a flexible and type-safe worker pool implementation with retry capabilities. The package allows you to create workers that can process tasks concurrently, with configurable retry behavior and pool size management.

## Installation

```bash
go get github.com/nisimpson/bee
```

## Usage

### Basic Task Example

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/nisimpson/bee"
)

// Define a task that processes a string and returns its length
func main() {
    // Create a task function
    task := bee.NewTask(func(ctx context.Context, input string) (int, error) {
        return len(input), nil
    })

    // Create a worker with retry options
    worker := bee.NewWorker(task,
        bee.WithRetryMaxAttempts(3),
        bee.WithRetryEvery(time.Second),
    )

    // Execute the task
    result, err := bee.Start(context.Background(), worker, "hello")
    if err != nil {
        panic(err)
    }
    fmt.Printf("String length: %d\n", result)
}
```

### Worker Pool Example

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/nisimpson/bee"
)

func main() {
    // Create a task that processes numbers
    task := bee.NewTask(func(ctx context.Context, n int) (int, error) {
        // Simulate work
        time.Sleep(time.Second)

        // Report progress
        fmt.Printf("worker %s progress: %d%%",
          bee.WorkerInfo(ctx).WorkerID,
          bee.TaskProgress(ctx),
        )

        return n * 2, nil
    })

    // Create a worker with retry and pool options
    worker := bee.NewWorker(task,
        bee.WithRetryMaxAttempts(3),
        bee.WithRetryExponentially(time.Second, time.Second*10),
    )

    // Create a pool with 5 workers
    pool := worker.Pool(
        bee.WithPoolMaxWorkers(5),
        bee.WithPoolWorkerDelay(time.Millisecond*100),
    )

    // Process multiple inputs concurrently
    inputs := []int{1, 2, 3, 4, 5}
    results, err := bee.Start(context.Background(), pool, inputs)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Results: %v\n", results)
}
```

## Features

- Type-safe task definitions using generics
- Configurable retry behavior with different backoff strategies
- Worker pool with concurrent task processing
- Progress tracking through context
- Flexible options for both retry and pool configurations

## Configuration Options

### Retry Options

- `WithRetryMaxAttempts(n)`: Set maximum number of retry attempts
- `WithRetryEvery(duration)`: Use constant backoff between retries
- `WithRetryExponentially(start, max)`: Use exponential backoff between retries

### Pool Options

- `WithPoolMaxWorkers(n)`: Set maximum number of concurrent workers
- `WithPoolWorkerDelay(duration)`: Set delay between task completions
