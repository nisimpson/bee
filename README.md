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
    task := bee.New(func(ctx context.Context, input string) (int, error) {
        return len(input), nil
    })

    // Create a worker with retry options
    worker := bee.Retry(task,
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
    task := bee.New(func(ctx context.Context, n int) (int, error) {
        // Simulate work
        time.Sleep(time.Second)

        // Report progress
        fmt.Printf("worker %s progress: %d%%",
            bee.TaskID(ctx),
            bee.TaskProgress(ctx),
        )

        return n * 2, nil
    })

    // Wrap task with retry options
    retry := bee.Retry(task,
        bee.WithRetryMaxAttempts(3),
        bee.WithRetryExponentially(time.Second, time.Second*10),
    )

    // Create a pool with 5 workers
    pool := bee.Pool(
        retry,
        bee.WithPoolMaxCapacity(5),
        bee.WithPoolWorkerIdleDuration(time.Millisecond*100),
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

### Stream Example

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/nisimpson/bee"
)

func main() {
    // Create a task that processes items from a channel
    task := bee.New(func(ctx context.Context, i int) (string, error) {
        // Simulate some work
        time.Sleep(100 * time.Millisecond)
        return fmt.Sprintf("processed-%d", i), nil
    })

    // Create a pool with 5 workers
    pool := bee.Pool(
        task,
        bee.WithPoolMaxCapacity(5),
        bee.WithPoolWorkerIdleDuration(time.Millisecond*100),
    )

    // Create a stream with buffer size and auto flush
    stream := bee.Stream(
        pool,
        bee.WithStreamBufferSize(10),
        bee.WithStreamFlushEvery(10 * time.Millisecond),
    )

    // Create an input channel
    input := make(chan int)
    go func() {
        // Send some items to process
        for i := 1; i <= 5; i++ {
            input <- i
        }
        close(input)
    }()

    // Process items from the channel
    ctx := context.Background()
    sink, err := bee.Start(ctx, stream, input)
    if err != nil {
        panic(err)
    }

    // Read results as they become available
    for result := range sink.Out() {
        fmt.Println(result)
    }

    // Check for any errors
    for err := range sink.Err() {
        fmt.Printf("Error: %v\n", err)
    }
}
```

## Features

- Type-safe task definitions using generics
- Configurable retry behavior with different backoff strategies
- Worker pool with concurrent task processing
- Pool stream for continuous procesing of inputs with buffering and automatic flushing
- Progress tracking through context
- Flexible options for both retry and pool configurations

## Configuration Options

### Retry Options

- `WithRetryMaxAttempts(n)`: Set maximum number of retry attempts
- `WithRetryEvery(duration)`: Use constant backoff between retries
- `WithRetryExponentially(start, max)`: Use exponential backoff between retries

### Pool Options

- `WithPoolMaxCapacity(n)`: Set maximum number of concurrent workers
- `WithPoolWorkerIdleDuration(duration)`: Set delay between task completions
