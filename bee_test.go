package bee

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestNewTask(t *testing.T) {
	task := NewTask(func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("%d", in), nil
	})

	result, err := Execute(context.Background(), task, 42)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != "42" {
		t.Errorf("Expected result to be '42', got '%s'", result)
	}
}

func TestCompose(t *testing.T) {
	task1 := NewTask(func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("%d", in), nil
	})

	task2 := NewTask(func(ctx context.Context, in string) (string, error) {
		return in + "!", nil
	})

	composed := Compose2(task1, task2)
	result, err := Execute(context.Background(), composed, 42)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != "42!" {
		t.Errorf("Expected result to be '42!', got '%s'", result)
	}
}

func TestComposeWithError(t *testing.T) {
	expectedErr := errors.New("task1 error")

	task1 := NewTask(func(ctx context.Context, in int) (string, error) {
		return "", expectedErr
	})

	task2 := NewTask(func(ctx context.Context, in string) (string, error) {
		return in + "!", nil
	})

	composed := Compose2(task1, task2)
	_, err := Execute(context.Background(), composed, 42)
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestWorker(t *testing.T) {
	var attempts int
	expectedErr := errors.New("task error")

	task := NewTask(func(ctx context.Context, in string) (string, error) {
		attempts++
		if attempts < 3 {
			return "", expectedErr
		}
		return "success", nil
	})

	worker := Retry(task,
		WithMaxAttempts(5),
		WithRetry(RetryLinear(time.Millisecond)),
	)

	result, err := Execute(context.Background(), worker, "test")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if result != "success" {
		t.Errorf("Expected result to be 'success', got '%s'", result)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestWorkerMaxAttemptsExceeded(t *testing.T) {
	task := NewTask(func(ctx context.Context, in string) (string, error) {
		return "", errors.New("always fails")
	})

	worker := Retry(task,
		WithMaxAttempts(3),
		WithRetry(RetryLinear(time.Millisecond)),
	)

	_, err := Execute(context.Background(), worker, "test")
	if !errors.Is(err, ErrMaxAttempts) {
		t.Errorf("Expected ErrMaxAttempts, got %v", err)
	}
}

func TestStream(t *testing.T) {
	task := NewTask(func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	stream := Stream(task,
		WithMaxWorkers(3),
		WithMinCapacity(5),
	)

	// Create input channel
	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	// Process inputs
	output, errs := Execute(context.Background(), stream, input)

	// Collect results
	results := make(map[int]bool)
	for result := range output {
		results[result] = true
	}

	// Check for errors
	for err := range errs {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	// Verify results
	for i := 1; i <= 5; i++ {
		if !results[i*2] {
			t.Errorf("Expected result %d not found", i*2)
		}
	}
}

func TestStreamWithError(t *testing.T) {
	task := NewTask(func(ctx context.Context, in int) (int, error) {
		if in == 3 {
			return 0, errors.New("error on 3")
		}
		return in * 2, nil
	})

	stream := Stream(task, WithMinCapacity(2), WithMaxWorkers(2))

	// Create input channel
	input := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		input <- i
	}
	close(input)

	// Process inputs
	output, errs := Execute(context.Background(), stream, input)

	// Collect results
	results := make([]int, 0)
	for result := range output {
		results = append(results, result)
	}

	// Check for errors
	found := false
	for err := range errs {
		if strings.Contains(err.Error(), "error on 3") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Expected an error, got nil")
	}
}

func TestPipe(t *testing.T) {
	doubler := NewTask(func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	toString := NewTask(func(ctx context.Context, in int) (string, error) {
		return fmt.Sprintf("%d", in), nil
	})

	doubleStream := Stream(doubler, WithMaxWorkers(2))
	stringStream := Stream(toString, WithMaxWorkers(2))

	pipeline := Pipe2(doubleStream, stringStream)

	// Create input channel
	input := make(chan int, 3)
	input <- 1
	input <- 2
	input <- 3
	close(input)

	// Process inputs
	output, errs := Execute(context.Background(), pipeline, input)

	// Collect results
	results := make(map[string]bool)
	for result := range output {
		results[result] = true
	}

	// Check for errors
	for err := range errs {
		t.Fatalf("Expected no error, got %v", err)
		return
	}

	// Verify results
	expected := []string{"2", "4", "6"}
	for _, exp := range expected {
		if !results[exp] {
			t.Errorf("Expected result %s not found", exp)
		}
	}
}

func TestRetryLinear(t *testing.T) {
	retryer := RetryLinear(time.Second)

	delay1 := retryer.NextRetry(0)
	if delay1 != time.Second {
		t.Errorf("Expected delay to be 1s, got %v", delay1)
	}

	delay2 := retryer.NextRetry(delay1)
	if delay2 != time.Second {
		t.Errorf("Expected delay to be 1s, got %v", delay2)
	}
}

func TestRetryExponential(t *testing.T) {
	retryer := RetryExponential(time.Second, time.Second*8)

	delay1 := retryer.NextRetry(0)
	if delay1 != time.Second {
		t.Errorf("Expected first delay to be 1s, got %v", delay1)
	}

	delay2 := retryer.NextRetry(delay1)
	if delay2 != time.Second*2 {
		t.Errorf("Expected second delay to be 2s, got %v", delay2)
	}

	delay3 := retryer.NextRetry(delay2)
	if delay3 != time.Second*4 {
		t.Errorf("Expected third delay to be 4s, got %v", delay3)
	}

	delay4 := retryer.NextRetry(delay3)
	if delay4 != time.Second*8 {
		t.Errorf("Expected fourth delay to be 8s, got %v", delay4)
	}

	delay5 := retryer.NextRetry(delay4)
	if delay5 != time.Second*8 {
		t.Errorf("Expected fifth delay to be capped at 8s, got %v", delay5)
	}
}

func TestProgress(t *testing.T) {
	ctx := context.Background()
	taskCtx := &taskContext{}
	ctx = setContext(ctx, taskCtx)

	// Test initial progress
	if Progress(ctx) != 0 {
		t.Errorf("Expected initial progress to be 0, got %f", Progress(ctx))
	}

	// Test after initialization
	taskCtx.init(10)
	if Progress(ctx) != 0 {
		t.Errorf("Expected progress after init to be 0, got %f", Progress(ctx))
	}

	// Test after increments
	for i := 0; i < 5; i++ {
		taskCtx.increment()
	}

	progress := Progress(ctx)
	if progress <= 0 {
		t.Errorf("Expected progress to be positive, got %f", progress)
	}

	// Test with context without taskContext
	emptyCtx := context.Background()
	if Progress(emptyCtx) != 0 {
		t.Errorf("Expected progress with empty context to be 0, got %f", Progress(emptyCtx))
	}
}

func TestWorkerID(t *testing.T) {
	ctx := context.Background()

	// Test with no worker ID
	if WorkerID(ctx) != -1 {
		t.Errorf("Expected worker ID to be -1, got %d", WorkerID(ctx))
	}

	// Test with worker ID
	ctx = setWorkerID(ctx, 42)
	if WorkerID(ctx) != 42 {
		t.Errorf("Expected worker ID to be 42, got %d", WorkerID(ctx))
	}
}

func TestOptions(t *testing.T) {
	// Test default options
	opts := defaultOptions()
	if opts.attempts != 1 {
		t.Errorf("Expected default attempts to be 1, got %d", opts.attempts)
	}
	if opts.workers != 1 {
		t.Errorf("Expected default workers to be 1, got %d", opts.workers)
	}
	if opts.capacity != -1 {
		t.Errorf("Expected default capacity to be -1, got %d", opts.capacity)
	}

	// Test WithMaxAttempts
	WithMaxAttempts(5)(&opts)
	if opts.attempts != 5 {
		t.Errorf("Expected attempts to be 5, got %d", opts.attempts)
	}

	// Test WithMaxWorkers
	WithMaxWorkers(10)(&opts)
	if opts.workers != 10 {
		t.Errorf("Expected workers to be 10, got %d", opts.workers)
	}

	// Test WithMinCapacity
	WithMinCapacity(20)(&opts)
	if opts.capacity != 20 {
		t.Errorf("Expected capacity to be 20, got %d", opts.capacity)
	}

	// Test WithRetry
	customRetryer := RetryLinear(time.Minute)
	WithRetry(customRetryer)(&opts)
	if opts.retryer != customRetryer {
		t.Errorf("Expected retryer to be set correctly")
	}
}
