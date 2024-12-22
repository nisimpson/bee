package bee_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nisimpson/bee/v2"
)

func TestNew(t *testing.T) {
	t.Run("successful task execution", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})

		ctx := context.Background()
		result, err := bee.Start(ctx, task, 5)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected result 10, got %d", result)
		}
	})

	t.Run("task execution with error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})

		ctx := context.Background()
		_, err := bee.Start(ctx, task, 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("task with context cancellation", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.Start(ctx, task, 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}

func TestNewStream(t *testing.T) {
	task := bee.New(func(ctx context.Context, i int) (int, error) {
		return i * 2, nil
	})

	t.Run("creation with no options", func(t *testing.T) {
		bee.Stream(bee.Pool(task))
	})

	t.Run("creation with options", func(t *testing.T) {
		bee.Stream(
			bee.Pool(task),
			bee.WithPoolMaxCapacity(5),
			bee.WithPoolMaxWorkers(10),
			bee.WithPoolWorkerIdleDuration(time.Millisecond),
			bee.WithRetryEvery(6*time.Second),
			bee.WithStreamBufferSize(100),
			bee.WithStreamFlushEvery(time.Millisecond),
		)
	})
}

func TestNewPool(t *testing.T) {
	task := bee.New(func(ctx context.Context, i int) (int, error) {
		return i * 2, nil
	})

	t.Run("creation with no options", func(t *testing.T) {
		bee.Pool(task)
	})

	t.Run("creation with options", func(t *testing.T) {
		bee.Pool(
			task,
			bee.WithPoolMaxCapacity(5),
			bee.WithPoolMaxWorkers(10),
			bee.WithPoolWorkerIdleDuration(time.Millisecond),
			bee.WithRetryEvery(6*time.Second),
		)
	})
}

func TestNewWorker(t *testing.T) {
	t.Run("creation with no options", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})
		bee.Retry(task)
	})

	t.Run("creation with options", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})
		bee.Retry(task,
			bee.WithRetryMaxAttempts(3),
			bee.WithRetryEvery(time.Second),
		)
	})
}

func TestStart(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			id := bee.TaskID(ctx)
			if id != "" {
				t.Errorf("Expected empty id, got %s", id)
			}
			return i * 2, nil
		})
		worker := bee.Retry(task)

		ctx := context.Background()
		result, err := bee.Start(ctx, worker, 5)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected result 10, got %d", result)
		}
	})

	t.Run("execution with error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})
		worker := bee.Retry(task)

		ctx := context.Background()
		_, err := bee.Start(ctx, worker, 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("execution with context cancellation", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.Retry(task)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.Start(ctx, worker, 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}

func TestAdvancedStart(t *testing.T) {
	t.Run("successful execution with ID", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if info != "test-worker" {
				t.Errorf("Expected worker ID 'test-worker', got %s", info)
			}
			return i * 2, nil
		})
		worker := bee.Retry(task)

		ctx := context.Background()
		result, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker"), worker, 5)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected result 10, got %d", result)
		}
	})

	t.Run("successful worker pool", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if info != "test-worker-pool#0" {
				t.Errorf("Expected worker ID 'test-worker-pool#0', got %s", info)
			}
			return i * 2, nil
		})
		task = bee.Retry(task)
		ctx := context.Background()
		pool := bee.Pool(task)
		results, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-pool"), pool, []int{5})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if results[0] != 10 {
			t.Errorf("Expected result 10, got %d", results[0])
		}
	})

	t.Run("compose two workers", func(t *testing.T) {
		double := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if info != "test-worker-pool#0" {
				t.Errorf("Expected worker ID 'test-worker-pool#0', got %s", info)
			}
			return i * 2, nil
		})
		print := bee.New(func(ctx context.Context, i int) (string, error) {
			info := bee.TaskID(ctx)
			if info != "test-worker-pool#0" {
				t.Errorf("Expected worker ID 'test-worker-pool#0', got %s", info)
			}
			return strconv.Itoa(i), nil
		})

		worker := bee.Compose(
			bee.Retry(double),
			bee.Retry(print),
		)

		ctx := context.Background()
		pool := bee.Pool(worker)
		results, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-pool"), pool, []int{5})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if results[0] != "10" {
			t.Errorf("Expected result 10, got %s", results[0])
		}
	})

	t.Run("compose two workers but first fails", func(t *testing.T) {
		double := bee.New(func(ctx context.Context, i int) (int, error) {
			return 0, errors.New("fail")
		})
		print := bee.New(func(ctx context.Context, i int) (string, error) {
			return strconv.Itoa(i), nil
		})

		worker := bee.Compose(
			bee.Retry(double),
			bee.Retry(print),
		)

		ctx := context.Background()
		pool := bee.Pool(worker)
		_, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-pool"), pool, []int{5})
		if err == nil {
			t.Errorf("Expected error")
		}
	})

	t.Run("successful pool stream", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info)
			}
			return i * 2, nil
		})
		task = bee.Retry(task)
		ctx := context.Background()
		stream := bee.Stream(bee.Pool(task))

		ints := make(chan int, 2)
		ints <- 5
		ints <- 7
		close(ints)

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Out():
				if !next {
					done = true
					continue
				}
				results = append(results, result)
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if errors.Join(errs...) != nil {
			t.Errorf("Expected no errors, got %v", errors.Join(errs...))
		}

		if results[0] != 10 {
			t.Errorf("Expected result 10, got %d", results[0])
		}
	})

	t.Run("successful pool stream with zero duration flush", func(t *testing.T) {
		task := bee.Retry(bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info)
			}
			return i * 2, nil
		}))

		ctx := context.Background()
		stream := bee.Stream(
			bee.Pool(task),
			bee.WithStreamFlushEvery(0),
		)

		ints := make(chan int, 2)
		go func() {
			for value := range 1000 {
				ints <- value
			}
			close(ints)
		}()

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Out():
				if !next {
					done = true
					continue
				}
				results = append(results, result)
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if errors.Join(errs...) != nil {
			t.Errorf("Expected no errors, got %v", errors.Join(errs...))
		}

		if len(results) == 0 {
			t.Errorf("Expected results")
		}
	})

	t.Run("successful pool stream with zero buffer size", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream', got %s", info)
			}
			return i * 2, nil
		})
		ctx := context.Background()
		stream := bee.Stream(
			bee.Pool(task),
			bee.WithStreamBufferSize(0),
		)
		ints := make(chan int, 2)
		go func() {
			for value := range 1000 {
				ints <- value
			}
			close(ints)
		}()

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Out():
				if !next {
					done = true
					continue
				}
				results = append(results, result)
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if errors.Join(errs...) != nil {
			t.Errorf("Expected no errors, got %v", errors.Join(errs...))
		}

		if len(results) == 0 {
			t.Errorf("Expected results")
		}
	})

	t.Run("empty pool stream", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info)
			}
			return i * 2, nil
		})
		ctx := context.Background()
		stream := bee.Stream(bee.Pool(task), bee.WithStreamFlushEvery(time.Microsecond))
		ints := make(chan int, 2)

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		time.Sleep(2 * time.Millisecond)
		close(ints)

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Out():
				if !next {
					done = true
					continue
				}
				results = append(results, result)
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if err := errors.Join(errs...); err != nil {
			t.Errorf("Expected no errors, got %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected no results, got %d", results[0])
		}
	})

	t.Run("cancelled pool stream", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info)
			}
			return i * 2, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		stream := bee.Stream(bee.Pool(task))
		ints := make(chan int, 2)

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		cancel()
		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Out():
				if !next {
					done = true
					continue
				}
				results = append(results, result)
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if len(errs) != 0 {
			t.Errorf("Expected no errors, got %d", len(errs))
		}

		if len(results) != 0 {
			t.Errorf("Expected no results, got %d", results[0])
		}
	})

	t.Run("failed pool stream", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			info := bee.TaskID(ctx)
			if !strings.Contains(info, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info)
			}
			return i * 2, errors.New("test error")
		})
		ctx := context.Background()
		stream := bee.Stream(bee.Pool(task))
		ints := make(chan int, 2)
		ints <- 5
		ints <- 7
		close(ints)

		sink, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker-stream"), stream, ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case _, next := <-sink.Out():
				if !next {
					done = true
				}
			case err := <-sink.Err():
				errs = append(errs, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for results and errors")
			}
		}

		if errors.Join(errs...).Error() == "" {
			t.Errorf("Expected error 'test error', got empty")
		}
	})

	t.Run("execution with error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})
		worker := bee.Retry(task)

		ctx := context.Background()
		_, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker"), worker, 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("execution with context cancellation", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.Retry(task)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker"), worker, 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})

	t.Run("pool execution with context cancellation", func(t *testing.T) {
		task := bee.New(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.Pool(task)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.Start(bee.ContextWithTaskID(ctx, "test-worker"), worker, []int{5})
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}
