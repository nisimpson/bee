package bee_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nisimpson/bee"
)

func TestNewStream(t *testing.T) {
	task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
		return i * 2, nil
	})

	t.Run("creation with no options", func(t *testing.T) {
		stream := bee.NewStream(task)
		if stream == nil {
			t.Error("Expected non-nil stream")
		}
	})

	t.Run("creation with options", func(t *testing.T) {
		stream := bee.NewStream(
			task,
			bee.WithPoolMaxCapacity(5),
			bee.WithPoolMaxWorkers(10),
			bee.WithPoolWorkerIdleDuration(time.Millisecond),
			bee.WithRetryEvery(6*time.Second),
			bee.WithStreamBufferSize(100),
			bee.WithStreamFlushEvery(time.Millisecond),
		)
		if stream == nil {
			t.Error("Expected non-nil stream")
		}
	})
}

func TestNewPool(t *testing.T) {
	task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
		return i * 2, nil
	})

	t.Run("creation with no options", func(t *testing.T) {
		pool := bee.NewPool(task)
		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})

	t.Run("creation with options", func(t *testing.T) {
		pool := bee.NewPool(
			task,
			bee.WithPoolMaxCapacity(5),
			bee.WithPoolMaxWorkers(10),
			bee.WithPoolWorkerIdleDuration(time.Millisecond),
			bee.WithRetryEvery(6*time.Second),
		)
		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})
}

func TestNewWorker(t *testing.T) {
	t.Run("creation with no options", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("creation with options", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})
		worker := bee.NewWorker(task,
			bee.WithRetryMaxAttempts(3),
			bee.WithRetryEvery(time.Second),
		)
		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})
}

func TestStart(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})
		worker := bee.NewWorker(task)

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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})
		worker := bee.NewWorker(task)

		ctx := context.Background()
		_, err := bee.Start(ctx, worker, 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("execution with context cancellation", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.NewWorker(task)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.Start(ctx, worker, 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}

func TestStartWithID(t *testing.T) {
	t.Run("successful execution with ID", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if info.WorkerID != "test-worker" {
				t.Errorf("Expected worker ID 'test-worker', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)

		ctx := context.Background()
		result, err := bee.StartWithID(ctx, worker, "test-worker", 5)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected result 10, got %d", result)
		}
	})

	t.Run("successful worker pool", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if info.WorkerID != "test-worker-pool#0" {
				t.Errorf("Expected worker ID 'test-worker-pool#0', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		pool := worker.Pool()
		if pool == nil {
			t.Error("Expected non-nil pool")
		}
		results, err := bee.StartWithID(ctx, pool, "test-worker-pool", []int{5})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if results[0] != 10 {
			t.Errorf("Expected result 10, got %d", results[0])
		}
	})

	t.Run("successful pool stream", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		stream := worker.Pool().Stream()
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)
		ints <- 5
		ints <- 7
		close(ints)

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		stream := worker.Pool().Stream(
			bee.WithStreamFlushEvery(0),
		)
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)
		go func() {
			for value := range 1000 {
				ints <- value
			}
			close(ints)
		}()

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		stream := worker.Pool().Stream(
			bee.WithStreamBufferSize(0),
		)
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)
		go func() {
			for value := range 1000 {
				ints <- value
			}
			close(ints)
		}()

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		results := make([]int, 0)
		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case <-ctx.Done():
				done = true
			case result, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		stream := worker.Pool().Stream(bee.WithStreamFlushEvery(time.Microsecond))
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
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
			case result, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info.WorkerID)
			}
			return i * 2, nil
		})
		worker := bee.NewWorker(task)
		ctx, cancel := context.WithCancel(context.Background())
		stream := worker.Pool().Stream()
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
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
			case result, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			info := bee.WorkerInfo(ctx)
			if !strings.Contains(info.WorkerID, "test-worker-stream") {
				t.Errorf("Expected worker ID 'test-worker-stream#0', got %s", info.WorkerID)
			}
			return i * 2, errors.New("test error")
		})
		worker := bee.NewWorker(task)
		ctx := context.Background()
		stream := worker.Pool().Stream()
		if stream == nil {
			t.Error("Expected non-nil stream")
		}

		ints := make(chan int, 2)
		ints <- 5
		ints <- 7
		close(ints)

		sink, err := bee.StartWithID(ctx, stream, "test-worker-stream", ints)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		errs := make([]error, 0)

		for done := false; !done; {
			select {
			case _, next := <-sink.Chan():
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
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})
		worker := bee.NewWorker(task)

		ctx := context.Background()
		_, err := bee.StartWithID(ctx, worker, "test-worker", 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("execution with context cancellation", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.NewWorker(task)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.StartWithID(ctx, worker, "test-worker", 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})

	t.Run("pool execution with context cancellation", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})
		worker := bee.NewWorker(task).Pool()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := bee.StartWithID(ctx, worker, "test-worker", []int{5})
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}
