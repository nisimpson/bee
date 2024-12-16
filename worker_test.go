package bee_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nisimpson/bee"
)

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
