package bee

import (
	"context"
	"testing"
	"time"
)

func TestWithRetryMaxAttempts(t *testing.T) {
	t.Run("valid attempts", func(t *testing.T) {
		opts := &Options{}
		maxAttempts := 5
		option := WithRetryMaxAttempts(maxAttempts)
		option(opts)

		// Test implementation details through behavior in NewWorker
		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryMaxAttempts(maxAttempts))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("zero attempts", func(t *testing.T) {
		opts := &Options{}
		option := WithRetryMaxAttempts(0)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryMaxAttempts(0))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("negative attempts", func(t *testing.T) {
		opts := &Options{}
		option := WithRetryMaxAttempts(-1)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryMaxAttempts(-1))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})
}

func TestWithRetryEvery(t *testing.T) {
	t.Run("valid duration", func(t *testing.T) {
		opts := &Options{}
		duration := time.Second
		option := WithRetryEvery(duration)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryEvery(duration))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("zero duration", func(t *testing.T) {
		opts := &Options{}
		option := WithRetryEvery(0)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryEvery(0))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("negative duration", func(t *testing.T) {
		opts := &Options{}
		option := WithRetryEvery(-time.Second)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), WithRetryEvery(-time.Second))

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})
}

func TestWithRetryExponentially(t *testing.T) {
	t.Run("valid durations", func(t *testing.T) {
		start := time.Second
		max := time.Minute
		option := WithRetryExponentially(start, max)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), option)

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})

	t.Run("max less than start", func(t *testing.T) {
		start := time.Minute
		max := time.Second
		option := WithRetryExponentially(start, max)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}), option)

		if worker == nil {
			t.Error("Expected non-nil worker")
		}
	})
}

func TestWithPoolMaxWorkers(t *testing.T) {
	t.Run("positive workers", func(t *testing.T) {
		opts := &Options{}
		numWorkers := 5
		option := WithPoolMaxWorkers(numWorkers)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}))
		pool := worker.Pool(WithPoolMaxWorkers(numWorkers))

		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})

	t.Run("zero workers", func(t *testing.T) {
		opts := &Options{}
		option := WithPoolMaxWorkers(0)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}))
		pool := worker.Pool(WithPoolMaxWorkers(0))

		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})
}

func TestWithPoolWorkerDelay(t *testing.T) {
	t.Run("positive delay", func(t *testing.T) {
		opts := &Options{}
		delay := time.Second
		option := WithPoolWorkerDelay(delay)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}))
		pool := worker.Pool(WithPoolWorkerDelay(delay))

		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})

	t.Run("zero delay", func(t *testing.T) {
		opts := &Options{}
		option := WithPoolWorkerDelay(0)
		option(opts)

		worker := NewWorker(NewTask(func(ctx context.Context, i int) (int, error) {
			return i, nil
		}))
		pool := worker.Pool(WithPoolWorkerDelay(0))

		if pool == nil {
			t.Error("Expected non-nil pool")
		}
	})
}

func TestBackoffs(t *testing.T) {
	t.Run("exponential backoff", func(t *testing.T) {
		start := time.Second
		max := time.Minute
		backoff := exponentialBackoff{
			startDuration: start,
			maxDuration:   max,
		}

		delay := backoff.NextRetry(0)
		for i := 0; i < 100; i++ {
			nextDelay := backoff.NextRetry(delay)
			if nextDelay < delay {
				t.Errorf("Expected next delay to be greater than or equal to previous delay")
			}
			delay = nextDelay
		}
	})

	t.Run("constant backoff", func(t *testing.T) {
		delay := time.Second
		backoff := constantBackoff(delay)
		delay = backoff.NextRetry(0)

		for i := 0; i < 100; i++ {
			nextDelay := backoff.NextRetry(delay)
			if nextDelay != delay {
				t.Errorf("Expected next delay to be greater than or equal to previous delay")
			}
			delay = nextDelay
		}
	})
}
