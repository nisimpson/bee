package bee

import (
	"context"
	"time"
)

// Task performs work, generating an output from a given input.
type Task[In any, Out any] interface {
	// Do is invoked by a [Worker] and should return an output or error if the task fails.
	Do(context.Context, In) (Out, error)
}

// TaskFunc is a function that implements [Task].
type TaskFunc[In any, Out any] func(context.Context, In) (Out, error)

// Do implements [Task].
func (f TaskFunc[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

// Worker are like [Task], but
type Worker[In any, Out any] interface {
	execute(context.Context, In) (Out, error)
}

type RetryBackoff interface {
	NextRetry(prev time.Duration) time.Duration
}

type RetryWorker[In any, Out any] struct {
	task    Task[In, Out]
	options Options
}

// NewWorker creates a new [RetryWorker] with the specified options.
func NewWorker[In any, Out any](task Task[In, Out], opts ...func(*Options)) *RetryWorker[In, Out] {
	options := Options{
		RetryOptions: RetryOptions{
			maxAttempts:  3,
			retryBackoff: constantBackoff(0), // no backoff
		},
	}
	options.apply(opts...)
	return &RetryWorker[In, Out]{
		task:    task,
		options: options,
	}
}

// execute implements [Worker], executing the target task and retrying upon failure.
func (w RetryWorker[In, Out]) execute(ctx context.Context, in In) (Out, error) {
	var (
		out     Out
		err     error
		backoff RetryBackoff  = w.options.RetryOptions.retryBackoff
		delay   time.Duration = backoff.NextRetry(0)
	)

	for i := 0; i < w.options.RetryOptions.maxAttempts; i++ {
		out, err = w.task.Do(ctx, in)
		if err == nil {
			return out, nil
		}
		select {
		// worker is cancelled; return immediately
		case <-ctx.Done():
			return out, ctx.Err()
		// wait for next retry; compute new delay duration
		case <-time.After(delay):
			delay = backoff.NextRetry(delay)
		}
	}

	// worker failed, return error
	return out, err
}

type PoolOptions struct {
	maxWorkers     int
	delayAfterWork time.Duration
}

type Pool[In any, Out any] struct {
	worker Worker[In, Out]
}
