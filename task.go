// Package bee provides a flexible and type-safe worker pool implementation with retry capabilities.
package bee

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrMaxAttempts is returned when a [Task] has been retried the maximum number of times without success.
	ErrMaxAttempts = errors.New("maximum attempts reached")
)

// Task is a generic interface that represents a unit of work that takes an input of type In
// and produces an output of type Out or an error of type Err.
type Task[In, Out, Err any] interface {
	execute(ctx context.Context, in In) (Out, Err)
}

// Execute runs a [Task] with the given context and input, returning the output and any error.
// This is the primary entry point for executing individual tasks.
func Execute[In, Out, Err any](ctx context.Context, task Task[In, Out, Err], in In) (Out, Err) {
	if _, ok := getContext(ctx); !ok {
		ctx = setContext(ctx, &taskContext{})
	}
	return task.execute(ctx, in)
}

// taskFunc is a function type that implements the Task interface.
type taskFunc[In, Out, Err any] func(ctx context.Context, in In) (Out, Err)

// execute implements the Task interface for taskFunc.
func (tf taskFunc[In, Out, Err]) execute(ctx context.Context, in In) (Out, Err) {
	return tf(ctx, in)
}

// NewTask creates a new [Task] from a function that processes input of type In and returns
// output of type Out and an error.
func NewTask[In, Out any](fn func(ctx context.Context, in In) (Out, error)) Task[In, Out, error] {
	return taskFunc[In, Out, error](fn)
}

// Compose creates a new task that chains multiple tasks together sequentially.
// It takes an initial task and a variadic number of additional tasks, where
// each task processes the same type T. The output of each task becomes
// the input to the next task in the sequence.
//
// Returns a single [Task] that processes all tasks in order.
func Compose[T any](task Task[T, T, error], tasks ...Task[T, T, error]) Task[T, T, error] {
	t := task
	for _, tt := range tasks {
		t = Compose2(t, tt)
	}
	return t
}

// Compose2 creates a new [Task] that chains two tasks together, where the output of the first task
// becomes the input to the second task.
func Compose2[T, U, V any](left Task[T, U, error], right Task[U, V, error]) Task[T, V, error] {
	return taskFunc[T, V, error](func(ctx context.Context, t T) (V, error) {
		var (
			u   U
			v   V
			err error
		)
		u, err = Execute(ctx, left, t)
		if err != nil {
			return v, err
		}
		v, err = Execute(ctx, right, u)
		return v, err
	})
}

// retryTask is a Task implementation that adds retry capabilities to an underlying task.
type retryTask[In, Out any] struct {
	Task[In, Out, error]         // The underlying task to execute
	options              Options // Configuration options for the worker
}

// Retry creates a [Task] that wraps an underlying task with retry capabilities.
// RetryTask will retry the task according to the provided options.
func Retry[In, Out any](task Task[In, Out, error], opts ...func(*Options)) Task[In, Out, error] {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	return retryTask[In, Out]{
		Task:    task,
		options: options,
	}
}

// execute implements the Task interface for worker, adding retry logic to the underlying task.
func (w retryTask[In, Out]) execute(ctx context.Context, in In) (out Out, err error) {
	var (
		attempt int
		delay   time.Duration
	)

	for attempt < w.options.attempts {
		out, err = w.Task.execute(ctx, in)
		if err == nil {
			return out, nil
		}
		delay = w.options.retryer.NextRetry(delay)
		time.Sleep(delay)
		attempt++
	}

	return out, fmt.Errorf("%w: %w", ErrMaxAttempts, err)
}

// stream is a [Task] implementation that processes inputs concurrently using a worker pool.
type stream[In, Out any] struct {
	Task[In, Out, error]         // The task to execute for each input
	options              Options // Configuration options for the stream
}

// TaskStream is a type alias for a [Task] that processes a channel of inputs and produces a channel of outputs.
type TaskStream[In, Out any] = Task[<-chan In, chan Out, <-chan error]

// Pipe connects multiple task streams of the same type in sequence. It takes an initial stream and
// a variadic number of additional streams, connecting them together using [Pipe2]. The output of each
// stream becomes the input to the next stream in the sequence. Returns a single [TaskStream] that
// processes all streams in order.
func Pipe[U any](stream TaskStream[U, U], streams ...TaskStream[U, U]) TaskStream[U, U] {
	pipe := stream
	for _, s := range streams {
		pipe = Pipe2(pipe, s)
	}
	return pipe
}

// Pipe2 connects two streams together, where the output channel of the first [TaskStream]
// becomes the input channel to the second.
func Pipe2[T, U, V any](left TaskStream[T, U], right TaskStream[U, V]) TaskStream[T, V] {
	return taskFunc[<-chan T, chan V, <-chan error](func(ctx context.Context, in <-chan T) (chan V, <-chan error) {
		var (
			u, uerr = Execute(ctx, left, in)
			v, verr = Execute(ctx, right, u)
		)
		return v, mergeErrorChannel(ctx, uerr, verr)
	})
}

// mergeErrorChannel combines multiple error channels into a single error channel.
// It takes a context and a variadic number of input error channels as parameters.
// When all input channels are closed and processed, it closes the output channel.
// The context allows for cancellation of the merging operation.
func mergeErrorChannel(ctx context.Context, cs ...<-chan error) <-chan error {
	out := make(chan error)
	var wg sync.WaitGroup

	merge := func(ctx context.Context, wg *sync.WaitGroup, in <-chan error, out chan<- error) {
		defer wg.Done()
		for {
			select {
			case err, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- err:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	for _, c := range cs {
		wg.Add(1)
		go merge(ctx, &wg, c, out)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Stream creates a new [TaskStream] that processes inputs concurrently using a worker pool.
// The stream will execute the given task for each input using multiple workers.
func Stream[In any, Out any](task Task[In, Out, error], opts ...func(*Options)) Task[<-chan In, chan Out, <-chan error] {
	options := defaultOptions()

	for _, opt := range opts {
		opt(&options)
	}

	return stream[In, Out]{
		Task:    task,
		options: options,
	}
}

// execute implements the Task interface for stream, processing inputs concurrently using multiple workers.
func (s stream[In, Out]) execute(ctx context.Context, in <-chan In) (chan Out, <-chan error) {
	var (
		size       = max(len(in), s.options.capacity)
		out        = make(chan Out, size)
		errCh      = make(chan error, size)
		taskctx, _ = getContext(ctx)
		wg         = &sync.WaitGroup{}
	)

	taskctx.init(len(in))

	for i := 0; i < s.options.workers; i++ {
		wg.Add(1)
		go func(workerctx context.Context) {
			defer wg.Done()
			for v := range in {
				if workerctx.Err() != nil {
					return
				}
				o, e := s.Task.execute(workerctx, v)
				taskctx.increment()
				if e != nil {
					errCh <- fmt.Errorf("task error: %w", e)
					continue
				}
				out <- o
			}
		}(setWorkerID(ctx, i))
	}

	go func() {
		wg.Wait()    // wait for workers to complete
		close(out)   // close output channel
		close(errCh) // close the error channel so stream error can consume
	}()

	return out, errCh
}

// Retryer defines an interface for determining the delay between retry attempts.
type Retryer interface {
	// NextRetry calculates the next retry delay based on the previous delay.
	NextRetry(prev time.Duration) time.Duration
}

// retryLinear implements a constant backoff retry strategy.
type retryLinear struct {
	period time.Duration // Fixed time period between retries
}

// NextRetry returns a constant delay for each retry attempt.
func (r retryLinear) NextRetry(prev time.Duration) time.Duration { return r.period }

// RetryLinear creates a [Retryer] that uses a constant delay between retry attempts.
func RetryLinear(period time.Duration) Retryer {
	return retryLinear{period: period}
}

// retryExponential implements an exponential backoff retry strategy.
type retryExponential struct {
	period time.Duration // Initial delay period
	max    time.Duration // Maximum delay period
}

// NextRetry doubles the previous delay for each retry attempt, up to a maximum value.
func (r retryExponential) NextRetry(prev time.Duration) time.Duration {
	if prev == 0 {
		return r.period
	}
	if prev >= r.max {
		return r.max
	}
	return prev * 2
}

// RetryExponential creates a [Retryer] that uses exponential backoff between retry attempts.
// The delay starts at the given period and doubles with each attempt, up to the maximum value.
func RetryExponential(period, max time.Duration) Retryer {
	period = min(period, max)
	return retryExponential{period: period, max: max}
}
