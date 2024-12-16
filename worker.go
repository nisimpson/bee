package bee

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Task represents a unit of work that can be executed by a [Worker].
// The generic parameters In and Out represent the input and output types of the task.
type Task[In any, Out any] interface {
	// Do is invoked by a [Worker] and should return an output or error if the task fails.
	Do(context.Context, In) (Out, error)
}

// taskFunc is a function type that implements the Task interface.
type taskFunc[In any, Out any] func(context.Context, In) (Out, error)

// NewTask provides a convenient way to create a [Task] from a simple function that matches
// the required signature.
func NewTask[In any, Out any](f func(context.Context, In) (Out, error)) Task[In, Out] {
	return taskFunc[In, Out](f)
}

// Do implements the [Task] interface by executing the wrapped function.
func (f taskFunc[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

// Worker represents an entity capable of executing a [Task] with specific input and output types.
type Worker[In any, Out any] interface {
	// execute processes the input and returns either the output or an error.
	execute(ctx context.Context, in In) (Out, error)
}

// Start executes the [Worker] with the provided input.
func Start[In any, Out any](ctx context.Context, w Worker[In, Out], in In) (Out, error) {
	return StartWithID(ctx, w, strconv.Itoa(rand.Int()), in)
}

// StartWithID executes the [Worker] with the provided input and a specific worker ID.
// The worker's ID is added to the context and can be retrieved using [WorkerInfo].
func StartWithID[In any, Out any](ctx context.Context, w Worker[In, Out], id string, in In) (Out, error) {
	ctx = contextWithWorkerInfo(ctx, workerInfo{WorkerID: id})
	return w.execute(ctx, in)
}

// RetryBackoff defines the interface for implementing retry backoff strategies.
type RetryBackoff interface {
	// NextRetry calculates the duration to wait before the next retry attempt
	// based on the previous retry duration.
	NextRetry(prev time.Duration) time.Duration
}

// RetryWorker wraps a [Task] with retry functionality.
// It will attempt to execute the task multiple times based on the configured [RetryOptions].
type RetryWorker[In any, Out any] struct {
	task    Task[In, Out] // task is the underlying task to execute with retries
	options RetryOptions  // options configure the retry behavior
}

// NewWorker creates a new [RetryWorker] with the specified options.
// The provided [Task] will be executed with retry behavior based on the configuration.
// [Options] can be provided to customize retry attempts and [RetryBackoff] strategy.
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
		options: options.RetryOptions,
	}
}

// execute implements the [Worker] interface by running the [Task] with retry logic.
// It will retry failed attempts according to the configured RetryOptions.
func (w RetryWorker[In, Out]) execute(ctx context.Context, in In) (Out, error) {
	var (
		out     Out
		err     error
		backoff = w.options.retryBackoff
		delay   = backoff.NextRetry(0)
	)

	for i := 0; i < w.options.maxAttempts; i++ {
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

// Pool represents a worker pool that can execute multiple tasks concurrently.
type Pool[In any, Out any] struct {
	worker  Worker[In, Out] // worker is the underlying worker instance used to process tasks
	options PoolOptions     // options contains the configuration for the worker pool
}

// newPool creates a new worker pool with the specified worker and options.
// The pool will use the provided worker to process tasks concurrently.
func newPool[In any, Out any](w Worker[In, Out], opts ...func(*Options)) *Pool[In, Out] {
	options := Options{
		PoolOptions: PoolOptions{}}

	options.apply(opts...)
	return &Pool[In, Out]{
		worker:  w,
		options: options.PoolOptions,
	}
}

// NewPool creates a new worker pool for concurrent execution of the input [Task].
// Add both retry and pool [Options] to configure the underlying [Worker] and [Pool] respectively.
func NewPool[In any, Out any](t Task[In, Out], opts ...func(*Options)) *Pool[In, Out] {
	w := NewWorker(t, opts...)
	return newPool(w, opts...)
}

// Pool creates a new worker pool using this [RetryWorker] as the [Worker] implementation.
// The provided options configure the pool's behavior.
func (w RetryWorker[In, Out]) Pool(opts ...func(*Options)) *Pool[In, Out] {
	return newPool(w, opts...)
}

// execute processes a batch of inputs concurrently using the pool of workers.
// It returns the outputs in the same order as the inputs, or an error if the execution fails.
func (p Pool[In, Out]) execute(ctx context.Context, in []In) ([]Out, error) {
	var (
		count    = len(in)
		queue    = make(chan In, count)
		resultQ  = make(chan Out, count)
		errQ     = make(chan error, count)
		wg       = sync.WaitGroup{}
		progress = newTaskProgress(float64(count))
		capacity = min(p.options.maxCapacity, count)
	)

	// if user does not specify capacity, assume creation of as many workers
	// as there are jobs.
	if capacity < 1 {
		capacity = count
	}

	prefix := WorkerInfo(ctx).WorkerID

	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func(workerID int, wg *sync.WaitGroup) {
			defer wg.Done()
			p.start(
				ctx,
				workerID,
				prefix,
				queue, resultQ, errQ,
				p.worker,
				progress,
			)
		}(i, &wg)
	}

	// enqueue work
	for _, in := range in {
		queue <- in
	}

	// close queue to signal workers to stop
	close(queue)

	// wait for workers to finish
	wg.Wait()
	close(resultQ)
	close(errQ)

	// collect results
	var (
		results = make([]Out, 0, count)
		errs    = make([]error, 0, count)
	)

	for result := range resultQ {
		results = append(results, result)
	}

	for err := range errQ {
		errs = append(errs, err)
	}

	return results, errors.Join(errs...)
}

// start launches worker goroutines to process tasks from the input queue.
// It manages the worker lifecycle and coordinates task execution.
func (p Pool[In, Out]) start(ctx context.Context,
	idx int,
	prefix string,
	jobs <-chan In,
	results chan<- Out,
	errs chan<- error,
	worker Worker[In, Out],
	progress *taskProgress,
) {
	workerID := fmt.Sprintf("%s#%s", prefix, strconv.Itoa(idx))
	ctx = contextWithProgress(ctx, progress)

	for job := range jobs {
		select {
		case <-ctx.Done():
			errs <- ctx.Err()
			return
		default:
			out, err := StartWithID(ctx, worker, workerID, job)
			progress.increment()
			time.Sleep(p.options.workerIdleDuration)
			results <- out
			errs <- err
		}
	}
}
