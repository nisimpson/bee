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

// Doer represents a unit of work that can be executed by a [Task].
// The generic parameters In and Out represent the input and output types of the task.
type Doer[In any, Out any] interface {
	// Do is invoked by a [Worker] and should return an output or error if the task fails.
	Do(context.Context, In) (Out, error)
}

// Task represents an entity capable of executing a task with specific input and output types.
type Task[In any, Out any] interface {
	// execute processes the input and returns either the output or an error.
	execute(ctx context.Context, in In) (Out, error)
}

// execute allows the function to implement [Task].
func (f TaskFunc[In, Out]) execute(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

// TaskFunc is a function type that implements the [Task] interface.
type TaskFunc[In any, Out any] func(context.Context, In) (Out, error)

// NewTask provides a convenient way to create a [Task] from a simple function that matches
// the required signature.
func NewTask[In any, Out any](f func(context.Context, In) (Out, error)) TaskFunc[In, Out] {
	return TaskFunc[In, Out](f)
}

// Do wraps a [Doer] into an executable [Task].
func Do[In any, Out any](doer Doer[In, Out]) TaskFunc[In, Out] {
	return NewTask(doer.Do)
}

// Retry returns a new [TaskFunc] that wraps a [RetryWorker] with the target [Options].
func (f TaskFunc[In, Out]) Retry(opts ...func(*Options)) TaskFunc[In, Out] {
	w := NewRetryWorker(f, opts...)
	return func(ctx context.Context, i In) (Out, error) {
		return w.execute(ctx, i)
	}
}

// Pool creates a new [TaskPool] that uses this [TaskFunc] for processing.
func (f TaskFunc[In, Out]) Pool(opts ...func(*Options)) TaskPool[In, Out] {
	return NewPool(f, opts...)
}

// Stream creates a new [TaskStream] that uses this [TaskFunc] for processing.
// The stream inherits the pool's concurrency settings while adding buffering capabilities.
func (f TaskFunc[In, Out]) Stream(opts ...func(*Options)) TaskStream[In, Out] {
	return NewStream(f, opts...)
}

// Compose bundles two workers {w, next}s, where w(T) = U and next(U) = V, and returns
// a new [Task] w2 where w2(T) = V.
func Compose[T any, U any, V any](w Task[T, U], next Task[U, V]) TaskFunc[T, V] {
	return func(ctx context.Context, in T) (V, error) {
		var (
			u   U
			v   V
			err error
		)
		u, err = w.execute(ctx, in)
		if err != nil {
			return v, err
		}
		return next.execute(ctx, u)
	}
}

// Start executes the [Task] with the provided input.
func Start[In any, Out any](ctx context.Context, w Task[In, Out], in In) (Out, error) {
	return StartWithID(ctx, w, strconv.Itoa(rand.Int()), in)
}

// StartWithID executes the [Task] with the provided input and a specific worker ID.
// The worker's ID is added to the context and can be retrieved using [WorkerInfo].
func StartWithID[In any, Out any](ctx context.Context, w Task[In, Out], id string, in In) (Out, error) {
	ctx = contextWithWorkerInfo(ctx, workerInfo{WorkerID: id})
	return w.execute(ctx, in)
}

// RetryBackoff defines the interface for implementing retry backoff strategies.
type RetryBackoff interface {
	// NextRetry calculates the duration to wait before the next retry attempt
	// based on the previous retry duration.
	NextRetry(prev time.Duration) time.Duration
}

// RetryWorker is a [Task] that wraps a [Doer] with retry functionality.
// It will attempt to execute the task multiple times based on the configured [RetryOptions].
type RetryWorker[In any, Out any] struct {
	task    Task[In, Out] // task is the underlying task to execute with retries
	options RetryOptions  // options configure the retry behavior
}

// NewRetryWorker creates a new [RetryWorker] with the specified options.
// The provided [Doer] will be executed with retry behavior based on the configuration.
// [Options] can be provided to customize retry attempts and [RetryBackoff] strategy.
//
// By default, the retry [Task] will only perform one attempt to execute the task.
func NewRetryWorker[In any, Out any](task Task[In, Out], opts ...func(*Options)) RetryWorker[In, Out] {
	options := Options{
		RetryOptions: RetryOptions{
			maxAttempts:  1,
			retryBackoff: constantBackoff(0), // no backoff
		},
	}
	options.apply(opts...)
	return RetryWorker[In, Out]{
		task:    task,
		options: options.RetryOptions,
	}
}

// execute implements the [Task] interface by running the underlying task with retry logic.
// It will retry failed attempts according to the configured RetryOptions.
func (w RetryWorker[In, Out]) execute(ctx context.Context, in In) (Out, error) {
	var (
		out     Out
		err     error
		backoff = w.options.retryBackoff
		delay   = backoff.NextRetry(0)
	)

	for i := 0; i < w.options.maxAttempts; i++ {
		out, err = w.task.execute(ctx, in)
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

// TaskPool represents a worker pool that can execute multiple tasks concurrently.
type TaskPool[In any, Out any] struct {
	worker  Task[In, Out] // worker is the underlying task instance used to process jobs
	options PoolOptions   // options contains the configuration for the worker pool
}

// NewPool creates a new worker pool with the specified worker and options.
// The pool will use the provided worker to process jobs concurrently.
func NewPool[In any, Out any](t Task[In, Out], opts ...func(*Options)) TaskPool[In, Out] {
	options := Options{
		PoolOptions: PoolOptions{}}

	options.apply(opts...)
	return TaskPool[In, Out]{
		worker:  t,
		options: options.PoolOptions,
	}
}

// execute processes a batch of inputs concurrently using the pool of workers.
// It returns the outputs in the same order as the inputs, or an error if the execution fails.
func (p TaskPool[In, Out]) execute(ctx context.Context, in []In) ([]Out, error) {
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
func (p TaskPool[In, Out]) start(ctx context.Context,
	idx int,
	prefix string,
	jobs <-chan In,
	results chan<- Out,
	errs chan<- error,
	worker Task[In, Out],
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

// TaskStream provides streaming capabilities for processing input values through a worker [TaskPool].
// It allows for continuous processing of inputs with buffering and automatic flushing.
//
// TaskStream processors are workers that expect a channel of type In as input, and will
// immediately return an output [Sink] when executed via [Start] or [StartWithID]. Use the
// sink's output and error channels to retrieve the task results:
//
//	input := make(chan int)
//	sink, _ := bee.Start(ctx, stream, input)
//	results := make([]int, 0)
//	errs := make([]error, 0)
//
//	go func() {
//		input <- 1
//		close(input)
//	}()
//
//	for done := false; !done; {
//		select {
//		case <-ctx.Done():
//			done = true
//		case result, next := <-sink.Chan():
//			if !next {
//				done = true
//			} else {
//				results = append(results, result)
//			}
//		case err := <-sink.Err():
//			errs = append(errs, err)
//		case <-time.After(timeoutDuration):
//			log.Fatal("Timeout waiting for results and errors")
//		}
//	}
type TaskStream[In any, Out any] struct {
	options Options           // Configuration options for the stream
	pool    TaskPool[In, Out] // The underlying worker pool used for processing
}

// newStream creates a new Stream instance with the given pool and options.
// The stream will use the pool for processing items in batches.
func newStream[In any, Out any](pool TaskPool[In, Out], opts ...func(*Options)) TaskStream[In, Out] {
	options := Options{}
	options.apply(opts...)

	return TaskStream[In, Out]{
		options: options,
		pool:    pool,
	}
}

// NewStream creates a new [TaskStream] that processes items using the given [Doer].
// The stream will automatically create a pool for concurrent processing.
func NewStream[In any, Out any](t Task[In, Out], opts ...func(*Options)) TaskStream[In, Out] {
	pool := NewPool(t, opts...)
	return newStream(pool, opts...)
}

// execute processes items from the input channel using the configured pool.
// Returns a [Sink] for accessing results and any error that occurred during setup.
func (s TaskStream[In, Out]) execute(ctx context.Context, in chan In) (out Sink[Out], err error) {
	sink := Sink[Out]{
		outch: make(chan Out, s.options.bufferSize),
		errch: make(chan error, s.options.bufferSize),
	}
	go s.collect(ctx, in, &sink)
	return sink, nil
}

// collect accumulates items from the source channel into batches for processing.
// Batches are processed when either the buffer is full or the flush timer expires.
func (s TaskStream[In, Out]) collect(ctx context.Context, source <-chan In, sink *Sink[Out]) {
	jobs := make([]In, 0, s.options.bufferSize)
	defer sink.close()
	for {
		select {
		case <-ctx.Done():
			return
		case job, next := <-source:
			if !next {
				s.flush(ctx, jobs, sink)
				return
			}
			jobs = append(jobs, job)
		case <-time.After(s.options.flushEvery):
			jobs = s.flush(ctx, jobs, sink)
		}
	}
}

// flush processes a batch of accumulated items using the worker pool.
// Results and errors are sent to the appropriate sink channels.
func (s TaskStream[In, Out]) flush(ctx context.Context, jobs []In, sink *Sink[Out]) []In {
	if len(jobs) == 0 {
		return nil
	}

	results, err := s.pool.execute(ctx, jobs)
	sink.errch <- err
	for _, result := range results {
		sink.outch <- result
	}

	return nil
}

// Sink handles the output stream from a [TaskStream] processor.
// It provides separate channels for successful results and errors.
type Sink[Out any] struct {
	outch chan Out   // Channel for successful results
	errch chan error // Channel for errors encountered during processing
}

// Chan returns a channel that provides access to successful task results.
// The channel is closed when processing is complete.
func (s Sink[Out]) Chan() <-chan Out {
	return s.outch
}

// Err returns a channel that provides access to errors encountered during processing.
// The channel is closed when processing is complete.
func (s Sink[Out]) Err() <-chan error {
	return s.errch
}

// close closes both the output and error channels of the [Sink].
// This should be called when the Sink is no longer needed.
func (s Sink[Out]) close() {
	close(s.outch)
	close(s.errch)
}
