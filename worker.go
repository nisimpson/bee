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

// Stream provides streaming capabilities for processing input values through a worker [Pool].
// It allows for continuous processing of inputs with buffering and automatic flushing.
type Stream[In any, Out any] struct {
	options Options       // Configuration options for the stream
	pool    Pool[In, Out] // The underlying worker pool used for processing
}

// newStream creates a new Stream instance with the given pool and options.
// The stream will use the pool for processing items in batches.
func newStream[In any, Out any](pool Pool[In, Out], opts ...func(*Options)) *Stream[In, Out] {
	options := Options{}
	options.bufferSize = 100
	options.flushAfter = time.Millisecond
	options.apply(opts...)

	return &Stream[In, Out]{
		options: options,
		pool:    pool,
	}
}

// NewStream creates a new [Stream] that processes items using the given [Task].
// The stream will automatically create a pool for concurrent processing.
func NewStream[In any, Out any](t Task[In, Out], opts ...func(*Options)) *Stream[In, Out] {
	return NewWorker(t, opts...).Pool(opts...).Stream(opts...)
}

// Stream creates a new [Stream] that uses this [Pool] for processing.
// The stream inherits the pool's concurrency settings while adding buffering capabilities.
func (p Pool[In, Out]) Stream(opts ...func(*Options)) *Stream[In, Out] {
	return newStream(p, opts...)
}

// execute processes items from the input channel using the configured pool.
// Returns a [Sink] for accessing results and any error that occurred during setup.
func (s Stream[In, Out]) execute(ctx context.Context, in chan In) (out Sink[Out], err error) {
	sink := Sink[Out]{
		outch: make(chan Out, s.options.bufferSize),
		errch: make(chan error, s.options.bufferSize),
	}
	go s.collect(ctx, in, &sink)
	return sink, nil
}

// collect accumulates items from the source channel into batches for processing.
// Batches are processed when either the buffer is full or the flush timer expires.
func (s Stream[In, Out]) collect(ctx context.Context, source chan In, sink *Sink[Out]) {
	jobs := make([]In, 0, s.options.bufferSize)
	for {
		select {
		case <-ctx.Done():
			sink.close()
			return
		case job, next := <-source:
			if !next {
				s.flush(ctx, jobs, sink)
				sink.close()
				return
			}
			jobs = append(jobs, job)
		case <-time.After(s.options.flushAfter):
			jobs = s.flush(ctx, jobs, sink)
		}
	}
}

// flush processes a batch of accumulated items using the worker pool.
// Results and errors are sent to the appropriate sink channels.
func (s Stream[In, Out]) flush(ctx context.Context, jobs []In, sink *Sink[Out]) []In {
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

// Sink handles the output stream from a [Stream] processor.
// It provides separate channels for successful results and errors.
type Sink[Out any] struct {
	outch chan Out   // Channel for successful results
	errch chan error // Channel for errors encountered during processing
}

// Chan returns a channel that provides access to successful task results.
// The channel is closed when processing is complete or an error occurs.
func (s Sink[Out]) Chan() <-chan Out {
	return s.outch
}

// Err returns a channel that provides access to errors encountered during processing.
// The channel is closed when processing is complete.
func (s Sink[Out]) Err() <-chan error {
	return s.errch
}

// Close closes both the output and error channels of the [Sink].
// This should be called when the Sink is no longer needed.
func (s Sink[Out]) close() {
	close(s.outch)
	close(s.errch)
}
