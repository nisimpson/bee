package bee

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Task is a procedure intended to perform a user-defined unit of work. Tasks
// can be executed via the [Start] or [StartWithID] functions.
type Task[I any, O any] interface {
	execute(context.Context, I) (O, error)
}

// Start executes the [Task] with the provided input.
func Start[In any, Out any](ctx context.Context, w Task[In, Out], in In) (Out, error) {
	return w.execute(ctx, in)
}

// TaskFunc is a function that implements [Task]. It also provides transformation methods
// to extend functionality.
type TaskFunc[I any, O any] func(context.Context, I) (O, error)

// New provides a convenient way to create a [Task] from a simple function that matches
// the required signature.
func New[I any, O any](f func(context.Context, I) (O, error)) Task[I, O] {
	return TaskFunc[I, O](f)
}

// execute executes the task.
func (f TaskFunc[I, O]) execute(ctx context.Context, in I) (O, error) {
	return f(ctx, in)
}

// Compose bundles two tasks {t, next}s, where t(T) = U and next(U) = V, and returns
// a new [Task] t2 where t2(T) = V.
func Compose[T any, U any, V any](t Task[T, U], next Task[U, V]) TaskFunc[T, V] {
	return func(ctx context.Context, in T) (V, error) {
		var (
			u   U
			v   V
			err error
		)
		u, err = t.execute(ctx, in)
		if err != nil {
			return v, err
		}
		return next.execute(ctx, u)
	}
}

// RetryBackoff defines the interface for implementing retry backoff strategies.
type RetryBackoff interface {
	// NextRetry calculates the duration to wait before the next retry attempt
	// based on the previous retry duration.
	NextRetry(prev time.Duration) time.Duration
}

type retryTask[I any, O any] struct {
	t       Task[I, O]
	options RetryOptions
}

// Retry creates a new [Task] that will be execute with retry behavior based on the configuration.
// [RetryOptions] can be provided to customize retry attempts and [RetryBackoff] strategy.
//
// By default, retry will only perform one attempt to execute the task.
func Retry[I any, O any](t Task[I, O], opts ...func(*Options)) Task[I, O] {
	o := Options{
		RetryOptions: RetryOptions{
			maxAttempts:  1,
			retryBackoff: constantBackoff(0),
		},
	}
	o.apply(opts...)
	return retryTask[I, O]{t: t, options: o.RetryOptions}
}

// execute implements the [Task] interface by running the underlying task with retry logic.
// It will retry failed attempts according to the configured RetryOptions.
func (r retryTask[I, O]) execute(ctx context.Context, in I) (O, error) {
	var (
		out     O
		err     error
		backoff = r.options.retryBackoff
		delay   = backoff.NextRetry(0)
	)

	for i := 0; i < r.options.maxAttempts; i++ {
		out, err = r.t.execute(ctx, in)
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

// taskPool represents a worker pool that can execute multiple tasks concurrently.
type taskPool[I any, O any] struct {
	Task[I, O]
	options PoolOptions
}

// Pool creates a [Task] pool with the specified worker and options.
// The pool uses goroutines to process multiple jobs concurrently.
func Pool[I any, O any](t Task[I, O], opts ...func(*Options)) Task[[]I, []O] {
	o := Options{}
	o.apply(opts...)
	return taskPool[I, O]{Task: t, options: o.PoolOptions}
}

// execute processes a batch of inputs concurrently using the pool of workers.
// It returns the outputs in the same order as the inputs, or an error if the execution fails.
func (p taskPool[I, O]) execute(ctx context.Context, in []I) ([]O, error) {
	var (
		count    = len(in)
		queue    = make(chan I, count)
		resultQ  = make(chan O, count)
		errQ     = make(chan error, count)
		wg       = sync.WaitGroup{}
		progress = newTaskProgress(uint64(count))
		capacity = min(p.options.maxCapacity, count)
	)

	// if user does not specify capacity, assume creation of as many workers
	// as there are jobs.
	if capacity < 1 {
		capacity = count
	}

	prefix := TaskID(ctx)

	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func(workerID int, wg *sync.WaitGroup) {
			defer wg.Done()
			p.start(
				ctx,
				workerID,
				prefix,
				queue, resultQ, errQ,
				p.Task,
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
		results = make([]O, 0, count)
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
func (p taskPool[I, O]) start(ctx context.Context,
	idx int,
	prefix string,
	jobs <-chan I,
	results chan<- O,
	errs chan<- error,
	worker Task[I, O],
	progress *taskProgress,
) {
	workerID := fmt.Sprintf("%s#%s", prefix, strconv.Itoa(idx))
	ctx = contextWithProgress(ctx, progress)
	ctx = ContextWithTaskID(ctx, workerID)

	for job := range jobs {
		select {
		case <-ctx.Done():
			errs <- ctx.Err()
			return
		default:
			out, err := Start(ctx, worker, job)
			progress.increment()
			time.Sleep(p.options.workerIdleDuration)
			results <- out
			errs <- err
		}
	}
}

type taskStream[I any, O any] struct {
	Task[[]I, []O]
	options StreamOptions
}

// Stream provides streaming capabilities for processing input values through a [Task] pool.
// It allows for continuous processing of inputs with buffering and automatic flushing.
//
// Stream processors are workers that expect a channel of type In as input, and will
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
func Stream[I any, O any](t Task[[]I, []O], opts ...func(*Options)) Task[chan I, Sink[O]] {
	o := Options{}
	o.apply(opts...)
	return taskStream[I, O]{Task: t, options: o.StreamOptions}
}

// execute processes items from the input channel using the configured pool.
// Returns a [Sink] for accessing results and any error that occurred during setup.
func (s taskStream[I, O]) execute(ctx context.Context, in chan I) (out Sink[O], err error) {
	sink := Sink[O]{
		outch: make(chan O, s.options.bufferSize),
		errch: make(chan error, s.options.bufferSize),
	}
	go s.collect(ctx, in, &sink)
	return sink, nil
}

// collect accumulates items from the source channel into batches for processing.
// Batches are processed when either the buffer is full or the flush timer expires.
func (s taskStream[I, O]) collect(ctx context.Context, source <-chan I, sink *Sink[O]) {
	jobs := make([]I, 0, s.options.bufferSize)
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
func (s taskStream[I, O]) flush(ctx context.Context, jobs []I, sink *Sink[O]) []I {
	if len(jobs) == 0 {
		return nil
	}

	results, err := s.Task.execute(ctx, jobs)
	sink.errch <- err
	for _, result := range results {
		sink.outch <- result
	}

	return nil
}

// Sink handles the output stream from a [TaskStream] processor.
// It provides separate channels for successful results and errors.
type Sink[O any] struct {
	outch chan O     // Channel for successful results
	errch chan error // Channel for errors encountered during processing
}

// Out returns a channel that provides access to successful task results.
// The channel is closed when processing is complete.
func (s Sink[O]) Out() <-chan O {
	return s.outch
}

// Err returns a channel that provides access to errors encountered during processing.
// The channel is closed when processing is complete.
func (s Sink[O]) Err() <-chan error {
	return s.errch
}

// close closes both the output and error channels of the [Sink].
// This should be called when the Sink is no longer needed.
func (s Sink[O]) close() {
	close(s.outch)
	close(s.errch)
}
