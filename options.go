package bee

import (
	"time"
)

// Options combine retry and pool configuration options for a [Task].
type Options struct {
	RetryOptions  // RetryOptions configures the retry behavior for failed tasks
	PoolOptions   // PoolOptions configures the worker pool behavior
	StreamOptions // StreamOptions configures the stream behavior for the task stream.
}

// apply configures the Options struct using the provided option functions.
// Each function can modify one or more fields of the Options struct.
func (o *Options) apply(opts ...func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

// RetryOptions configures how tasks are retried when they fail.
type RetryOptions struct {
	maxAttempts  int          // maxAttempts is the maximum number of times to retry a failed task
	retryBackoff RetryBackoff // retryBackoff determines the delay between retry attempts
}

// WithRetryMaxAttempts configures the maximum number of [RetryWorker] attempts for failed tasks.
// A value of 0 means no retries will be attempted.
func WithRetryMaxAttempts(n int) func(*Options) {
	return func(o *Options) {
		o.maxAttempts = n
	}
}

// WithBackoff configures the backoff strategy used between retry attempts.
// The provided RetryBackoff implementation determines the delay between retries.
func WithBackoff(b RetryBackoff) func(*Options) {
	return func(o *Options) {
		o.retryBackoff = b
	}
}

// constantBackoff implements RetryBackoff with a fixed delay between retries.
type constantBackoff time.Duration

// NextRetry returns a constant duration regardless of the previous retry delay.
func (b constantBackoff) NextRetry(time.Duration) time.Duration {
	return time.Duration(b)
}

// WithRetryEvery configures a constant [RetryBackoff] strategy with the specified duration.
// Each [RetryWorker] attempt will wait for exactly the same duration.
func WithRetryEvery(d time.Duration) func(*Options) {
	return WithBackoff(constantBackoff(d))
}

// exponentialBackoff implements [RetryBackoff] with exponentially increasing delays.
type exponentialBackoff struct {
	startDuration time.Duration // startDuration is the initial delay duration
	maxDuration   time.Duration // maxDuration is the maximum delay duration
}

// WithRetryExponentially configures an exponential [RetryBackoff] strategy.
// The delay between [RetryWorker] retries starts at 'start'
// and doubles until reaching 'max'.
func WithRetryExponentially(start, max time.Duration) func(*Options) {
	return WithBackoff(exponentialBackoff{
		startDuration: start,
		maxDuration:   max,
	})
}

// NextRetry implements [RetryBackoff] by doubling the previous retry duration.
// The returned duration will not exceed the configured maximum duration.
func (b exponentialBackoff) NextRetry(d time.Duration) time.Duration {
	d = max(d, b.startDuration)
	if d < b.maxDuration {
		d = min(d*2, b.maxDuration)
	}
	return d
}

// PoolOptions configures the behavior of a worker pool. Configuring the
// maximum number of workers with a defined execution delay determines
// the overall throughput of a [Pool]. For example:
//
//	5 workers * 1s delay = 5 executions per second
type PoolOptions struct {
	maxCapacity        int           // maxCapacity is the maximum number of concurrent workers in the pool
	workerIdleDuration time.Duration // workerIdleDuration is the duration to wait after completing a task
}

// WithPoolMaxWorkers configures the maximum number of concurrent workers in a pool.
// This controls the level of parallelism when processing tasks.
//
// Deprecated: use [WithPoolMaxCapacity] instead.
func WithPoolMaxWorkers(n int) func(*Options) {
	return WithPoolMaxCapacity(n)
}

// WithPoolMaxWorkers configures the maximum number of concurrent [Task]
// instances in a [Pool]. This controls the level of parallelism when processing tasks.
// A pool with max capacity M and task count N will generate min(N, M) workers.
// A max capacity of zero or less will generate N workers.
func WithPoolMaxCapacity(n int) func(*Options) {
	return func(o *Options) {
		o.maxCapacity = n
	}
}

// WithPoolWorkerDelay configures the delay between task executions for each worker.
// This can help prevent overwhelming systems when processing many tasks.
//
// Deprecated: Use [WithPoolWorkerIdleDuration] instead.
func WithPoolWorkerDelay(d time.Duration) func(*Options) {
	return WithPoolWorkerIdleDuration(d)
}

// WithPoolWorkerIdleDuration configures the delay between task executions for each worker.
// This can help prevent overwhelming systems when processing many tasks.
func WithPoolWorkerIdleDuration(d time.Duration) func(*Options) {
	return func(o *Options) {
		o.workerIdleDuration = d
	}
}

type StreamOptions struct {
	bufferSize int           // bufferSize is the size of the buffer used to store tasks before processing.
	flushEvery time.Duration // flushEvery is the duration after which the stream will be flushed if not empty.
}

// WithStreamBufferSize configures the buffer size for stream processing.
// The buffer size determines the maximum number of results sent to the sink channel
// before blocking the processor.
func WithStreamBufferSize(n int) func(*Options) {
	return func(o *Options) {
		o.bufferSize = n
	}
}

// WithStreamFlushEvery sets the duration after which buffered items will be automatically processed.
// A zero duration will cause the buffer to flush immediately.
func WithStreamFlushEvery(d time.Duration) func(*Options) {
	return func(o *Options) {
		o.flushEvery = d
	}
}
