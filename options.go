// Package bee provides a flexible and type-safe worker pool implementation with retry capabilities.
package bee

import "time"

// Options contains configuration settings for workers and streams.
type Options struct {
	retryer  Retryer // Strategy for determining retry delays
	attempts int     // Maximum number of retry attempts
	workers  int     // Number of concurrent workers in a stream
	capacity int     // Minimum capacity for output channels
}

// defaultOptions returns a new Options struct with default values.
// By default, tasks will be executed once with a linear retry strategy of 1 second,
// using a single worker and no minimum capacity.
func defaultOptions() Options {
	return Options{
		retryer:  RetryLinear(time.Second),
		attempts: 1,
		workers:  1,
		capacity: -1,
	}
}

// WithMaxAttempts configures the maximum number of retry attempts for a task.
// This determines how many times a task will be retried before giving up.
func WithMaxAttempts(attempts int) func(*Options) {
	return func(o *Options) {
		o.attempts = attempts
	}
}

// WithMaxWorkers configures the maximum number of concurrent workers in a stream.
// This determines how many tasks can be processed simultaneously.
func WithMaxWorkers(workers int) func(*Options) {
	return func(o *Options) {
		o.workers = workers
	}
}

// WithMinCapacity configures the minimum capacity for output channels in a stream.
// This can help prevent blocking when processing large numbers of tasks.
func WithMinCapacity(size int) func(*Options) {
	return func(o *Options) {
		o.capacity = size
	}
}

// WithRetry configures the retry strategy for a worker.
// This determines how long to wait between retry attempts.
func WithRetry(retryer Retryer) func(*Options) {
	return func(o *Options) {
		o.retryer = retryer
	}
}
