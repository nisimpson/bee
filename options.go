package bee

import (
	"time"
)

type Options struct {
	RetryOptions
	PoolOptions
}

func (o *Options) apply(opts ...func(*Options)) {
	for _, opt := range opts {
		opt(o)
	}
}

// RetryOptions modify and configure [RetryWorker] instances.
type RetryOptions struct {
	maxAttempts  int
	retryBackoff RetryBackoff
}

func WithRetryMaxAttempts(n int) func(*Options) {
	return func(o *Options) {
		o.maxAttempts = n
	}
}

func WithBackoff(b RetryBackoff) func(*Options) {
	return func(o *Options) {
		o.retryBackoff = b
	}
}

type constantBackoff time.Duration

func (b constantBackoff) NextRetry(time.Duration) time.Duration {
	return time.Duration(b)
}

func WithRetryEvery(d time.Duration) func(*Options) {
	return WithBackoff(constantBackoff(d))
}

type exponentialBackoff struct {
	startDuration time.Duration
	maxDuration   time.Duration
}

func WithRetryExponentially(start, max time.Duration) func(*Options) {
	return WithBackoff(exponentialBackoff{
		startDuration: start,
		maxDuration:   max,
	})
}

func (b exponentialBackoff) NextRetry(d time.Duration) time.Duration {
	d = max(d, b.startDuration)
	if d < b.maxDuration {
		d = min(d*2, b.maxDuration)
	}
	return d
}
