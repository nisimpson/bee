// Package bee provides a flexible and type-safe worker pool implementation with retry capabilities.
package bee

import (
	"context"
	"sync"
)

// taskContext tracks progress information for a task or group of tasks.
type taskContext struct {
	mu      sync.RWMutex
	total   int // Total number of tasks to process
	current int // Number of tasks completed so far
}

// init initializes the task context with the total number of tasks to process.
func (c *taskContext) init(total int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = 0
	c.total = total
}

// increment increases the count of completed tasks by one.
// If the current count exceeds the total, the total is updated to match.
func (c *taskContext) increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current++
	if c.current > c.total {
		c.total = c.current
	}
}

// Progress calculates the percentage of tasks completed, from 0 to 100.
func (c *taskContext) Progress() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.total == 0 {
		return 0
	}
	return float64(c.current) / float64(c.total) * 100
}

// contextKey is a unique key type for storing task context in a context.Context.
type contextKey struct{}

// setContext adds a taskContext to a context.Context and returns the new context.
func setContext(ctx context.Context, c *taskContext) context.Context {
	return context.WithValue(ctx, contextKey{}, c)
}

// getContext retrieves the taskContext from a context.Context if it exists.
// Returns the taskContext and a boolean indicating whether it was found.
func getContext(parent context.Context) (*taskContext, bool) {
	c, ok := parent.Value(contextKey{}).(*taskContext)
	return c, ok
}

// Progress returns the current progress percentage of tasks in the given context.
// If no task context is found, returns 0.
func Progress(ctx context.Context) float64 {
	if c, ok := getContext(ctx); ok {
		return c.Progress()
	}
	return 0
}

// workerIDContextKey is a unique key type for storing worker IDs in a context.Context.
type workerIDContextKey struct{}

// setWorkerID adds a worker ID to a context.Context and returns the new context.
func setWorkerID(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, workerIDContextKey{}, id)
}

// WorkerID retrieves the worker ID from a context.Context if it exists.
// Returns the worker ID or -1 if no worker ID is found.
func WorkerID(ctx context.Context) int {
	c, ok := ctx.Value(workerIDContextKey{}).(int)
	if !ok {
		return -1
	}
	return c
}
