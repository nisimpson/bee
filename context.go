package bee

import (
	"context"
	"math"
	"sync"
)

// workerInfo contains identification information for a worker.
type workerInfo struct {
	// WorkerID is the unique identifier for the worker
	WorkerID string
}

// workerctx is a context key type for storing worker information.
type workerctx struct{}

// contextWithWorkerInfo adds worker identification information to the context.
func contextWithWorkerInfo(ctx context.Context, info workerInfo) context.Context {
	return context.WithValue(ctx, workerctx{}, info)
}

// WorkerInfo retrieves the worker identification information from the context.
// This allows tasks to know which worker is executing them.
func WorkerInfo(ctx context.Context) workerInfo {
	workctx := ctx.Value(workerctx{})
	if workctx == nil {
		return workerInfo{}
	}
	return workctx.(workerInfo)
}

// taskProgress tracks the progress of task execution.
type taskProgress struct {
	m     *sync.Mutex // m protects concurrent access to progress tracking
	count float64     // count represents the number of completed tasks
	total float64     // total represents the total number of tasks to process
}

// progressctx is a context key type for storing task progress information.
type progressctx struct{}

// TaskProgress retrieves the current progress percentage from the context.
// Returns an integer between 0 and 100 representing the completion percentage.
func TaskProgress(ctx context.Context) int {
	prog := ctx.Value(progressctx{})
	if prog == nil {
		return 0
	}
	return prog.(*taskProgress).progress()
}

// contextWithProgress adds progress tracking capability to the context.
// This allows tracking progress across multiple goroutines in a worker pool.
func contextWithProgress(ctx context.Context, prog *taskProgress) context.Context {
	return context.WithValue(ctx, progressctx{}, prog)
}

// newTaskProgress creates a new progress tracker for the specified number of tasks.
func newTaskProgress(total float64) *taskProgress {
	return &taskProgress{
		m:     &sync.Mutex{},
		count: 0,
		total: total,
	}
}

// increment advances the progress counter by one completed task.
// This method is thread-safe.
func (tc *taskProgress) increment() {
	tc.m.Lock()
	defer tc.m.Unlock()
	tc.count++
}

// progress calculates the current completion percentage.
// Returns an integer between 0 and 100.
func (tc taskProgress) progress() int {
	tc.m.Lock()
	defer tc.m.Unlock()
	pct := (tc.count / tc.total) * 100
	return int(math.Floor(pct))
}
