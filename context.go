package bee

import (
	"context"
	"sync/atomic"
)

// taskInfo contains identification information for a task.
type taskInfo struct {
	// TaskID is the unique identifier for the task
	TaskID string
}

// taskctx is a context key type for storing task information.
type taskctx struct{}

// ContextWithTaskID adds worker identification information to the context.
func ContextWithTaskID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, taskctx{}, taskInfo{TaskID: id})
}

// TaskID retrieves the worker identification information from the context.
// This allows tasks to know which worker is executing them.
func TaskID(ctx context.Context) string {
	t := ctx.Value(taskctx{})
	if t == nil {
		return ""
	}
	return t.(taskInfo).TaskID
}

// taskProgress tracks the progress of task execution.
type taskProgress struct {
	count *atomic.Uint64 // count represents the number of completed tasks
	total uint64         // total represents the total number of tasks to process
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
func newTaskProgress(total uint64) *taskProgress {
	return &taskProgress{
		total: total,
		count: &atomic.Uint64{},
	}
}

// increment advances the progress counter by one completed task.
// This method is thread-safe.
func (tc *taskProgress) increment() {
	tc.count.Add(1)
}

// progress calculates the current completion percentage.
// Returns an integer between 0 and 100.
func (tc taskProgress) progress() int {
	count := tc.count.Load()
	pct := (float64(count) / float64(tc.total)) * 100
	return int(pct)
}
