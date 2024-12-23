package bee

import (
	"context"
	"testing"
)

func TestWorkerInfo(t *testing.T) {
	// Test case 1: Test with context containing worker info
	t.Run("context with worker info", func(t *testing.T) {
		ctx := context.Background()
		ctx = ContextWithTaskID(ctx, "test-worker")

		gotInfo := TaskID(ctx)
		if gotInfo != "test-worker" {
			t.Errorf("WorkerInfo() = %v, want %v", gotInfo, "test-worker")
		}
	})

	// Test case 2: Test with empty context
	t.Run("empty context", func(t *testing.T) {
		ctx := context.Background()
		info := TaskID(ctx)
		if info != "" {
			t.Errorf("Expected empty worker info, got ID = %s", info)
		}
	})
}

func TestTaskProgress(t *testing.T) {
	// Test case 1: Test with context containing progress
	t.Run("context with progress", func(t *testing.T) {
		ctx := context.Background()
		progress := newTaskProgress(100)
		ctx = contextWithProgress(ctx, progress)

		gotProgress := TaskProgress(ctx)
		if gotProgress != 0 {
			t.Errorf("TaskProgress() = %d, want 0", gotProgress)
		}

		// Increment progress and check
		progress.increment()
		gotProgress = TaskProgress(ctx)
		if gotProgress != 1 {
			t.Errorf("TaskProgress() after increment = %d, want 1", gotProgress)
		}
	})

	// Test case 2: Test with empty context
	t.Run("empty context", func(t *testing.T) {
		ctx := context.Background()
		progress := TaskProgress(ctx)
		if progress != 0 {
			t.Errorf("Expected 0 progress for empty context, got %d", progress)
		}
	})
}
