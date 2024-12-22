package bee_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nisimpson/bee"
)

func TestNewTask(t *testing.T) {
	t.Run("successful task execution", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		})

		ctx := context.Background()
		result, err := task(ctx, 5)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected result 10, got %d", result)
		}
	})

	t.Run("task execution with error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			return 0, expectedErr
		})

		ctx := context.Background()
		_, err := task(ctx, 5)
		if err != expectedErr {
			t.Errorf("Expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("task with context cancellation", func(t *testing.T) {
		task := bee.NewTask(func(ctx context.Context, i int) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := task(ctx, 5)
		if err == nil {
			t.Error("Expected context cancellation error")
		}
	})
}
