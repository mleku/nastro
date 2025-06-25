package ephemeral

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pippellia-btc/nastro"
	"github.com/pippellia-btc/nastro/utils"
)

// Run with `go test -race` to verify concurrency safety
func TestConcurrency(t *testing.T) {
	const duration = 1 * time.Minute
	const saveEvery = 1 * time.Millisecond
	const deleteEvery = 2 * time.Millisecond

	// capacity must be large enough to prevent saves from overwriting older non-nil events,
	// which would not increase the store.Size but would increase the counter.
	const capacity = int(duration / saveEvery)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	store := New(capacity)
	expectedSize := atomic.Int64{}
	errChan := make(chan error, 10)

	// Saver
	go func() {
		ticker := time.NewTicker(saveEvery)
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				event := utils.RandomEvent()
				event.ID = "0" // so every deletion is successful

				store.Save(ctx, event)
				expectedSize.Add(1)
			}
		}
	}()

	// Deleter
	go func() {
		ticker := time.NewTicker(deleteEvery)
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				store.Delete(ctx, "0")
				expectedSize.Add(-1)
			}
		}
	}()

	// Size-checker
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				store.mu.RLock()
				var size int64
				for _, event := range store.events {
					if event != nil {
						size++
					}
				}

				// compare while keeping the lock, to avoid in-between saves or deletions
				expected := expectedSize.Load()
				if size != expected {
					errChan <- fmt.Errorf("expected size %d, got %d", expected, size)
				}
				store.mu.RUnlock()
			}
		}
	}()

	select {
	case err := <-errChan:
		t.Fatal(err)

	case <-ctx.Done():
		// test passed
	}
}

func TestInterface(t *testing.T) {
	var _ nastro.Store = &Ephemeral{}
}
