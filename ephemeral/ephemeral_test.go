package ephemeral

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
	"github.com/pippellia-btc/nastro/utils"
)

// Run with `go test -race` to verify concurrency safety
func TestConcurrency(t *testing.T) {
	const duration = 1 * time.Minute
	const saveEvery = 1 * time.Millisecond
	const deleteEvery = 2 * time.Millisecond

	// capacity must be large enough to prevent saves from overwriting older non-nil events,
	// which would NOT increase the store.Size but would increase the counter.
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

func TestReplace(t *testing.T) {
	tests := []struct {
		name  string
		setup func() *Store
		event *nostr.Event

		saved bool
		err   error
		size  int
	}{
		{
			name:  "regular event, error",
			setup: Empty,
			event: &nostr.Event{Kind: 1},
			saved: false,
			err:   nastro.ErrInvalidReplacement,
		},
		{
			name:  "ephemeral event, error",
			setup: Empty,
			event: &nostr.Event{Kind: 20001},
			saved: false,
			err:   nastro.ErrInvalidReplacement,
		},
		{
			name:  "empty, saved",
			setup: Empty,
			event: &nostr.Event{Kind: 3},
			saved: true,
			size:  1,
		},
		{
			name:  "one event, replaced",
			setup: OneEvent(3),
			event: &nostr.Event{Kind: 3, CreatedAt: 1},
			saved: true,
			size:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := test.setup()
			saved, err := store.Replace(context.Background(), test.event)

			if !errors.Is(err, test.err) {
				t.Fatalf("expected error %v, got %v", test.err, err)
			}

			if saved != test.saved {
				t.Fatalf("expected saved %v, got %v", test.saved, saved)
			}

			size := store.Size()
			if size != test.size {
				t.Fatalf("expected size %d, got %v", test.size, size)
			}
		})
	}
}

func TestInterface(t *testing.T) {
	var _ nastro.Store = &Store{}
}

func Empty() *Store { return New(100) }

func OneEvent(kind int) func() *Store {
	return func() *Store {
		store := New(100)
		store.Save(context.Background(), &nostr.Event{CreatedAt: 0, Kind: kind})
		return store
	}
}
