package ephemeral

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

// Ephemeral is an in-memory, thread-safe ring-buffer for storing Nostr events with a fixed memory footprint.
// Because of its efficiency and limited scope, it doesn't have query limits.
type Ephemeral struct {
	mu       sync.RWMutex
	events   []*nostr.Event
	write    int
	capacity int
}

// New creates an ephemeral store with the provided capacity.
func New(capacity int) *Ephemeral {
	return &Ephemeral{
		events:   make([]*nostr.Event, capacity),
		capacity: capacity,
	}
}

// Size returns the number of events currently stored.
func (e *Ephemeral) Size() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var size int
	for _, event := range e.events {
		if event != nil {
			size++
		}
	}
	return size
}

// Capacity returns the maximum number of events that can be stored.
func (e *Ephemeral) Capacity() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.capacity
}

// Resize the ephemeral store with the provided capacity.
func (e *Ephemeral) Resize(capacity int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.write = 0
	e.capacity = capacity
	events := make([]*nostr.Event, capacity)

	for _, event := range e.events {
		if event != nil {
			events[e.write] = event
			e.write++

			if e.write >= capacity {
				// reached capacity
				break
			}
		}
	}
	e.events = events
}

func (e *Ephemeral) Save(ctx context.Context, event *nostr.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.events[e.write] = event
	e.write = (e.write + 1) % e.capacity
	return nil
}

func (e *Ephemeral) Replace(ctx context.Context, event *nostr.Event) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, stored := range e.events {
		if stored == nil {
			continue
		}

		if isReplacementCandidate(event, stored) {
			if event.CreatedAt > stored.CreatedAt {
				e.events[i] = event
				return true, nil
			}
			return false, nil
		}
	}

	// no candidates found, save
	e.events[e.write] = event
	e.write = (e.write + 1) % e.capacity
	return true, nil
}

// isReplacementCandidate returns whether e1 and e2 are of the same category (replaceable, addressable), and same kind, author...
func isReplacementCandidate(e1, e2 *nostr.Event) bool {
	switch {
	case nostr.IsReplaceableKind(e1.Kind):
		return e1.Kind == e2.Kind && e1.PubKey == e2.PubKey

	case nostr.IsAddressableKind(e1.Kind):
		d1 := e1.Tags.GetD()
		d2 := e2.Tags.GetD()
		return e1.Kind == e2.Kind && e1.PubKey == e2.PubKey && d1 == d2

	default:
		return false
	}
}

func (e *Ephemeral) Delete(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pos := slices.IndexFunc(e.events, func(event *nostr.Event) bool { return event != nil && event.ID == id })
	if pos == -1 {
		return nil
	}

	e.events[pos] = nil
	return nil
}

func (e *Ephemeral) Query(ctx context.Context, filter *nostr.Filter) ([]nostr.Event, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	limit := e.capacity
	if filter.Limit > 0 {
		limit = min(filter.Limit, e.capacity)
	}

	events := make([]nostr.Event, 0, limit)
	for _, event := range e.events {
		if len(events) >= limit {
			break
		}

		if event != nil && filter.Matches(event) {
			events = append(events, *event)
		}
	}

	// sort events in descending order by their CreatedAt
	slices.SortFunc(events, func(e1, e2 nostr.Event) int { return cmp.Compare(e2.CreatedAt, e1.CreatedAt) })
	return events, nil
}

func (e *Ephemeral) Count(ctx context.Context, filter *nostr.Filter) (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	limit := e.capacity
	if filter.Limit > 0 {
		limit = min(filter.Limit, e.capacity)
	}

	var count int
	for _, event := range e.events {
		if count >= limit {
			break
		}

		if event != nil && filter.Matches(event) {
			count++
		}
	}
	return int64(count), nil
}
