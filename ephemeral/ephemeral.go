// The ephemeral package defines an in-memory, thread-safe ring-buffer for storing Nostr events
package ephemeral

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

// Ephemeral is an in-memory, thread-safe ring-buffer for storing Nostr events.
// It maintains a fixed memory footprint, storing up to `capacity` events.
// When new events are saved and the capacity is full, they overwrite the oldest events
// in a circular fashion.
//
// Due to its expected small capacity (e.g. 1000 events) and in-memory nature,
// it does not impose query result limits beyond what the Nostr filter itself specifies
type Store struct {
	mu       sync.RWMutex
	events   []*nostr.Event
	write    int
	capacity int
}

// New creates an ephemeral store with the provided capacity.
func New(capacity int) *Store {
	return &Store{
		events:   make([]*nostr.Event, capacity),
		capacity: capacity,
	}
}

// Size returns the number of events currently stored.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var size int
	for _, event := range s.events {
		if event != nil {
			size++
		}
	}
	return size
}

// Capacity returns the maximum number of events that can be stored.
func (s *Store) Capacity() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.capacity
}

// Resize the ephemeral store with the provided capacity.
func (s *Store) Resize(capacity int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.write = 0
	s.capacity = capacity
	events := make([]*nostr.Event, capacity)

	for _, event := range s.events {
		if event != nil {
			events[s.write] = event
			s.write++

			if s.write >= capacity {
				// reached capacity
				break
			}
		}
	}
	s.events = events
}

func (s *Store) Save(ctx context.Context, event *nostr.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events[s.write] = event
	s.write = (s.write + 1) % s.capacity
	return nil
}

func (s *Store) Replace(ctx context.Context, event *nostr.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, stored := range s.events {
		if stored == nil {
			continue
		}

		if isReplacementCandidate(event, stored) {
			if event.CreatedAt > stored.CreatedAt {
				s.events[i] = event
				return true, nil
			}
			return false, nil
		}
	}

	// no candidates found, save
	s.events[s.write] = event
	s.write = (s.write + 1) % s.capacity
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

func (s *Store) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := slices.IndexFunc(s.events, func(event *nostr.Event) bool { return event != nil && event.ID == id })
	if pos == -1 {
		return nil
	}

	s.events[pos] = nil
	return nil
}

func (s *Store) Query(ctx context.Context, filter *nostr.Filter) ([]nostr.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := s.capacity
	if filter.Limit > 0 {
		limit = min(filter.Limit, s.capacity)
	}

	events := make([]nostr.Event, 0, limit)
	for _, event := range s.events {
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

func (s *Store) Count(ctx context.Context, filter *nostr.Filter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := s.capacity
	if filter.Limit > 0 {
		limit = min(filter.Limit, s.capacity)
	}

	var count int
	for _, event := range s.events {
		if count >= limit {
			break
		}

		if event != nil && filter.Matches(event) {
			count++
		}
	}
	return int64(count), nil
}
