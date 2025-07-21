package nastro

import (
	"context"
	"errors"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrInvalidReplacement = errors.New("called Replace on a non-replaceable event")
	ErrInternalQuery      = errors.New("internal query error")
	ErrUnspecifiedLimit   = errors.New("unspecified filter's limit")
)

type Store interface {
	// Save the event in the store. For replaceable/addressable event, it is
	// recommended to call Replace instead
	Save(ctx context.Context, event *nostr.Event) error

	// Delete the event with the provided id. If the event is not found, nothing happens and nil is returned.
	Delete(ctx context.Context, id string) error

	// Replace an old event with the new one according to NIP-01.
	//
	// The replacement happens if the event is strictly newer than the stored event
	// within the same 'category' (kind, pubkey, and d-tag if addressable).
	// If no such stored event exists, and the event is a replaceable/addressable kind, it is simply saved.
	//
	// Calling Replace on a non-replaceable/addressable event returns [ErrInvalidReplacement]
	//
	// Replace returns true if the event has been saved/superseded a previous one,
	// false in case of errors or if a stored event in the same 'category' is newer or equal.
	//
	// More info here: https://github.com/nostr-protocol/nips/blob/master/01.md#kinds
	Replace(ctx context.Context, event *nostr.Event) (bool, error)

	// Query stored events matching the provided filters.
	Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error)

	// Count stored events matching the provided filters.
	Count(ctx context.Context, filters ...nostr.Filter) (int64, error)
}

// FilterValidator validates one or more nostr filters before executing a query.
type FilterValidator func(...nostr.Filter) error

func DefaultFilterValidator(filters ...nostr.Filter) error {
	for _, f := range filters {
		if !f.LimitZero && f.Limit < 1 {
			return ErrUnspecifiedLimit
		}
	}
	return nil
}

// EventValidator validates an event before writing it into the store.
type EventValidator func(*nostr.Event) error

// RemoveZeros removes filters with LimitZero set to true.
func RemoveZeros(filters []nostr.Filter) []nostr.Filter {
	result := make([]nostr.Filter, 0, len(filters))
	for _, f := range filters {
		if !f.LimitZero {
			result = append(result, f)
		}
	}
	return result
}

func IsValidReplacement(kind int) bool {
	return nostr.IsReplaceableKind(kind) || nostr.IsAddressableKind(kind)
}
