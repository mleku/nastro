package nastro

import (
	"context"
	"errors"
	"fmt"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrInvalidReplacement = errors.New("called Replace on a non-replaceable event")

	ErrTooManyIDs     = errors.New("too many IDs in filter")
	ErrTooManyAuthors = errors.New("too many authors in filter")
	ErrTooManyKinds   = errors.New("too many kinds in filter")
	ErrTooManyTags    = errors.New("too many tags in filter")
	ErrEmptyFilter    = errors.New("filter must specify at least one ID, kind, author, tag, or time range")

	ErrInternalQuery = errors.New("internal query error")
)

type Store interface {
	// Save the event in the store
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

	// Query stored events matching the provided filter.
	Query(ctx context.Context, filter *nostr.Filter) ([]nostr.Event, error)

	// Count stored events matching the provided filter.
	Count(ctx context.Context, filter *nostr.Filter) (int64, error)
}

// QueryLimits protects the database from queries (not counts) that are too expensive.
type QueryLimits struct {
	MaxIDs     int
	MaxKinds   int
	MaxAuthors int
	MaxTags    int
	MaxLimit   int
}

func NewQueryLimits() QueryLimits {
	return QueryLimits{
		MaxIDs:     100,
		MaxKinds:   10,
		MaxAuthors: 100,
		MaxTags:    5,
		MaxLimit:   1000,
	}
}

// Validate returns an error if the filter breaks any of the [QueryLimits].
// It modifies the filter's limit if it's either not set or too big.
func (q QueryLimits) Validate(filter *nostr.Filter) error {
	IDs := len(filter.IDs)
	if IDs > q.MaxIDs {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyIDs, q.MaxIDs, IDs)
	}

	kinds := len(filter.Kinds)
	if kinds > q.MaxKinds {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyKinds, q.MaxKinds, kinds)
	}

	authors := len(filter.Authors)
	if authors > q.MaxAuthors {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyAuthors, q.MaxAuthors, authors)
	}

	tags := len(filter.Tags)
	if tags > q.MaxTags {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyTags, q.MaxTags, tags)
	}

	if IDs+kinds+authors+tags == 0 && filter.Since == nil && filter.Until == nil {
		return ErrEmptyFilter
	}

	if filter.Limit < 1 || filter.Limit > q.MaxLimit {
		filter.Limit = q.MaxLimit
	}
	return nil
}

func ValidateReplacement(kind int) error {
	if !nostr.IsReplaceableKind(kind) && !nostr.IsAddressableKind(kind) {
		return ErrInvalidReplacement
	}
	return nil
}
