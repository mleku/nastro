package nastro

import (
	"context"
	"errors"
	"fmt"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrTooManyEventTags    = errors.New("too many tags in event")
	ErrTooMuchEventContent = errors.New("event content is too big")

	ErrEmptyFilters         = errors.New("filters slice cannot be empty")
	ErrEmptyFilter          = errors.New("filter must specify at least one ID, kind, author, tag, or time range")
	ErrTooManyFilterIDs     = errors.New("too many IDs in filters")
	ErrTooManyFilterAuthors = errors.New("too many authors in filters")
	ErrTooManyFilterKinds   = errors.New("too many kinds in filters")
	ErrTooManyFilterTags    = errors.New("too many tags in filters")

	ErrInvalidReplacement = errors.New("called Replace on a non-replaceable event")
	ErrInternalQuery      = errors.New("internal query error")
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
		MaxIDs:     500,
		MaxKinds:   50,
		MaxAuthors: 500,
		MaxTags:    25,
		MaxLimit:   5000,
	}
}

// Validate returns an error if the filters breaks any of the [QueryLimits].
// It modifies the filter.Limit id unset or too big.
func (q QueryLimits) Validate(filters ...nostr.Filter) error {
	if len(filters) == 0 {
		return ErrEmptyFilters
	}

	var IDs, kinds, authors, tags int
	for i := range filters {
		if IsEmptyFilter(filters[i]) {
			return fmt.Errorf("filters[%d]: %w", i, ErrEmptyFilter)
		}

		if filters[i].Limit < 1 || filters[i].Limit > q.MaxLimit/len(filters) {
			filters[i].Limit = q.MaxLimit / len(filters)
		}

		IDs += len(filters[i].IDs)
		kinds += len(filters[i].Kinds)
		authors += len(filters[i].Authors)
		tags += len(filters[i].Tags)
	}

	if IDs > q.MaxIDs {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyFilterIDs, q.MaxIDs, IDs)
	}
	if kinds > q.MaxKinds {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyFilterKinds, q.MaxKinds, kinds)
	}
	if authors > q.MaxAuthors {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyFilterAuthors, q.MaxAuthors, authors)
	}
	if tags > q.MaxTags {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyFilterTags, q.MaxTags, tags)
	}
	return nil
}

// WriteLimits protects the database from saves or replaces that are too expensive.
type WriteLimits struct {
	MaxTags          int
	MaxContentLenght int
}

func NewWriteLimits() WriteLimits {
	return WriteLimits{
		MaxTags:          10000,
		MaxContentLenght: 50000,
	}
}

// Validate returns an error if the event breaks any of the [WriteLimits].
func (w WriteLimits) Validate(event *nostr.Event) error {
	if len(event.Tags) > w.MaxTags {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooManyEventTags, w.MaxTags, len(event.Tags))
	}

	if len(event.Content) > w.MaxContentLenght {
		return fmt.Errorf("%w: max %d, requested %d", ErrTooMuchEventContent, w.MaxContentLenght, len(event.Content))
	}
	return nil
}

func IsValidReplacement(kind int) bool {
	return nostr.IsReplaceableKind(kind) || nostr.IsAddressableKind(kind)
}

func IsEmptyFilter(f nostr.Filter) bool {
	return len(f.IDs) == 0 && len(f.Kinds) == 0 && len(f.Authors) == 0 && len(f.Tags) == 0 && f.Since == nil && f.Until == nil
}
