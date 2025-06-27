package sqlite

import (
	"reflect"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestDefaultQueryBuilder(t *testing.T) {
	tests := []struct {
		name    string
		filters nostr.Filters
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: nostr.Filters{{Kinds: []int{0, 1}, Limit: 100}},
			query: Query{
				SQL:  "SELECT * FROM events AS e WHERE e.kind IN (?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{0, 1, 100},
			},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}, Limit: 11}},
			query: Query{
				SQL:  "SELECT * FROM events AS e WHERE e.pubkey IN (?,?,?) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"aaa", "bbb", "xxx", 11},
			},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"someone"},
				},
			}},

			query: Query{
				SQL:  "SELECT * FROM events AS e WHERE EXISTS (SELECT 1 FROM event_tags AS t WHERE t.event_id = e.id AND ((t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value IN (?)))) ORDER BY e.created_at DESC, e.id ASC LIMIT ?",
				Args: []any{"e", "xxx", "yyy", "p", "someone", 11},
			},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}, Limit: 69},
				{Authors: []string{"aaa", "bbb"}, Limit: 420},
			},
			query: Query{
				SQL:  "SELECT DISTINCT * FROM (SELECT * FROM events AS e WHERE e.kind IN (?,?) UNION ALL SELECT * FROM events AS e WHERE e.pubkey IN (?,?)) ORDER BY created_at DESC, id ASC LIMIT ?",
				Args: []any{0, 1, "aaa", "bbb", 69 + 420},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := DefaultQueryBuilder(test.filters...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(query[0], test.query) {
				t.Fatalf("expected query %v, got %v", test.query, query[0])
			}
		})
	}
}

func TestDefaultCountBuilder(t *testing.T) {
	tests := []struct {
		name    string
		filters nostr.Filters
		query   Query
	}{
		{
			name:    "single filter, kind",
			filters: nostr.Filters{{Kinds: []int{0, 1}}},
			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE e.kind IN (?,?)",
				Args: []any{0, 1},
			},
		},
		{
			name:    "single filter, authors",
			filters: nostr.Filters{{Authors: []string{"aaa", "bbb", "xxx"}}},
			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE e.pubkey IN (?,?,?)",
				Args: []any{"aaa", "bbb", "xxx"},
			},
		},
		{
			name: "single filter, tags",
			filters: nostr.Filters{{
				Limit: 11,
				Tags: nostr.TagMap{
					"e": {"xxx", "yyy"},
					"p": {"someone"},
				},
			}},

			query: Query{
				SQL:  "SELECT COUNT(*) FROM events AS e WHERE EXISTS (SELECT 1 FROM event_tags AS t WHERE t.event_id = e.id AND ((t.key = ? AND t.value IN (?,?)) OR (t.key = ? AND t.value IN (?))))",
				Args: []any{"e", "xxx", "yyy", "p", "someone"},
			},
		},
		{
			name: "multiple filter",
			filters: nostr.Filters{
				{Kinds: []int{0, 1}},
				{Authors: []string{"aaa", "bbb"}},
			},
			query: Query{
				SQL:  "SELECT ((SELECT COUNT(*) FROM events AS e WHERE e.kind IN (?,?)) + (SELECT COUNT(*) FROM events AS e WHERE e.pubkey IN (?,?)))",
				Args: []any{0, 1, "aaa", "bbb"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := DefaultCountBuilder(test.filters...)
			if err != nil {
				t.Fatalf("expected error nil, got %v", err)
			}

			if !reflect.DeepEqual(query[0], test.query) {
				t.Fatalf("expected query %v, got %v", test.query, query[0])
			}
		})
	}
}
