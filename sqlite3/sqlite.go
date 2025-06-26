// The sqlite package defines an extensible sqlite3 store for Nostr Events.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
)

const baseSchema = `
	CREATE TABLE IF NOT EXISTS events (
       id TEXT PRIMARY KEY,
       pubkey TEXT NOT NULL,
       created_at INTEGER NOT NULL,
       kind INTEGER NOT NULL,
       tags JSONB NOT NULL,
	   d_tag TEXT,
       content TEXT NOT NULL,
       sig TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS pubkey_idx ON events(pubkey);
	CREATE INDEX IF NOT EXISTS time_idx ON events(created_at DESC);
	CREATE INDEX IF NOT EXISTS kind_idx ON events(kind);
	CREATE INDEX IF NOT EXISTS addressable_idx ON events(kind, pubkey, d_tag);`

// Store of Nostr events store that uses an sqlite3 database.
// It embeds the *sql.DB connection for direct interaction and manages
// configurable limits and query building for efficient event handling.
type Store struct {
	*sql.DB
	builder QueryBuilder

	queryLimits nastro.QueryLimits
	writeLimits nastro.WriteLimits
}

// QueryBuilder converts multiple nostr filters into one or more sqlite queries and lists of arguments.
// Not all filters can be combined into a single query, but many can.
//
// It's useful to specify a custom query builder to leverage additional schemas that have been
// provided in the [New] constructor.
//
// For an example, check out the [DefaultQueryBuilder]
type QueryBuilder func(filters nostr.Filters) (queries []string, args [][]any, err error)

type Option func(*Store) error

func WithQueryBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.builder = b
		return nil
	}
}

func WithAdditionalSchema(schema string) Option {
	return func(s *Store) error {
		if _, err := s.DB.Exec(schema); err != nil {
			return fmt.Errorf("failed to apply additional schema: %w", err)
		}
		return nil
	}
}

func WithQueryLimits(q nastro.QueryLimits) Option {
	return func(s *Store) error {
		s.queryLimits = q
		return nil
	}
}

func WithWriteLimits(w nastro.WriteLimits) Option {
	return func(s *Store) error {
		s.writeLimits = w
		return nil
	}
}

// New returns an sqlite3 store connected to the sqlite file located at the URL,
// after applying the base schema, and the provided options.
func New(URL string, opts ...Option) (*Store, error) {
	DB, err := sql.Open("sqlite3", URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to sqlite3 at %s: %w", URL, err)
	}

	if _, err := DB.Exec(baseSchema); err != nil {
		return nil, fmt.Errorf("failed to apply base schema: %w", err)
	}

	if _, err := DB.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	store := &Store{
		DB:          DB,
		builder:     DefaultQueryBuilder,
		queryLimits: nastro.NewQueryLimits(),
		writeLimits: nastro.NewWriteLimits(),
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *Store) Save(ctx context.Context, e *nostr.Event) error {
	if err := s.writeLimits.Validate(e); err != nil {
		return err
	}

	tags, err := json.Marshal(e.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal the tags of event with ID %s: %w", e.ID, err)
	}

	var dTag string
	if nostr.IsAddressableKind(e.Kind) {
		dTag = e.Tags.GetD()
	}

	_, err = s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, d_tag, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, e.ID, e.PubKey, e.CreatedAt, e.Kind, tags, dTag, e.Content, e.Sig)

	if err != nil {
		return fmt.Errorf("failed to save event with ID %s: %w", e.ID, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	if _, err := s.DB.ExecContext(ctx, "DELETE FROM events WHERE id = $1", id); err != nil {
		return fmt.Errorf("failed to delete event with ID %s: %w", id, err)
	}
	return nil
}

func (s *Store) Replace(ctx context.Context, event *nostr.Event) (bool, error) {
	if err := s.writeLimits.Validate(event); err != nil {
		return false, err
	}

	var query string
	var args []any

	switch {
	case nostr.IsReplaceableKind(event.Kind):
		query = "SELECT id, created_at FROM events WHERE kind = $1 AND pubkey = $2"
		args = []any{event.Kind, event.PubKey}

	case nostr.IsAddressableKind(event.Kind):
		query = "SELECT id, created_at FROM events WHERE kind = $1 AND pubkey = $2 AND d_tag = $3"
		args = []any{event.Kind, event.PubKey, event.Tags.GetD()}

	default:
		return false, fmt.Errorf("%w: event ID %s, kind %d", nastro.ErrInvalidReplacement, event.ID, event.Kind)
	}

	var oldID string
	var oldCreatedAt nostr.Timestamp
	row := s.DB.QueryRowContext(ctx, query, args...)
	err := row.Scan(&oldID, &oldCreatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		if err := s.Save(ctx, event); err != nil {
			return false, err
		}
		return true, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to query for old events: %w", err)
	}

	if oldCreatedAt >= event.CreatedAt {
		// event is not newer, don't replace
		return false, nil
	}

	if err = s.replace(ctx, event, oldID); err != nil {
		return false, err
	}
	return true, nil
}

// replace the event with the provided id with the new event.
// It's an atomic version of Save(ctx, new) + Delete(ctx, id)
func (s *Store) replace(ctx context.Context, new *nostr.Event, id string) error {
	tags, err := json.Marshal(new.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal the tags: %w", err)
	}

	var dTag string
	if nostr.IsAddressableKind(new.Kind) {
		dTag = new.Tags.GetD()
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to initiate the transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, d_tag, content, sig)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, new.ID, new.PubKey, new.CreatedAt, new.Kind, tags, dTag, new.Content, new.Sig)

	if err != nil {
		return fmt.Errorf("failed to save event with ID %s: %w", new.ID, err)
	}

	if _, err = tx.ExecContext(ctx, "DELETE FROM events WHERE id = $1", id); err != nil {
		return fmt.Errorf("failed to delete old event with ID %s: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to replace event %s with event %s: %w", id, new.ID, err)
	}
	return nil
}

func (s *Store) Query(ctx context.Context, filters nostr.Filters) ([]nostr.Event, error) {
	return s.QueryWithBuilder(ctx, filters, s.builder)
}

func (s *Store) QueryWithBuilder(ctx context.Context, filters nostr.Filters, builder QueryBuilder) ([]nostr.Event, error) {
	if err := s.queryLimits.Validate(filters); err != nil {
		return nil, err
	}

	queries, args, err := builder(filters)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	events := make([]nostr.Event, 0, s.queryLimits.MaxLimit)
	for i := range queries {
		rows, err := s.DB.QueryContext(ctx, queries[i], args[i]...)
		if errors.Is(err, sql.ErrNoRows) {
			// no results, skip to next query
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("failed to fetch events with query %s: %w", queries[i], err)
		}
		defer rows.Close()

		for rows.Next() {
			var event nostr.Event
			err = rows.Scan(&event.ID, &event.PubKey, &event.CreatedAt, &event.Kind, &event.Tags, &event.Content, &event.Sig)
			if err != nil {
				return events, fmt.Errorf("%w: failed to scan event row: %w", nastro.ErrInternalQuery, err)
			}

			events = append(events, event)
		}

		if err := rows.Err(); err != nil {
			return events, fmt.Errorf("%w: failed to scan event row: %w", nastro.ErrInternalQuery, err)
		}
	}
	return events, nil
}

// Default [QueryBuilder] for the [baseSchema]. It does not perform NIP-50 full text search.
func DefaultQueryBuilder(filters nostr.Filters) (queries []string, args [][]any, err error) {
	if len(filters) == 0 {
		return nil, nil, nastro.ErrEmptyFilters
	}

	queries = make([]string, len(filters))
	args = make([][]any, len(filters))

	for i, filter := range filters {
		queries[i], args[i] = buildQuery(filter)
	}
	return queries, args, nil
}

func buildQuery(filter nostr.Filter) (query string, args []any) {
	var conditions []string

	if len(filter.IDs) > 0 {
		conditions = append(conditions, "id IN "+ValueList(len(filter.IDs)))
		for _, ID := range filter.IDs {
			args = append(args, ID)
		}
	}

	if len(filter.Kinds) > 0 {
		conditions = append(conditions, "kind IN "+ValueList(len(filter.Kinds)))
		for _, kind := range filter.Kinds {
			args = append(args, kind)
		}
	}

	if len(filter.Authors) > 0 {
		conditions = append(conditions, "pubkey IN "+ValueList(len(filter.Authors)))
		for _, author := range filter.Authors {
			args = append(args, author)
		}
	}

	if filter.Until != nil {
		conditions = append(conditions, "created_at <= ?")
		args = append(args, filter.Until.Time().Unix())
	}

	if filter.Since != nil {
		conditions = append(conditions, "created_at >= ?")
		args = append(args, filter.Since.Time().Unix())
	}

	if len(filter.Tags) > 0 {
		tagCond := make([]string, 0, len(filter.Tags))
		for key, vals := range filter.Tags {
			if len(vals) == 0 {
				continue
			}

			tagCond = append(tagCond,
				`EXISTS (
				SELECT 1 FROM json_each(tags) 
				WHERE json_valid(tags)
				AND json_each.value ->> 0 = ? 
				AND json_each.value ->> 1 IN `+ValueList(len(vals))+
					` )`)

			args = append(args, key)
			for _, val := range vals {
				args = append(args, val)
			}
		}

		// tag conditions are OR-ed together
		conditions = append(conditions, "( "+strings.Join(tagCond, " OR ")+" )")
	}

	query = "SELECT * FROM events WHERE " + strings.Join(conditions, " AND ") + " ORDER BY created_at DESC, id LIMIT ?"
	args = append(args, filter.Limit)
	return query, args
}

// ValueList for sqlite queries. e.g. ValueList(4) = "(?,?,?,?)".
// It panics if n < 1.
func ValueList(n int) string {
	return "(?" + strings.Repeat(",?", n-1) + ")"
}
