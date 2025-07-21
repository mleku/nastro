// The sqlite package defines an extensible sqlite3 store for Nostr events.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nbd-wtf/go-nostr"
	"github.com/pippellia-btc/nastro"
)

const schema = `
	CREATE TABLE IF NOT EXISTS events (
       id TEXT PRIMARY KEY,
       pubkey TEXT NOT NULL,
       created_at INTEGER NOT NULL,
       kind INTEGER NOT NULL,
       tags JSONB NOT NULL,
       content TEXT NOT NULL,
       sig TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS pubkey_idx ON events(pubkey);
	CREATE INDEX IF NOT EXISTS time_idx ON events(created_at DESC);
	CREATE INDEX IF NOT EXISTS kind_idx ON events(kind);
	
	CREATE TABLE IF NOT EXISTS event_tags (
		event_id TEXT NOT NULL,
		key TEXT NOT NULL,
		value TEXT NOT NULL,
		
		PRIMARY KEY (event_id, key, value),
		FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS event_tags_key_value_idx ON event_tags(key, value);

	CREATE TRIGGER IF NOT EXISTS d_tags_ai AFTER INSERT ON events
	WHEN NEW.kind BETWEEN 30000 AND 39999 
	BEGIN
	INSERT INTO event_tags (event_id, key, value)
		SELECT NEW.id, 'd', json_extract(value, '$[1]')
		FROM json_each(NEW.tags)
		WHERE json_type(value) = 'array' AND json_array_length(value) > 1 AND json_extract(value, '$[0]') = 'd'
		LIMIT 1;
	END;`

// Store of Nostr events that uses an sqlite3 database.
// It embeds the *sql.DB connection for direct interaction and manages optional validators and query builders.
type Store struct {
	*sql.DB
	retries int // the maximum number of retries after a write failure "database is locked"

	validateFilter nastro.FilterValidator
	validateEvent  nastro.EventValidator

	queryBuilder QueryBuilder
	countBuilder QueryBuilder
}

// QueryBuilder converts multiple nostr filters into one or more sqlite queries and lists of arguments.
// Not all filters can be combined into a single query, but many can.
// Filters passed to the query builder have been previously validated by [nastro.QueryLimits] (if specified).
//
// It's useful to specify custom query/count builders to leverage additional schemas that have been
// provided in the [New] constructor.
//
// For examples, check out the [DefaultQueryBuilder] and [DefaultCountBuilder]
type QueryBuilder func(filters ...nostr.Filter) (queries []Query, err error)

type Query struct {
	SQL  string
	Args []any
}

type Option func(*Store) error

// WithRetries sets how many times to retry a locked database operation
// after the first failed attempt. Each retry waits 20ms + jitter (~5ms on average).
func WithRetries(n int) Option {
	return func(s *Store) error {
		if n < 0 {
			return errors.New("number of retries must be non-negative")
		}
		s.retries = n
		return nil
	}
}

// WithFilterValidator sets a custom [nastro.FilterValidator] on the Store.
// It will be used to validate filters before executing queries.
func WithFilterValidator(v nastro.FilterValidator) Option {
	return func(s *Store) error {
		s.validateFilter = v
		return nil
	}
}

// WithEventValidator sets a custom [nastro.EventValidator] on the Store.
// It will be used to validate events before inserting them into the database.
func WithEventValidator(v nastro.EventValidator) Option {
	return func(s *Store) error {
		s.validateEvent = v
		return nil
	}
}

// WithQueryBuilder allows to specify the query builder used by the store in [Store.Query].
func WithQueryBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.queryBuilder = b
		return nil
	}
}

// WithCountBuilder allows to specify the query builder used by the store in [Store.Count].
func WithCountBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.countBuilder = b
		return nil
	}
}

// WithAdditionalSchema allows to specify an additional database schema, like new tables,
// virtual tables, indexes and triggers.
func WithAdditionalSchema(schema string) Option {
	return func(s *Store) error {
		if _, err := s.DB.Exec(schema); err != nil {
			return fmt.Errorf("failed to apply additional schema: %w", err)
		}
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

	if _, err := DB.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to apply base schema: %w", err)
	}

	if _, err := DB.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	store := &Store{
		DB:             DB,
		validateFilter: nastro.DefaultFilterValidator,
		validateEvent:  func(e *nostr.Event) error { return nil },
		queryBuilder:   DefaultQueryBuilder,
		countBuilder:   DefaultCountBuilder,
	}

	for _, opt := range opts {
		if err := opt(store); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// IsDatabaseLocked returns true if the error indicates a locked SQLite database.
func IsDatabaseLocked(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database is locked")
}

// withRetries executes the given database operation with automatic retries
// in case of a "database is locked" error. It executes [Store.retries]+1 times,
// waiting 20ms + jitter (5ms on average) between attempts to reduce contention.
// Returns the operation error immediately if it’s not a locking issue.
//
// Note: this function is only useful for writes and not reads if the journal
// mode is set to WAL (default), as readers don't lock the database.
func (s *Store) withRetries(op func() error) error {
	for i := range s.retries + 1 {
		err := op()
		if !IsDatabaseLocked(err) {
			return err
		}

		if i < s.retries {
			// sleep unless it's the last try
			jitter := time.Duration(rand.IntN(10)) * time.Millisecond
			time.Sleep(20*time.Millisecond + jitter)
		}
	}
	return fmt.Errorf("database is locked: performed (%d) attempts", s.retries+1)
}

func (s *Store) Save(ctx context.Context, e *nostr.Event) error {
	if err := s.validateEvent(e); err != nil {
		return err
	}

	tags, err := json.Marshal(e.Tags)
	if err != nil {
		return fmt.Errorf("failed to marshal the tags of event with ID %s: %w", e.ID, err)
	}

	err = s.withRetries(func() error {
		_, err := s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`, e.ID, e.PubKey, e.CreatedAt, e.Kind, tags, e.Content, e.Sig)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to save event with ID %s: %w", e.ID, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	err := s.withRetries(func() error {
		_, err := s.DB.ExecContext(ctx, "DELETE FROM events WHERE id = $1", id)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to delete event with ID %s: %w", id, err)
	}
	return nil
}

func (s *Store) Replace(ctx context.Context, event *nostr.Event) (bool, error) {
	if err := s.validateEvent(event); err != nil {
		return false, err
	}

	var query string
	var args []any

	switch {
	case nostr.IsReplaceableKind(event.Kind):
		query = "SELECT id, created_at FROM events WHERE kind = $1 AND pubkey = $2"
		args = []any{event.Kind, event.PubKey}

	case nostr.IsAddressableKind(event.Kind):
		query = "SELECT e.id, e.created_at FROM events AS e JOIN event_tags AS t ON e.id = t.event_id WHERE e.kind = $1 AND e.pubkey = $2 AND t.key = 'd' AND t.value = $3;"
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
		return false, fmt.Errorf("failed to query for old events to replace: %w", err)
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

	return s.withRetries(func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to initiate the transaction: %w", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, new.ID, new.PubKey, new.CreatedAt, new.Kind, tags, new.Content, new.Sig)

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
	})
}

func (s *Store) Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error) {
	return s.QueryWithBuilder(ctx, s.queryBuilder, filters...)
}

// QueryWithBuilder generates an sqlite query for the filters with the provided [QueryBuilder], and executes it.
func (s *Store) QueryWithBuilder(ctx context.Context, build QueryBuilder, filters ...nostr.Filter) ([]nostr.Event, error) {
	if err := s.validateFilter(filters...); err != nil {
		return nil, err
	}

	queries, err := build(filters...)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	var events []nostr.Event
	for i, query := range queries {
		rows, err := s.DB.QueryContext(ctx, query.SQL, query.Args...)
		if errors.Is(err, sql.ErrNoRows) {
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

func (s *Store) Count(ctx context.Context, filters ...nostr.Filter) (int64, error) {
	return s.CountWithBuilder(ctx, s.countBuilder, filters...)
}

// CountWithBuilder generates an sqlite query for the filters with the provided [QueryBuilder], and executes it.
func (s *Store) CountWithBuilder(ctx context.Context, builder QueryBuilder, filters ...nostr.Filter) (int64, error) {
	queries, err := builder(filters...)
	if err != nil {
		return 0, fmt.Errorf("failed to build count query: %w", err)
	}

	var total int64
	for i, query := range queries {
		var count int64
		row := s.DB.QueryRowContext(ctx, query.SQL, query.Args...)
		err := row.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to count events with query %s: %w", queries[i], err)
		}

		total += count
	}
	return total, nil
}

func DefaultQueryBuilder(filters ...nostr.Filter) ([]Query, error) {
	filters = nastro.RemoveZeros(filters)
	switch len(filters) {
	case 0:
		return nil, nil

	case 1:
		query, args := buildQuery(filters[0])
		query += " ORDER BY e.created_at DESC, e.id ASC LIMIT ?"
		args = append(args, filters[0].Limit)
		return []Query{{SQL: query, Args: args}}, nil

	default:
		subQueries := make([]string, 0, len(filters))
		allArgs := make([]any, 0, len(filters))
		limit := 0

		for _, filter := range filters {
			query, args := buildQuery(filter)
			subQueries = append(subQueries, query)
			allArgs = append(allArgs, args...)
			limit += filter.Limit
		}

		query := "SELECT * FROM (" + strings.Join(subQueries, " UNION ALL ") + ")" +
			" GROUP BY id ORDER BY created_at DESC, id ASC LIMIT ?"
		allArgs = append(allArgs, limit)
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func DefaultCountBuilder(filters ...nostr.Filter) ([]Query, error) {
	filters = nastro.RemoveZeros(filters)
	switch len(filters) {
	case 0:
		return nil, nil

	case 1:
		query, args := buildCount(filters[0])
		return []Query{{SQL: query, Args: args}}, nil

	default:
		subQueries := make([]string, 0, len(filters))
		allArgs := make([]any, 0, len(filters))

		for _, filter := range filters {
			query, args := buildCount(filter)
			subQueries = append(subQueries, "("+query+")")
			allArgs = append(allArgs, args...)
		}

		// TODO: we are summing all counts together, without any deduplication
		query := "SELECT (" + strings.Join(subQueries, " + ") + ")"
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func buildQuery(filter nostr.Filter) (string, []any) {
	sql := toSql(filter)
	if sql.JoinTags {
		query := "SELECT e.* FROM events AS e JOIN event_tags AS t ON t.event_id = e.id" +
			" WHERE " + strings.Join(sql.Conditions, " AND ") + " GROUP BY e.id"
		return query, sql.Args
	}

	query := "SELECT e.* FROM events AS e"
	if len(sql.Conditions) > 0 {
		query += " WHERE " + strings.Join(sql.Conditions, " AND ")
	}
	return query, sql.Args
}

func buildCount(filter nostr.Filter) (string, []any) {
	sql := toSql(filter)
	if sql.JoinTags {
		query := "SELECT COUNT(DISTINCT e.id) FROM events AS e JOIN event_tags AS t ON t.event_id = e.id" +
			" WHERE " + strings.Join(sql.Conditions, " AND ")
		return query, sql.Args
	}

	query := "SELECT COUNT(e.id) FROM events AS e"
	if len(sql.Conditions) > 0 {
		query += " WHERE " + strings.Join(sql.Conditions, " AND ")
	}
	return query, sql.Args
}

type sqlFilter struct {
	Conditions []string
	Args       []any
	JoinTags   bool
}

func toSql(filter nostr.Filter) sqlFilter {
	s := sqlFilter{}
	if len(filter.IDs) > 0 {
		s.Conditions = append(s.Conditions, "e.id"+equalityClause(filter.IDs))
		for _, id := range filter.IDs {
			s.Args = append(s.Args, id)
		}
	}

	if len(filter.Kinds) > 0 {
		s.Conditions = append(s.Conditions, "e.kind"+equalityClause(filter.Kinds))
		for _, kind := range filter.Kinds {
			s.Args = append(s.Args, kind)
		}
	}

	if len(filter.Authors) > 0 {
		s.Conditions = append(s.Conditions, "e.pubkey"+equalityClause(filter.Authors))
		for _, pk := range filter.Authors {
			s.Args = append(s.Args, pk)
		}
	}

	if filter.Until != nil {
		s.Conditions = append(s.Conditions, "e.created_at <= ?")
		s.Args = append(s.Args, filter.Until.Time().Unix())
	}

	if filter.Since != nil {
		s.Conditions = append(s.Conditions, "e.created_at >= ?")
		s.Args = append(s.Args, filter.Since.Time().Unix())
	}

	if len(filter.Tags) > 0 {
		conds := make([]string, 0, len(filter.Tags))
		args := make([]any, 0, len(filter.Tags))

		for key, vals := range filter.Tags {
			if len(vals) == 0 {
				continue
			}

			conds = append(conds, "(t.key = ? AND t.value"+equalityClause(vals)+")")
			args = append(args, key)
			for _, v := range vals {
				args = append(args, v)
			}
		}

		if len(conds) > 0 {
			s.JoinTags = true
			s.Conditions = append(s.Conditions, strings.Join(conds, " OR "))
			s.Args = append(s.Args, args...)
		}
	}
	return s
}

// equalityClause returns the appropriate SQL comparison operator and placeholder(s)
// for use in a WHERE clause, based on the number of values provided.
// If the slice contains one value, it returns " = ?".
// If it contains multiple values, it returns " IN (?, ?, ... )" with the correct number of placeholders.
// It panics is vals is nil or empty.
func equalityClause[T any](vals []T) string {
	if len(vals) == 1 {
		return " = ?"
	}
	return " IN (?" + strings.Repeat(",?", len(vals)-1) + ")"
}
