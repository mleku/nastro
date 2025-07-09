// The sqlite package defines an extensible sqlite3 store for Nostr events.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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

// Store of Nostr events store that uses an sqlite3 database.
// It embeds the *sql.DB connection for direct interaction and manages
// configurable limits and query building.
type Store struct {
	*sql.DB
	queryBuilder QueryBuilder
	countBuilder QueryBuilder

	queryLimits nastro.QueryLimits
	writeLimits nastro.WriteLimits
}

// QueryBuilder converts multiple nostr filters into one or more sqlite queries and lists of arguments.
// Filters passed to the query builder have been previously validated by [nastro.QueryLimits]
// Not all filters can be combined into a single query, but many can.
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

func WithQueryBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.queryBuilder = b
		return nil
	}
}

func WithCountBuilder(b QueryBuilder) Option {
	return func(s *Store) error {
		s.countBuilder = b
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

	if _, err := DB.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to apply base schema: %w", err)
	}

	if _, err := DB.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	store := &Store{
		DB:           DB,
		queryBuilder: DefaultQueryBuilder,
		countBuilder: DefaultCountBuilder,
		queryLimits:  nastro.NewQueryLimits(),
		writeLimits:  nastro.NewWriteLimits(),
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

	_, err = s.DB.ExecContext(ctx, `INSERT OR IGNORE INTO events (id, pubkey, created_at, kind, tags, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7)`, e.ID, e.PubKey, e.CreatedAt, e.Kind, tags, e.Content, e.Sig)

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
}

func (s *Store) Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error) {
	return s.QueryWithBuilder(ctx, s.queryBuilder, filters...)
}

// QueryWithBuilder generates an sqlite query for the filters with the provided builder, and executes it.
func (s *Store) QueryWithBuilder(ctx context.Context, builder QueryBuilder, filters ...nostr.Filter) ([]nostr.Event, error) {
	if err := s.queryLimits.Validate(filters...); err != nil {
		return nil, err
	}

	queries, err := builder(filters...)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	events := make([]nostr.Event, 0, s.queryLimits.MaxLimit)
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

// CountWithBuilder generates an sqlite query for the filters with the provided builder, and executes it.
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
	switch len(filters) {
	case 0:
		return nil, nastro.ErrEmptyFilters

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

		query := "SELECT DISTINCT * FROM (" + strings.Join(subQueries, " UNION ALL ") + ")" +
			" ORDER BY created_at DESC, id ASC LIMIT ?"
		allArgs = append(allArgs, limit)
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func DefaultCountBuilder(filters ...nostr.Filter) ([]Query, error) {
	switch len(filters) {
	case 0:
		return nil, nastro.ErrEmptyFilters

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

		query := "SELECT (" + strings.Join(subQueries, " + ") + ")"
		return []Query{{SQL: query, Args: allArgs}}, nil
	}
}

func buildQuery(filter nostr.Filter) (string, []any) {
	conditions, args := sqlConditions(filter)
	query := "SELECT * FROM events AS e" + " WHERE " + strings.Join(conditions, " AND ")
	return query, args
}

func buildCount(filter nostr.Filter) (string, []any) {
	conditions, args := sqlConditions(filter)
	query := "SELECT COUNT(*) FROM events AS e" + " WHERE " + strings.Join(conditions, " AND ")
	return query, args
}

func sqlConditions(filter nostr.Filter) (conditions []string, args []any) {
	if len(filter.IDs) > 0 {
		conditions = append(conditions, "e.id IN "+ValueList(len(filter.IDs)))
		for _, ID := range filter.IDs {
			args = append(args, ID)
		}
	}

	if len(filter.Kinds) > 0 {
		conditions = append(conditions, "e.kind IN "+ValueList(len(filter.Kinds)))
		for _, kind := range filter.Kinds {
			args = append(args, kind)
		}
	}

	if len(filter.Authors) > 0 {
		conditions = append(conditions, "e.pubkey IN "+ValueList(len(filter.Authors)))
		for _, author := range filter.Authors {
			args = append(args, author)
		}
	}

	if filter.Until != nil {
		conditions = append(conditions, "e.created_at <= ?")
		args = append(args, filter.Until.Time().Unix())
	}

	if filter.Since != nil {
		conditions = append(conditions, "e.created_at >= ?")
		args = append(args, filter.Since.Time().Unix())
	}

	if len(filter.Tags) > 0 {
		tagCond := make([]string, 0, len(filter.Tags))
		for key, vals := range filter.Tags {
			if len(vals) == 0 {
				continue
			}

			tagCond = append(tagCond, "(t.key = ? AND t.value IN "+ValueList(len(vals))+")")
			args = append(args, key)
			for _, val := range vals {
				args = append(args, val)
			}
		}

		if len(tagCond) > 0 {
			conditions = append(conditions,
				"EXISTS (SELECT 1 FROM event_tags AS t "+
					"WHERE t.event_id = e.id "+
					"AND ("+strings.Join(tagCond, " OR ")+")"+
					")",
			)
		}
	}
	return conditions, args
}

// query = "SELECT id, pubkey, created_at, kind, tags, content, sig FROM events WHERE " +
// strings.Join(conditions, " AND ") + " ORDER BY created_at DESC, id LIMIT ?"
// args = append(args, filter.Limit)

// ValueList for sqlite queries. e.g. ValueList(4) = "(?,?,?,?)".
// It panics if n < 1.
func ValueList(n int) string {
	return "(?" + strings.Repeat(",?", n-1) + ")"
}
