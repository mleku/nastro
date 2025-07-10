# nastro
A collection of plug&play but highly configurable databases for Nostr.

## Installation

```
go get github.com/pippellia-btc/nastro
```

## Simple Abstraction

All databases fullfil the `Store` interface, which exposes easy to use methods for dealing with everything Nostr.  

```golang
type Store interface {
	Save(ctx context.Context, event *nostr.Event) error

	Delete(ctx context.Context, id string) error

	Replace(ctx context.Context, event *nostr.Event) (bool, error)

	Query(ctx context.Context, filters ...nostr.Filter) ([]nostr.Event, error)

	Count(ctx context.Context, filters ...nostr.Filter) (int64, error)
}
```

## Structural Customization

Fine-tune core parameters and even add custom schemas and query builders with functional options:

```golang
store, err := sqlite.New("relay.sqlite",
    sqlite.WithQueryLimits(limits),
    sqlite.WithAdditionalSchema(schema),
)
```

## Behavioral Customization

You can also use your custom query builder only for specific queries, or writing your own functions
having access the the SQL client.

```golang
rows, err := store.DB.QueryContext(ctx, myQuery, myArgs...)
```

## Requirements for Databases

These are the new requirements for new databases to be added to `nastro`:
- fullfil the `Store` interface.
- apply `QueryLimits` and `WriteLimits` in the `Query`, `Save` and `Replace` methods respectively.
- expose the underlying database client (if any).
- allow customization of core parameters, schemas and query builders with functional options.
