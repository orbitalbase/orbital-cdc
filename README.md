# cdc

`cdc` is an experimental PostgreSQL change data capture library for Go. It reads committed changes from PostgreSQL's logical replication stream and can generate table-specific Go structs and callbacks from a live schema. The workflow is inspired by sqlc: configure the database objects you care about, generate Go code, and connect callbacks in your application.

This is an early `v0.x` project. The public API and supported PostgreSQL types may change between minor releases.

## Requirements

- PostgreSQL with logical replication enabled (`wal_level = logical`). Changing this setting requires a PostgreSQL restart.
- A database user allowed to connect, read the catalogs, and use logical replication. Grant `SELECT` on published tables as required by your PostgreSQL version and hosting provider.
- Go 1.25 or newer.

Create a publication explicitly. This tool does not create or alter publications:

```sql
CREATE PUBLICATION app_cdc_pub FOR TABLE public.users, public.orders;
```

For a publication that covers all tables, use `FOR ALL TABLES` only when that is the intended scope. Configure `max_replication_slots` and `max_wal_senders` for your deployment. A persistent replication slot retains WAL until the consumer acknowledges it, so monitor disk usage and remove slots deliberately when a consumer is retired.

## 1. Install the generator

```sh
go install github.com/mingerz/cdc102/cmd/pgrepl-gen@v0.1.0
```

The module path currently uses the local repository identity. If this repository is published under another GitHub owner or name, update `go.mod`, the generated import path, and these install instructions before tagging a release.

## 2. Initialize configuration and generate types

Point `init` at a development database. It reads PostgreSQL's catalogs and writes a `pgrepl.yaml` listing ordinary tables. It does not write the connection string to the file.

```sh
pgrepl-gen init --dsn "$DATABASE_URL"
```

Disable tables you do not want generated, and keep the publication table list aligned with the tables you intend to consume:

```yaml
version: "1"
database_url_env: CDC_DATABASE_URL
publication: app_cdc_pub
slot: app_cdc_slot
tables:
  - schema: public
    name: users
    enabled: true
  - schema: public
    name: orders
    enabled: true
  - schema: public
    name: internal_logs
    enabled: false
```

Set the runtime database URL and generate Go code:

```sh
export CDC_DATABASE_URL="$DATABASE_URL"
pgrepl-gen generate
```

This writes `internal/cdc/cdc.gen.go`. Commit both `pgrepl.yaml` and the generated file so schema changes are visible in review. Run `pgrepl-gen generate` after changing the database schema or enabled table list.

Common PostgreSQL types such as integer, bigint, text, boolean, timestamps, and bytea receive Go types directly. Other types are emitted as `any`; the generated code does not guess a potentially lossy Go representation. Nullable supported fields are pointers. Generated rows include `CDCFieldsPresent`, a map that distinguishes an omitted replica identity field from a SQL `NULL` value. At runtime, known PostgreSQL OIDs are decoded through pgx's `pgtype` codecs.

## 3. Handle typed events in your application

Implement the generated `Handler` interface for the tables enabled in your configuration, then pass its `Dispatcher` to the runtime:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mingerz/cdc102"
	generated "my-app/internal/cdc"
)

type handler struct{}

func (handler) OnUsersInsert(ctx context.Context, row generated.UsersRow) error {
	log.Printf("new user: %s", row.Email)
	return nil
}

func (handler) OnUsersUpdate(ctx context.Context, before, after generated.UsersRow) error {
	log.Printf("user email changed: %s -> %s", before.Email, after.Email)
	return nil
}

func (handler) OnUsersDelete(ctx context.Context, before generated.UsersRow) error {
	log.Printf("deleted user: %s", before.Email)
	return nil
}

func (handler) OnUsersTruncate(ctx context.Context) error { return nil }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config := generated.RuntimeConfig(os.Getenv("CDC_DATABASE_URL"))
	err := cdc.Run(ctx, config, generated.Dispatcher{Handler: handler{}})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
```

The example assumes the enabled table is named `public.users`, so the generator emits a `UsersRow` type and the callbacks shown above. Callback names and fields follow the actual configured schema.

## Runtime behavior and current limits

- `Run` consumes PostgreSQL `pgoutput` protocol version 1. It creates a persistent logical replication slot if it does not exist; it does not remove the slot when the process exits.
- Events are buffered until PostgreSQL reports a transaction commit. The handler is called for each event in that committed transaction. The slot is acknowledged only after every handler call succeeds.
- Delivery is at least once. A process failure after some callbacks run but before the commit is acknowledged can replay those events. Make side effects idempotent, or persist an application checkpoint with the side effect.
- `Run` returns on connection, decode, or handler errors. The application owns restart/backoff policy; automatic reconnect is not implemented yet.
- Large in-progress transactions are buffered in memory. Streaming protocol v2 is not supported yet.
- Update and delete old rows contain only the replica identity data PostgreSQL sends. With the default replica identity, that is commonly the key columns, not a full row. Unchanged TOAST values are represented by `cdc.UnchangedToast` in the generic event; generated row conversion leaves those fields at their zero value and does not mark them present.
- The generator emits `any` for types outside its built-in mapping. PostgreSQL custom types, domains, enums, arrays, and extensions need application-specific handling before relying on a concrete Go type.
- The `enabled` flag controls generation and dispatch. It does not alter the database publication. Events from disabled tables are ignored by the generated dispatcher.

## Versioning and releases

The library starts at `v0.1.0` while the API is experimental. Releases use Go module semantic version tags (`v0.x.y`); use `v1.0.0` once the event and delivery contracts are stable. Breaking releases after v1 require the Go module major-version suffix, such as `/v2`. Push a `v*.*.*` tag to create a GitHub release; the Go module is then fetched directly from the tagged repository by `go install` or `go get`.

The GitHub Actions workflows run formatting, vet, and Go package checks on pushes and pull requests. The release workflow creates a GitHub release from a pushed version tag.
