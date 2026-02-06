# presto-go

A Go client library for [Presto](https://prestodb.io/) and [Trino](https://trino.io/) SQL query engines.

## Features

- **`database/sql` driver** — use the standard Go database API (`sql.Open`, `db.Query`, `rows.Scan`)
- Full Presto/Trino REST API support (query, fetch, cancel)
- Trino compatibility mode (automatic header translation)
- Session management with isolated, cloneable sessions
- Transaction state tracking (automatic via response headers)
- Batch result streaming with memory-efficient `Drain` API
- Automatic retry with exponential backoff on 503 responses and transient connection errors
- Gzip request/response compression
- Thread-safe concurrent session access
- Fluent API for session configuration
- Pre-minted query ID support

## Installation

```bash
go get github.com/ethanyzhang/presto-go
```

## Quick Start

### Using `database/sql` (recommended)

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/ethanyzhang/presto-go" // registers "presto" driver
)

func main() {
	db, err := sql.Open("presto", "presto://localhost:8080/hive/default")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, name FROM users WHERE active = ?", true)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
}
```

### Using the low-level API

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ethanyzhang/presto-go"
)

func main() {
	client, err := presto.NewClient("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	session := client.NewSession()
	session.Catalog("hive").Schema("default").User("analyst")

	ctx := context.Background()
	results, _, err := session.Query(ctx, "SELECT id, name FROM users LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}

	err = results.Drain(ctx, func(qr *presto.QueryResults) error {
		for _, row := range qr.Data {
			var parsed []any
			json.Unmarshal(row, &parsed)
			fmt.Println(parsed)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

## Usage

### `database/sql` Driver

#### DSN Format

```
presto://[user[:password]@]host[:port][/catalog[/schema]][?key=value&...]
trino://...   (enables Trino header mode)
```

Default port is 8080 for both schemes. Query parameters:

| Parameter | Description |
|-----------|-------------|
| `timezone` | Session time zone |
| `client_tags` | Comma-separated tags |
| `client_info` | Client info string |
| `source` | Query source identifier |
| *(other)* | Set as session properties |

#### Using `sql.OpenDB` with a Connector

```go
connector, err := presto.NewConnector("presto://user@host:8080/hive/default")
if err != nil {
    log.Fatal(err)
}
db := sql.OpenDB(connector)
```

#### Parameter Interpolation

The driver interpolates `?` placeholders client-side into SQL literals:

```go
rows, err := db.Query("SELECT * FROM t WHERE name = ? AND id = ?", "alice", 42)
```

#### Transactions

```go
tx, err := db.BeginTx(ctx, nil)
// ... use tx.Query / tx.Exec ...
tx.Commit() // or tx.Rollback()
```

### Client Initialization

```go
// Basic client
client, err := presto.NewClient("http://presto-coordinator:8080")

// With basic auth
client, err := presto.NewClient("http://presto-coordinator:8080", "base64-encoded-credentials")

// Trino mode with HTTPS
client, err := presto.NewClient("http://trino-coordinator:8443")
client.IsTrino(true).ForceHTTPS(true)
```

### Session Management

Sessions provide isolated execution contexts. Each session maintains its own catalog, schema, user identity, transaction state, and session parameters.

```go
// Create an isolated session from the client
session := client.NewSession()
session.Catalog("hive").Schema("production").User("etl_user")

// Set session parameters
session.SessionParam("query_max_memory", "2GB")
session.SessionParam("join_distribution_type", "PARTITIONED")

// Clone a session for parallel workloads
s2 := session.Clone()
s2.Schema("staging")
```

### Query Execution

```go
// Simple query
results, _, err := session.Query(ctx, "SELECT * FROM orders WHERE status = 'pending'")

// Query with pre-minted ID (for tracking)
results, _, err := session.QueryWithPreMintedID(ctx, "SELECT 1", "custom-query-id", "slug")

// Manual batch fetching
for results.HasMoreBatch() {
    err := results.FetchNextBatch(ctx)
    if err != nil {
        log.Fatal(err)
    }
    // Process results.Data
}

// Streaming drain (memory-efficient for large result sets)
err = results.Drain(ctx, func(qr *presto.QueryResults) error {
    // Process each batch; data is cleared after handler returns
    fmt.Printf("Batch: %d rows\n", len(qr.Data))
    return nil
})
```

### Cancellation

Context cancellation automatically triggers server-side query cleanup:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

results, _, err := session.Query(ctx, "SELECT * FROM large_table")
// If context times out during FetchNextBatch, the query is canceled on the server
```

### Request Options

Override individual request settings without modifying the session:

```go
opt := func(r *http.Request) {
    r.Header.Set("X-Custom-Header", "value")
}
results, _, err := session.Query(ctx, "SELECT 1", opt)
```

## Testing

```bash
go test ./... -v
```

### Mock Server

The `prestotest` package provides a `MockPrestoServer` for integration testing. It uses only the standard library (`net/http`), so it introduces no additional dependencies.

```go
import (
    "github.com/ethanyzhang/presto-go"
    "github.com/ethanyzhang/presto-go/prestotest"
)

func TestMyApp(t *testing.T) {
    mock := prestotest.NewMockPrestoServer()
    defer mock.Close()

    mock.AddQuery(&prestotest.MockQueryTemplate{
        SQL:         "SELECT * FROM users",
        Columns:     []presto.Column{{Name: "id", Type: "bigint"}},
        Data:        [][]any{{1}, {2}, {3}},
        DataBatches: 2,
    })

    client, _ := presto.NewClient(mock.URL())
    session := client.NewSession()

    results, _, err := session.Query(context.Background(), "SELECT * FROM users")
    // Assert on results...
}
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
