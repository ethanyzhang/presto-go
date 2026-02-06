package presto

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	sql.Register("presto", &prestoDriver{})
}

// --- DSN Parsing ---

// dsnConfig holds the parsed DSN parameters.
type dsnConfig struct {
	host       string
	port       string
	user       string
	password   string
	catalog    string
	schema     string
	isTrino    bool
	timezone   string
	clientTags []string
	clientInfo string
	source     string
	// Unrecognized query params become session properties.
	sessionProps map[string]string
}

// parseDSN parses a Presto/Trino DSN string.
//
// Format: presto://[user[:password]@]host[:port][/catalog[/schema]][?key=value&...]
//
//	trino://...
//
// Query params: timezone, client_tags, client_info, source.
// Unrecognized params become session properties.
func parseDSN(dsn string) (*dsnConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}

	cfg := &dsnConfig{
		sessionProps: make(map[string]string),
	}

	switch u.Scheme {
	case "presto":
		cfg.port = "8080"
	case "trino":
		cfg.isTrino = true
		cfg.port = "8080"
	default:
		return nil, fmt.Errorf("unsupported scheme %q: must be presto or trino", u.Scheme)
	}

	// User info
	if u.User != nil {
		cfg.user = u.User.Username()
		if p, ok := u.User.Password(); ok {
			cfg.password = p
		}
	}

	// Host and port
	cfg.host = u.Hostname()
	if cfg.host == "" {
		return nil, fmt.Errorf("missing host in DSN")
	}
	if p := u.Port(); p != "" {
		cfg.port = p
	}

	// Path: /catalog/schema
	path := strings.TrimPrefix(u.Path, "/")
	if path != "" {
		parts := strings.SplitN(path, "/", 2)
		cfg.catalog = parts[0]
		if len(parts) > 1 {
			cfg.schema = parts[1]
		}
	}

	// Query params
	for key, values := range u.Query() {
		val := values[0]
		switch key {
		case "timezone":
			cfg.timezone = val
		case "client_tags":
			cfg.clientTags = strings.Split(val, ",")
		case "client_info":
			cfg.clientInfo = val
		case "source":
			cfg.source = val
		default:
			cfg.sessionProps[key] = val
		}
	}

	return cfg, nil
}

// serverURL returns the base HTTP URL for the Presto/Trino server.
func (cfg *dsnConfig) serverURL() string {
	return fmt.Sprintf("http://%s:%s", cfg.host, cfg.port)
}

// --- Parameter Interpolation ---

// valueToSQL converts a Go driver.Value to a SQL literal string.
func valueToSQL(v driver.Value) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		if val {
			return "TRUE", nil
		}
		return "FALSE", nil
	case string:
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'", nil
	case []byte:
		return "X'" + hex.EncodeToString(val) + "'", nil
	case time.Time:
		return "TIMESTAMP '" + val.Format("2006-01-02 15:04:05.000") + "'", nil
	default:
		return "", fmt.Errorf("unsupported parameter type: %T", v)
	}
}

// interpolateParams replaces ? placeholders in the query with SQL literals.
// It skips ? characters inside single-quoted string literals.
func interpolateParams(query string, args []driver.Value) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	var buf strings.Builder
	buf.Grow(len(query) + len(args)*8)
	argIdx := 0
	inString := false

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			if inString && i+1 < len(query) && query[i+1] == '\'' {
				// Escaped quote inside string literal
				buf.WriteByte('\'')
				buf.WriteByte('\'')
				i++
				continue
			}
			inString = !inString
			buf.WriteByte(ch)
			continue
		}
		if ch == '?' && !inString {
			if argIdx >= len(args) {
				return "", fmt.Errorf("not enough arguments: query has more placeholders than the %d provided arguments", len(args))
			}
			s, err := valueToSQL(args[argIdx])
			if err != nil {
				return "", err
			}
			buf.WriteString(s)
			argIdx++
			continue
		}
		buf.WriteByte(ch)
	}

	if argIdx != len(args) {
		return "", fmt.Errorf("too many arguments: %d provided but only %d placeholders in query", len(args), argIdx)
	}
	return buf.String(), nil
}

// --- Type Conversion ---

// normalizeType strips parameterized parts from a Presto type string.
// e.g. "varchar(255)" → "varchar", "decimal(10,2)" → "decimal"
func normalizeType(t string) string {
	lower := strings.ToLower(strings.TrimSpace(t))

	// Strip parameterized parts: take everything before '('
	if idx := strings.IndexByte(lower, '('); idx >= 0 {
		return lower[:idx]
	}
	return lower
}

// scanTypeForPrestoType returns the reflect.Type that Scan should use for a given Presto type.
func scanTypeForPrestoType(prestoType string) reflect.Type {
	switch normalizeType(prestoType) {
	case "bigint", "integer", "smallint", "tinyint":
		return reflect.TypeOf(int64(0))
	case "double", "real":
		return reflect.TypeOf(float64(0))
	case "boolean":
		return reflect.TypeOf(false)
	case "varchar", "char", "decimal", "json":
		return reflect.TypeOf("")
	case "varbinary":
		return reflect.TypeOf([]byte(nil))
	case "date", "timestamp", "timestamp with time zone", "time", "time with time zone":
		return reflect.TypeOf(time.Time{})
	default:
		// array, map, row, and unknown types → string (JSON)
		return reflect.TypeOf("")
	}
}

// convertValue converts a JSON-unmarshalled value to the appropriate Go type
// based on the Presto column type.
func convertValue(val any, prestoType string) (driver.Value, error) {
	if val == nil {
		return nil, nil
	}

	norm := normalizeType(prestoType)

	switch norm {
	case "bigint", "integer", "smallint", "tinyint":
		switch v := val.(type) {
		case float64:
			return int64(v), nil
		case json.Number:
			return v.Int64()
		default:
			return nil, fmt.Errorf("cannot convert %T to int64 for type %s", val, prestoType)
		}

	case "double", "real":
		switch v := val.(type) {
		case float64:
			return v, nil
		case json.Number:
			return v.Float64()
		default:
			return nil, fmt.Errorf("cannot convert %T to float64 for type %s", val, prestoType)
		}

	case "boolean":
		if b, ok := val.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("cannot convert %T to bool for type %s", val, prestoType)

	case "varchar", "char":
		if s, ok := val.(string); ok {
			return s, nil
		}
		return fmt.Sprintf("%v", val), nil

	case "decimal":
		// Return as string for precision safety
		switch v := val.(type) {
		case string:
			return v, nil
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		case json.Number:
			return v.String(), nil
		default:
			return fmt.Sprintf("%v", val), nil
		}

	case "date":
		if s, ok := val.(string); ok {
			return time.Parse("2006-01-02", s)
		}
		return nil, fmt.Errorf("cannot convert %T to date", val)

	case "timestamp":
		if s, ok := val.(string); ok {
			return parseTimestamp(s)
		}
		return nil, fmt.Errorf("cannot convert %T to timestamp", val)

	case "timestamp with time zone":
		if s, ok := val.(string); ok {
			return parseTimestampWithTZ(s)
		}
		return nil, fmt.Errorf("cannot convert %T to timestamp with time zone", val)

	case "varbinary":
		if s, ok := val.(string); ok {
			// Presto returns varbinary as base64
			return []byte(s), nil
		}
		return nil, fmt.Errorf("cannot convert %T to varbinary", val)

	default:
		// array, map, row, json, and unknown types → JSON string
		b, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}
}

// parseTimestamp parses a Presto timestamp string (without time zone).
func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05.000000000",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}

// parseTimestampWithTZ parses a Presto "timestamp with time zone" string.
func parseTimestampWithTZ(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.000 MST",
		"2006-01-02 15:04:05.000 -07:00",
		"2006-01-02 15:04:05.000000 MST",
		"2006-01-02 15:04:05.000000 -07:00",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05 -07:00",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp with time zone %q", s)
}

// --- Driver Types ---

// prestoDriver implements driver.Driver and driver.DriverContext.
type prestoDriver struct{}

var _ driver.Driver = (*prestoDriver)(nil)
var _ driver.DriverContext = (*prestoDriver)(nil)

// Open implements driver.Driver. It parses the DSN and returns a new connection.
func (d *prestoDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (d *prestoDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn)
}

// --- Connector ---

// ConnectorOption configures a prestoConnector.
type ConnectorOption func(*prestoConnector)

// WithSessionSetup registers a hook that is called on every new Session created
// by the connector's Connect method. This allows external modules (e.g., Kerberos
// auth) to configure sessions without modifying the core driver.
func WithSessionSetup(fn func(*Session)) ConnectorOption {
	return func(c *prestoConnector) {
		c.sessionSetup = fn
	}
}

// prestoConnector implements driver.Connector. It creates a shared Client
// (via sync.Once) and produces new Sessions for each Connect call.
type prestoConnector struct {
	cfg          *dsnConfig
	client       *Client
	once         sync.Once
	err          error
	sessionSetup func(*Session)
}

var _ driver.Connector = (*prestoConnector)(nil)

// NewConnector creates a new driver.Connector from a DSN string.
// Use this with sql.OpenDB for connection pool management.
func NewConnector(dsn string, opts ...ConnectorOption) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &prestoConnector{cfg: cfg}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Connect implements driver.Connector.
func (c *prestoConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.once.Do(func() {
		c.client, c.err = NewClient(c.cfg.serverURL())
		if c.err != nil {
			return
		}
		c.client.isTrino = c.cfg.isTrino
	})
	if c.err != nil {
		return nil, c.err
	}

	session := c.client.NewSession()

	if c.cfg.user != "" {
		if c.cfg.password != "" {
			session.UserPassword(c.cfg.user, c.cfg.password)
		} else {
			session.User(c.cfg.user)
		}
	}
	if c.cfg.catalog != "" {
		session.Catalog(c.cfg.catalog)
	}
	if c.cfg.schema != "" {
		session.Schema(c.cfg.schema)
	}
	if c.cfg.timezone != "" {
		session.TimeZone(c.cfg.timezone)
	}
	if c.cfg.clientInfo != "" {
		session.ClientInfo(c.cfg.clientInfo)
	}
	if c.cfg.source != "" {
		session.ClientInfo(c.cfg.source)
	}
	if len(c.cfg.clientTags) > 0 {
		session.ClientTags(c.cfg.clientTags...)
	}
	for k, v := range c.cfg.sessionProps {
		session.SessionParam(k, v)
	}

	if c.sessionSetup != nil {
		c.sessionSetup(session)
	}

	return &prestoConn{session: session}, nil
}

// Driver implements driver.Connector.
func (c *prestoConnector) Driver() driver.Driver {
	return &prestoDriver{}
}

// --- Connection ---

// prestoConn implements driver.Conn, driver.QueryerContext, driver.ExecerContext,
// and driver.ConnBeginTx.
type prestoConn struct {
	session *Session
	closed  bool
}

var _ driver.Conn = (*prestoConn)(nil)
var _ driver.QueryerContext = (*prestoConn)(nil)
var _ driver.ExecerContext = (*prestoConn)(nil)
var _ driver.ConnBeginTx = (*prestoConn)(nil)

// Prepare implements driver.Conn.
func (c *prestoConn) Prepare(query string) (driver.Stmt, error) {
	return &prestoStmt{conn: c, query: query}, nil
}

// Close implements driver.Conn.
func (c *prestoConn) Close() error {
	c.closed = true
	return nil
}

// Begin implements driver.Conn. Use BeginTx instead.
func (c *prestoConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx.
func (c *prestoConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != 0 && driver.IsolationLevel(opts.Isolation) != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("presto: isolation level %d is not supported", opts.Isolation)
	}
	if opts.ReadOnly {
		return nil, fmt.Errorf("presto: read-only transactions are not supported")
	}

	_, err := c.execDirect(ctx, "START TRANSACTION")
	if err != nil {
		return nil, fmt.Errorf("presto: failed to start transaction: %w", err)
	}
	return &prestoTx{conn: c}, nil
}

// QueryContext implements driver.QueryerContext.
func (c *prestoConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	positional, err := namedToPositional(args)
	if err != nil {
		return nil, err
	}
	interpolated, err := interpolateParams(query, positional)
	if err != nil {
		return nil, err
	}

	qr, _, err := c.session.Query(ctx, interpolated)
	if err != nil {
		return nil, err
	}

	// Drain through empty batches to get column metadata + first data
	for len(qr.Data) == 0 && qr.HasMoreBatch() {
		if err := qr.FetchNextBatch(ctx); err != nil {
			return nil, err
		}
	}

	return newPrestoRows(qr, ctx)
}

// ExecContext implements driver.ExecerContext.
func (c *prestoConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	positional, err := namedToPositional(args)
	if err != nil {
		return nil, err
	}
	interpolated, err := interpolateParams(query, positional)
	if err != nil {
		return nil, err
	}

	return c.execDirect(ctx, interpolated)
}

// execDirect executes a query and drains all results, returning the final result.
func (c *prestoConn) execDirect(ctx context.Context, query string) (driver.Result, error) {
	qr, _, err := c.session.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	// Drain all batches
	for qr.HasMoreBatch() {
		if err := qr.FetchNextBatch(ctx); err != nil {
			return nil, err
		}
	}

	return &prestoResult{updateCount: qr.UpdateCount}, nil
}

// namedToPositional converts named values to positional driver.Value slice.
func namedToPositional(args []driver.NamedValue) ([]driver.Value, error) {
	positional := make([]driver.Value, len(args))
	for i, arg := range args {
		positional[i] = arg.Value
	}
	return positional, nil
}

// --- Result ---

// prestoResult implements driver.Result.
type prestoResult struct {
	updateCount *int64
}

var _ driver.Result = (*prestoResult)(nil)

// LastInsertId implements driver.Result. Presto does not support auto-increment IDs.
func (r *prestoResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("presto: LastInsertId is not supported")
}

// RowsAffected implements driver.Result.
func (r *prestoResult) RowsAffected() (int64, error) {
	if r.updateCount == nil {
		return 0, nil
	}
	return *r.updateCount, nil
}

// --- Rows ---

// prestoRows implements driver.Rows along with optional column type interfaces.
type prestoRows struct {
	qr      *QueryResults
	ctx     context.Context
	columns []Column
	// Current batch of parsed rows
	rows [][]any
	// Current position within the batch
	pos    int
	closed bool
}

var _ driver.Rows = (*prestoRows)(nil)

// newPrestoRows creates a prestoRows from a QueryResults, parsing the initial data batch.
func newPrestoRows(qr *QueryResults, ctx context.Context) (*prestoRows, error) {
	r := &prestoRows{
		qr:      qr,
		ctx:     ctx,
		columns: qr.Columns,
	}
	if err := r.parseBatch(); err != nil {
		return nil, err
	}
	return r, nil
}

// parseBatch decodes the current qr.Data into r.rows.
func (r *prestoRows) parseBatch() error {
	r.pos = 0
	if len(r.qr.Data) == 0 {
		r.rows = nil
		return nil
	}

	r.rows = make([][]any, len(r.qr.Data))
	for i, raw := range r.qr.Data {
		var row []any
		if err := json.Unmarshal(raw, &row); err != nil {
			return fmt.Errorf("presto: failed to unmarshal row data: %w", err)
		}
		r.rows[i] = row
	}
	return nil
}

// Columns implements driver.Rows.
func (r *prestoRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close implements driver.Rows.
func (r *prestoRows) Close() error {
	r.closed = true
	return nil
}

// Next implements driver.Rows.
func (r *prestoRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	for r.pos >= len(r.rows) {
		// Current batch exhausted; try to fetch the next one
		if !r.qr.HasMoreBatch() {
			return io.EOF
		}
		if err := r.qr.FetchNextBatch(r.ctx); err != nil {
			return err
		}
		if err := r.parseBatch(); err != nil {
			return err
		}
	}

	row := r.rows[r.pos]
	r.pos++

	for i, col := range r.columns {
		if i >= len(row) {
			dest[i] = nil
			continue
		}
		val, err := convertValue(row[i], col.Type)
		if err != nil {
			return err
		}
		dest[i] = val
	}
	return nil
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *prestoRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return strings.ToUpper(normalizeType(r.columns[index].Type))
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *prestoRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return reflect.TypeOf("")
	}
	return scanTypeForPrestoType(r.columns[index].Type)
}

// --- Statement ---

// prestoStmt implements driver.Stmt, driver.StmtQueryContext, and driver.StmtExecContext.
type prestoStmt struct {
	conn  *prestoConn
	query string
}

var _ driver.Stmt = (*prestoStmt)(nil)
var _ driver.StmtQueryContext = (*prestoStmt)(nil)
var _ driver.StmtExecContext = (*prestoStmt)(nil)

// Close implements driver.Stmt.
func (s *prestoStmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt. Returns -1 to disable driver-side validation.
func (s *prestoStmt) NumInput() int {
	return -1
}

// Exec implements driver.Stmt.
func (s *prestoStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

// Query implements driver.Stmt.
func (s *prestoStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

// ExecContext implements driver.StmtExecContext.
func (s *prestoStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

// QueryContext implements driver.StmtQueryContext.
func (s *prestoStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

// namedValues converts positional args to NamedValue slice.
func namedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}

// --- Transaction ---

// prestoTx implements driver.Tx.
type prestoTx struct {
	conn *prestoConn
}

var _ driver.Tx = (*prestoTx)(nil)

// Commit implements driver.Tx.
func (tx *prestoTx) Commit() error {
	_, err := tx.conn.execDirect(context.Background(), "COMMIT")
	return err
}

// Rollback implements driver.Tx.
func (tx *prestoTx) Rollback() error {
	_, err := tx.conn.execDirect(context.Background(), "ROLLBACK")
	return err
}
