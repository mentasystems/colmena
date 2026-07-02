package colmena

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

// The colmena driver adapts database/sql onto the store's two pools: writes
// (and transactions) run on the single writer connection, reads on the
// read-only pool. Unlike v1, transactions are real SQLite transactions —
// queries inside a *sql.Tx see the transaction's own uncommitted writes.

type colmenaConnector struct {
	node   *Node
	dbName string
}

func (c *colmenaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	st, err := c.node.stores.get(c.dbName)
	if err != nil {
		return nil, err
	}
	return &colmenaConn{store: st}, nil
}

func (c *colmenaConnector) Driver() driver.Driver { return &colmenaDriver{} }

type colmenaDriver struct{}

func (d *colmenaDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("colmena: use Node.DB() or Node.OpenDB() instead of sql.Open")
}

type colmenaConn struct {
	store    *store
	activeTx *sql.Tx
}

func (c *colmenaConn) Prepare(query string) (driver.Stmt, error) {
	return &colmenaStmt{conn: c, query: query}, nil
}

func (c *colmenaConn) Close() error {
	if c.activeTx != nil {
		c.activeTx.Rollback()
		c.activeTx = nil
	}
	return nil
}

func (c *colmenaConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *colmenaConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.activeTx != nil {
		return nil, errors.New("colmena: nested transactions not supported")
	}
	tx, err := c.store.writer.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	c.activeTx = tx
	return &colmenaTx{conn: c}, nil
}

func (c *colmenaConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	iArgs := namedToAny(args)
	if c.activeTx != nil {
		return c.activeTx.ExecContext(ctx, query, iArgs...)
	}
	return c.store.writer.ExecContext(ctx, query, iArgs...)
}

func (c *colmenaConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	iArgs := namedToAny(args)
	var rows *sql.Rows
	var err error
	if c.activeTx != nil {
		// Inside a transaction reads must observe its writes: same conn.
		rows, err = c.activeTx.QueryContext(ctx, query, iArgs...)
	} else {
		rows, err = c.store.reader.QueryContext(ctx, query, iArgs...)
	}
	if err != nil {
		return nil, err
	}
	return newWrappedRows(rows)
}

// ── Transaction ─────────────────────────────────────────────────────────────

type colmenaTx struct {
	conn *colmenaConn
}

func (tx *colmenaTx) Commit() error {
	if tx.conn.activeTx == nil {
		return errors.New("colmena: transaction already completed")
	}
	err := tx.conn.activeTx.Commit()
	tx.conn.activeTx = nil
	return err
}

func (tx *colmenaTx) Rollback() error {
	if tx.conn.activeTx == nil {
		return errors.New("colmena: transaction already completed")
	}
	err := tx.conn.activeTx.Rollback()
	tx.conn.activeTx = nil
	return err
}

// ── Statement ───────────────────────────────────────────────────────────────

type colmenaStmt struct {
	conn  *colmenaConn
	query string
}

func (s *colmenaStmt) Close() error  { return nil }
func (s *colmenaStmt) NumInput() int { return -1 }

func (s *colmenaStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, valuesToNamed(args))
}

func (s *colmenaStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, valuesToNamed(args))
}

// ── Rows ────────────────────────────────────────────────────────────────────

type wrappedRows struct {
	sqlRows *sql.Rows
	cols    []string
}

func newWrappedRows(rows *sql.Rows) (*wrappedRows, error) {
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	return &wrappedRows{sqlRows: rows, cols: cols}, nil
}

func (r *wrappedRows) Columns() []string { return r.cols }
func (r *wrappedRows) Close() error      { return r.sqlRows.Close() }

func (r *wrappedRows) Next(dest []driver.Value) error {
	if !r.sqlRows.Next() {
		if err := r.sqlRows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	holders := make([]any, len(dest))
	scanArgs := make([]any, len(dest))
	for i := range holders {
		scanArgs[i] = &holders[i]
	}
	if err := r.sqlRows.Scan(scanArgs...); err != nil {
		return err
	}
	for i, v := range holders {
		dest[i] = v
	}
	return nil
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func namedToAny(args []driver.NamedValue) []any {
	result := make([]any, len(args))
	for i, a := range args {
		result[i] = a.Value
	}
	return result
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
