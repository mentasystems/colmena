# BUG: UPDATE inside `sql.Tx` returns `RowsAffected = 0` even when the row exists and matches

Filed by an animux user (single-node deployment on Fly.io). Reproduced in
production against `colmena v0.8.0`.

## Symptom

An `UPDATE … WHERE …` issued through `tx.ExecContext(...)` returns
`sql.Result.RowsAffected() == 0`, even when:

1. A `SELECT` from the same `*sql.Tx` immediately before the `UPDATE` finds
   the row and reports the WHERE-clause columns hold the expected values.
2. The `UPDATE` reduced to its weakest form (`WHERE pk = ?`, no other
   predicates, no values that would short-circuit) **also** reports
   `RowsAffected = 0`.
3. `tx.Commit()` (or skipping the tx altogether and using `db.ExecContext`
   directly) does apply the change.

The same statement run through `db.ExecContext` (no surrounding
`BeginTx`) returns `RowsAffected = 1` and persists. In other words, the
UPDATE runs, but its row-count appears to be lost when the call goes
through the `*sql.Tx` wrapper.

## Reproduction

Single-node Colmena (Raft leader = self) on the `mentasystems/colmena`
v0.8.0 release. The relevant code path:

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

// Probe — SELECT inside the tx finds the row.
var code string
var redeemedAt sql.NullString
_ = tx.QueryRowContext(ctx,
    `SELECT code, redeemed_at FROM promo_codes WHERE code = ?`, in,
).Scan(&code, &redeemedAt)
// observed: code = in, redeemedAt = {String:"" Valid:false}  (NULL)

// Original UPDATE — should match.
res, _ := tx.ExecContext(ctx,
    `UPDATE promo_codes SET redeemed_by = ?, redeemed_at = ?
     WHERE code = ? AND redeemed_at IS NULL`,
    uid, now, in)
n, _ := res.RowsAffected()         // observed: 0

// Diagnostic UPDATE — only PK predicate, value that wouldn't be a no-op
// for SQLite (writing the same value still bumps page counter, so it
// is NOT short-circuited).
res2, err := tx.ExecContext(ctx,
    `UPDATE promo_codes SET note = note WHERE code = ?`, in)
// observed: err == nil, RowsAffected() == 0
```

Production logs of those four printlns:

```
redeem: uid=58b3824c267ec318de3aabc7cbbe8c69 code="PMC7PPNL37NR" len=12
redeem: probe err=<nil> dbCode="PMC7PPNL37NR" dbRedeemed={ false}
redeem: total promo_codes rows=4
redeem: UPDATE with NULL guard rowsAffected=0
redeem: UPDATE no-op WHERE code=? err=<nil> rowsAffected2=0
```

The same code rewritten without `BeginTx`:

```go
res, err := db.ExecContext(ctx,
    `UPDATE promo_codes SET redeemed_by = ?, redeemed_at = ?
     WHERE code = ? AND redeemed_at IS NULL`, uid, now, in)
// RowsAffected() == 1, write persists.
```

## What I think is happening

`Node.DB()` returns a `*sql.DB` whose driver routes writes through Raft
so the leader's `Apply` actually mutates the SQLite store. When that
write is wrapped in `BeginTx`, one of two things appears to be going on:

1. The `*sql.Tx` runs against a snapshot/connection that the Raft-applied
   write never lands on, so the row count comes back as 0 even though
   the apply path will eventually mutate it.
2. The driver's `Result.RowsAffected()` is unconditionally `0` for any
   write issued through a `Tx` (because the apply happens out-of-band and
   the synchronous return from `ExecContext` has no row count to surface).

Either way the practical effect is that **`tx.ExecContext` for any
non-INSERT write produces a result that the caller cannot trust** — and
the natural Go idiom of guarding race-on-claim with
`UPDATE … WHERE … claimed_at IS NULL` then checking `RowsAffected` is
silently broken.

`INSERT` inside a `BeginTx` works (animux's `CreatePromo` happily
inserts rows that subsequent SELECTs see), so the problem is specific
to UPDATE/DELETE row-count reporting from the `*sql.Tx` path. I haven't
tested DELETE.

## Workaround

Drop `BeginTx` and issue each write as a separate `db.ExecContext`. The
race-on-claim invariant is preserved because the `UPDATE … WHERE …
claimed_at IS NULL` is itself atomic at the SQLite level. You lose
multi-statement atomicity, which for the animux promo redeem flow means
the second UPDATE (granting the user the perk) can in theory fail after
the first UPDATE (consuming the code) succeeds — recoverable manually
but unpleasant.

## Suggested fix / discussion

- If writes inside a `*sql.Tx` legitimately can't return a row count via
  Raft, the driver should at least return a sentinel error
  (`ErrTxRowCountUnavailable` or similar) instead of silently reporting
  `0` — which is indistinguishable from "your WHERE matched nothing"
  and corrupts a very common idiom.
- Better: expose a `node.WriteTransaction(...)` API that batches multiple
  writes into a single Raft Apply, returning per-statement row counts.
  That preserves atomicity AND row-count semantics, at the cost of a
  bespoke API instead of `database/sql`.
- Document the limitation prominently in the README. As-is, a Colmena
  user porting working `*sql.DB` code into `BeginTx` for atomicity will
  hit this, and the failure mode (row count silently `0`) makes the bug
  look like a bad WHERE clause until you instrument it.

## Versions

- Colmena: v0.8.0 (commit `2fc9558`)
- Go: 1.23
- Raft: single-node (leader = self)
- SQLite: whatever Colmena bundles (`mattn/go-sqlite3` per `go.sum`)
- Date: 2026-05-02
