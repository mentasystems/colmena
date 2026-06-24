package colmena

import (
	"sync"
	"time"
)

// WriteBatcher accumulates individual write operations and submits them
// as a single batched Raft apply. This amortizes the consensus cost across
// many statements, yielding 10-100x throughput improvement.
type WriteBatcher struct {
	node     *Node
	window   time.Duration
	maxBatch int

	mu      sync.Mutex
	pending []batchEntry
	timer   *time.Timer
	closed  bool
}

type batchEntry struct {
	cmd    *Command
	result chan batchResult
}

type batchResult struct {
	resp *ApplyResult
	err  error
}

func newWriteBatcher(node *Node, window time.Duration, maxBatch int) *WriteBatcher {
	return &WriteBatcher{
		node:     node,
		window:   window,
		maxBatch: maxBatch,
	}
}

// submit enqueues a command for batched execution. The caller blocks until
// the batch containing this command is applied via Raft.
func (b *WriteBatcher) submit(cmd *Command) (*ApplyResult, error) {
	ch := make(chan batchResult, 1)

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrNotLeader
	}

	b.pending = append(b.pending, batchEntry{cmd: cmd, result: ch})
	pendingLen := len(b.pending)

	if pendingLen == 1 {
		// First entry in a new batch — start the window timer.
		b.timer = time.AfterFunc(b.window, b.flush)
	}

	if pendingLen >= b.maxBatch {
		// Batch is full — flush immediately.
		b.stopTimer()
		go b.flush()
	}

	b.mu.Unlock()

	res := <-ch
	return res.resp, res.err
}

// flush collects all pending entries, groups them by target database, and
// applies one merged CommandExecuteMulti per database through Raft.
// Individual results are distributed back to each caller.
//
// Grouping by DB is load-bearing: a Command carries a single DB field and the
// FSM routes the whole command to that one store, so entries for different
// databases must never share a merged command — the other database's
// statements would execute against the wrong SQLite file.
func (b *WriteBatcher) flush() {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	entries := b.pending
	b.pending = nil
	b.stopTimer()
	b.mu.Unlock()

	groups := make(map[string][]batchEntry)
	var order []string // deterministic flush order: first-submitted DB first
	for _, e := range entries {
		db := e.cmd.DB
		if db == "" {
			db = "default" // FSM treats "" as "default"; group them together
		}
		if _, ok := groups[db]; !ok {
			order = append(order, db)
		}
		groups[db] = append(groups[db], e)
	}
	for _, db := range order {
		b.flushGroup(db, groups[db])
	}
}

// flushGroup merges the statements of same-DB entries into one
// CommandExecuteMulti and applies it, demuxing results back to each caller.
func (b *WriteBatcher) flushGroup(db string, entries []batchEntry) {
	// Single-entry batch: apply directly without merging.
	if len(entries) == 1 {
		resp, err := b.applyDirect(entries[0].cmd)
		entries[0].result <- batchResult{resp: resp, err: err}
		return
	}

	// Multi-entry batch: merge all statements and track offsets.
	var allStmts []Statement
	offsets := make([]int, len(entries)) // start index of each entry's statements
	for i, e := range entries {
		offsets[i] = len(allStmts)
		allStmts = append(allStmts, e.cmd.Statements...)
	}

	merged := &Command{
		Type:       CommandExecuteMulti,
		DB:         db,
		Statements: allStmts,
	}

	data, err := marshalCommandVersion(merged, b.node.effectiveCommandVersion())
	if err != nil {
		for _, e := range entries {
			e.result <- batchResult{err: err}
		}
		return
	}

	resp, err := b.node.applyRaft(data)
	if err != nil {
		for _, e := range entries {
			e.result <- batchResult{err: err}
		}
		return
	}

	// Distribute results back to individual callers.
	for i, e := range entries {
		start := offsets[i]
		count := len(e.cmd.Statements)
		entryResults := resp.Results[start : start+count]
		e.result <- batchResult{resp: &ApplyResult{Results: entryResults}}
	}
}

func (b *WriteBatcher) applyDirect(cmd *Command) (*ApplyResult, error) {
	data, err := marshalCommandVersion(cmd, b.node.effectiveCommandVersion())
	if err != nil {
		return nil, err
	}
	return b.node.applyRaft(data)
}

func (b *WriteBatcher) stopTimer() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}

// close drains pending entries with an error and prevents new submissions.
func (b *WriteBatcher) close() {
	b.mu.Lock()
	b.closed = true
	b.stopTimer()
	pending := b.pending
	b.pending = nil
	b.mu.Unlock()

	for _, e := range pending {
		e.result <- batchResult{err: ErrNotLeader}
	}
}
