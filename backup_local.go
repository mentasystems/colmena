package colmena

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// LocalBackend stores backups on the local filesystem. Layout:
//
//	<dir>/<generation>/snapshot.db.gz
//	<dir>/<generation>/wal/<index>-<offset>-<ts>.seg.gz
//
// Useful for tests and for replicating to a mounted volume; production
// deployments normally use the S3 backend (backup/s3).
type LocalBackend struct {
	dir string
}

// NewLocalBackend creates (if needed) and wraps a backup directory.
func NewLocalBackend(dir string) (*LocalBackend, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("colmena: local backend: %w", err)
	}
	return &LocalBackend{dir: dir}, nil
}

func (b *LocalBackend) genDir(generation string) string {
	return filepath.Join(b.dir, generation)
}

func (b *LocalBackend) WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error {
	dir := b.genDir(generation)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, "snapshot.db.gz"), r)
}

func (b *LocalBackend) WriteWALSegment(ctx context.Context, generation string, seg WALSegmentInfo, r io.Reader, size int64) error {
	dir := filepath.Join(b.genDir(generation), "wal")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, seg.segmentKey()), r)
}

func (b *LocalBackend) Generations(ctx context.Context) ([]Generation, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
	var out []Generation
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, err := ParseGenerationID(e.Name())
		if err != nil {
			continue // foreign dir
		}
		out = append(out, Generation{ID: e.Name(), CreatedAt: ts})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (b *LocalBackend) WALSegments(ctx context.Context, generation string) ([]WALSegmentInfo, error) {
	dir := filepath.Join(b.genDir(generation), "wal")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []WALSegmentInfo
	for _, e := range entries {
		seg, err := parseSegmentKey(e.Name())
		if err != nil {
			continue
		}
		if info, err := e.Info(); err == nil {
			seg.Size = info.Size()
		}
		out = append(out, seg)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].Offset < out[j].Offset
	})
	return out, nil
}

func (b *LocalBackend) ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(b.genDir(generation), "snapshot.db.gz"))
}

func (b *LocalBackend) ReadWALSegment(ctx context.Context, generation string, seg WALSegmentInfo) (io.ReadCloser, error) {
	return os.Open(filepath.Join(b.genDir(generation), "wal", seg.segmentKey()))
}

func (b *LocalBackend) DeleteGeneration(ctx context.Context, generation string) error {
	return os.RemoveAll(b.genDir(generation))
}

func (b *LocalBackend) Close() error { return nil }

// atomicWrite writes r to path via a temp file + rename so readers never see
// a partial object.
func atomicWrite(path string, r io.Reader) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
