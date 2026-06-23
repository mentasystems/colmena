// Package s3 provides an S3-compatible backup backend for Colmena.
//
// This backend works with any S3-compatible storage: AWS S3, MinIO,
// Backblaze B2, DigitalOcean Spaces, Cloudflare R2, etc.
//
// Usage:
//
//	backend, err := s3.NewBackend(s3.Config{
//	    Endpoint:  "s3.amazonaws.com",
//	    Bucket:    "my-backups",
//	    Prefix:    "colmena/prod",
//	    Region:    "us-east-1",
//	    AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
//	    SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
//	})
//
//	node, err := colmena.New(colmena.Config{
//	    // ...
//	    Backup: &colmena.BackupConfig{Backend: backend},
//	})
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mentasystems/colmena"
)

// Config holds the S3 backend configuration.
type Config struct {
	// Endpoint is the S3 endpoint (e.g., "s3.amazonaws.com", "minio.local:9000").
	Endpoint string

	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix is an optional key prefix for all backup objects (e.g., "colmena/prod").
	Prefix string

	// Region is the AWS region. Default: "us-east-1".
	Region string

	// AccessKey and SecretKey for authentication.
	AccessKey string
	SecretKey string

	// UsePathStyle forces path-style addressing (required for MinIO and most non-AWS S3).
	UsePathStyle bool

	// UseHTTPS enables HTTPS. Default: true.
	UseHTTPS *bool
}

// Backend stores backups in an S3-compatible object store.
type Backend struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewBackend creates a new S3 backup backend.
func NewBackend(cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("colmena/s3: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	scheme := "https"
	if cfg.UseHTTPS != nil && !*cfg.UseHTTPS {
		scheme = "http"
	}

	endpoint := fmt.Sprintf("%s://%s", scheme, cfg.Endpoint)

	client := s3.New(s3.Options{
		Region:       cfg.Region,
		BaseEndpoint: &endpoint,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.AccessKey, cfg.SecretKey, "",
		),
		UsePathStyle: cfg.UsePathStyle,
	})

	prefix := strings.TrimSuffix(cfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}

	return &Backend{
		client: client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

func (b *Backend) key(parts ...string) string {
	return b.prefix + strings.Join(parts, "/")
}

func (b *Backend) WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	// Write snapshot.
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &b.bucket,
		Key:           aws.String(b.key(generation, "snapshot.db")),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("put snapshot: %w", err)
	}

	// Write metadata.
	meta := colmena.Generation{
		ID:        generation,
		CreatedAt: time.Now().UTC(),
	}
	metaJSON, _ := json.Marshal(meta)
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &b.bucket,
		Key:           aws.String(b.key(generation, "meta.json")),
		Body:          bytes.NewReader(metaJSON),
		ContentLength: aws.Int64(int64(len(metaJSON))),
	})
	return err
}

func (b *Backend) WriteWAL(ctx context.Context, generation string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &b.bucket,
		Key:           aws.String(b.key(generation, "wal.db")),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	return err
}

func (b *Backend) Generations(ctx context.Context) ([]colmena.Generation, error) {
	prefix := b.prefix
	var gens []colmena.Generation

	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: &b.bucket,
		Prefix: &prefix,
	})

	seen := make(map[string]bool)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			key := *obj.Key
			// Look for meta.json files.
			if !strings.HasSuffix(key, "/meta.json") {
				continue
			}
			genID := strings.TrimPrefix(key, b.prefix)
			genID = strings.TrimSuffix(genID, "/meta.json")
			if seen[genID] {
				continue
			}
			seen[genID] = true

			// Read metadata.
			out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &b.bucket,
				Key:    obj.Key,
			})
			if err != nil {
				continue
			}
			var gen colmena.Generation
			json.NewDecoder(out.Body).Decode(&gen)
			out.Body.Close()
			gens = append(gens, gen)
		}
	}

	sort.Slice(gens, func(i, j int) bool {
		return gens[i].CreatedAt.After(gens[j].CreatedAt)
	})
	return gens, nil
}

func (b *Backend) ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucket,
		Key:    aws.String(b.key(generation, "snapshot.db")),
	})
	if err != nil {
		return nil, fmt.Errorf("get snapshot: %w", err)
	}
	return out.Body, nil
}

func (b *Backend) ReadWAL(ctx context.Context, generation string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucket,
		Key:    aws.String(b.key(generation, "wal.db")),
	})
	if err != nil {
		return nil, fmt.Errorf("get WAL: %w", err)
	}
	return out.Body, nil
}

func (b *Backend) Close() error {
	return nil
}
