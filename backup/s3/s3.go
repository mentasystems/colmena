// Package s3 provides an S3-compatible backup backend for Colmena with zero
// external dependencies (AWS Signature V4 over net/http).
//
// Works with any S3-compatible storage: OVH Object Storage, AWS S3, MinIO,
// Cloudflare R2, Backblaze B2, etc. Objects are addressed virtual-hosted
// style: https://<bucket>.<endpoint-host>/<prefix>/…
//
//	backend, err := s3.NewBackend(s3.Config{
//	    Endpoint:  "https://s3.gra.io.cloud.ovh.net",
//	    Bucket:    "myapp-backups",
//	    Prefix:    "myapp/default",
//	    Region:    "gra",
//	    AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
//	    SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
//	})
package s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/mentasystems/colmena"
)

// Config holds the S3 backend configuration.
type Config struct {
	// Endpoint is the base S3 endpoint, e.g. "https://s3.gra.io.cloud.ovh.net".
	// A bare host is accepted and treated as https.
	Endpoint string
	// Bucket is the bucket name (addressed virtual-hosted style).
	Bucket string
	// Prefix is the key prefix for all objects of this backend. Use one
	// prefix per database, e.g. "myapp/default".
	Prefix string
	// Region is the SigV4 region (e.g. "gra", "us-east-1", "auto").
	Region string
	// AccessKey / SecretKey are the S3 credentials.
	AccessKey string
	SecretKey string
	// HTTPClient overrides the default client (30s timeout) when set.
	HTTPClient *http.Client
}

// Backend implements colmena.BackupBackend over S3.
type Backend struct {
	host   string // virtual-hosted: <bucket>.<endpoint-host>
	prefix string // normalized, no leading/trailing slash ("" allowed)
	region string
	access string
	secret string
	http   *http.Client
}

// NewBackend validates cfg and returns a ready backend.
func NewBackend(cfg Config) (*Backend, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("s3: Endpoint, Bucket, AccessKey and SecretKey are required")
	}
	ep := cfg.Endpoint
	if !strings.Contains(ep, "://") {
		ep = "https://" + ep
	}
	u, err := url.Parse(ep)
	if err != nil {
		return nil, fmt.Errorf("s3: bad endpoint: %w", err)
	}
	region := cfg.Region
	if region == "" {
		region = "auto"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &Backend{
		host:   cfg.Bucket + "." + u.Host,
		prefix: strings.Trim(cfg.Prefix, "/"),
		region: region,
		access: cfg.AccessKey,
		secret: cfg.SecretKey,
		http:   client,
	}, nil
}

func (b *Backend) key(parts ...string) string {
	all := append([]string{}, parts...)
	if b.prefix != "" {
		all = append([]string{b.prefix}, all...)
	}
	return strings.Join(all, "/")
}

// ── colmena.BackupBackend ───────────────────────────────────────────────────

func (b *Backend) WriteSnapshot(ctx context.Context, generation string, r io.Reader, size int64) error {
	return b.put(ctx, b.key("generations", generation, "snapshot.db.gz"), r, size)
}

func (b *Backend) WriteWALSegment(ctx context.Context, generation string, seg colmena.WALSegmentInfo, r io.Reader, size int64) error {
	return b.put(ctx, b.key("generations", generation, "wal", segmentName(seg)), r, size)
}

func (b *Backend) Generations(ctx context.Context) ([]colmena.Generation, error) {
	// Generation ids are directly under generations/ — list with delimiter.
	prefix := b.key("generations") + "/"
	dirs, _, err := b.listAll(ctx, prefix, "/")
	if err != nil {
		return nil, err
	}
	var out []colmena.Generation
	for _, d := range dirs {
		id := strings.TrimSuffix(strings.TrimPrefix(d, prefix), "/")
		ts, err := colmena.ParseGenerationID(id)
		if err != nil {
			continue // foreign key
		}
		out = append(out, colmena.Generation{ID: id, CreatedAt: ts})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (b *Backend) WALSegments(ctx context.Context, generation string) ([]colmena.WALSegmentInfo, error) {
	prefix := b.key("generations", generation, "wal") + "/"
	_, objs, err := b.listAll(ctx, prefix, "")
	if err != nil {
		return nil, err
	}
	var out []colmena.WALSegmentInfo
	for _, o := range objs {
		seg, err := parseSegmentName(strings.TrimPrefix(o.Key, prefix))
		if err != nil {
			continue
		}
		seg.Size = o.Size
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

func (b *Backend) ReadSnapshot(ctx context.Context, generation string) (io.ReadCloser, error) {
	return b.get(ctx, b.key("generations", generation, "snapshot.db.gz"))
}

func (b *Backend) ReadWALSegment(ctx context.Context, generation string, seg colmena.WALSegmentInfo) (io.ReadCloser, error) {
	return b.get(ctx, b.key("generations", generation, "wal", segmentName(seg)))
}

func (b *Backend) DeleteGeneration(ctx context.Context, generation string) error {
	prefix := b.key("generations", generation) + "/"
	_, objs, err := b.listAll(ctx, prefix, "")
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := b.delete(ctx, o.Key); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) Close() error { return nil }

// segmentName mirrors colmena's canonical segment naming.
func segmentName(seg colmena.WALSegmentInfo) string {
	return fmt.Sprintf("%016x-%016x-%016x.seg.gz", seg.Index, seg.Offset, seg.CreatedAt.UnixNano())
}

func parseSegmentName(name string) (colmena.WALSegmentInfo, error) {
	base, ok := strings.CutSuffix(name, ".seg.gz")
	if !ok {
		return colmena.WALSegmentInfo{}, fmt.Errorf("s3: bad segment name %q", name)
	}
	parts := strings.Split(base, "-")
	if len(parts) != 3 {
		return colmena.WALSegmentInfo{}, fmt.Errorf("s3: bad segment name %q", name)
	}
	var idx, off, ts int64
	if _, err := fmt.Sscanf(parts[0]+" "+parts[1]+" "+parts[2], "%x %x %x", &idx, &off, &ts); err != nil {
		return colmena.WALSegmentInfo{}, fmt.Errorf("s3: bad segment name %q", name)
	}
	return colmena.WALSegmentInfo{Index: idx, Offset: off, CreatedAt: time.Unix(0, ts)}, nil
}

// ── HTTP + SigV4 ────────────────────────────────────────────────────────────

const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // sha256("")

func (b *Backend) put(ctx context.Context, key string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://"+b.host+"/"+escapeKey(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	b.sign(req, hexSHA256(data))
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // safe-ignore: drain for reuse
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("s3: put %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (b *Backend) get(ctx context.Context, key string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+b.host+"/"+escapeKey(key), nil)
	if err != nil {
		return nil, err
	}
	b.sign(req, emptyPayloadHash)
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("s3: get %s: status %d", key, resp.StatusCode)
	}
	return resp.Body, nil
}

func (b *Backend) delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "https://"+b.host+"/"+escapeKey(key), nil)
	if err != nil {
		return err
	}
	b.sign(req, emptyPayloadHash)
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // safe-ignore: drain for reuse
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("s3: delete %s: status %d", key, resp.StatusCode)
	}
	return nil
}

type listedObject struct {
	Key  string
	Size int64
}

// listAll pages through ListObjectsV2. With delimiter "/" it returns common
// prefixes ("directories") plus objects; with "" only objects.
func (b *Backend) listAll(ctx context.Context, prefix, delimiter string) (dirs []string, objs []listedObject, err error) {
	token := ""
	for {
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("prefix", prefix)
		if delimiter != "" {
			q.Set("delimiter", delimiter)
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+b.host+"/?"+canonicalQuery(q), nil)
		if err != nil {
			return nil, nil, err
		}
		b.sign(req, emptyPayloadHash)
		resp, err := b.http.Do(req)
		if err != nil {
			return nil, nil, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, nil, fmt.Errorf("s3: list %s: status %d: %s", prefix, resp.StatusCode, truncate(string(body), 200))
		}
		var parsed struct {
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
			Contents              []struct {
				Key  string `xml:"Key"`
				Size int64  `xml:"Size"`
			} `xml:"Contents"`
			CommonPrefixes []struct {
				Prefix string `xml:"Prefix"`
			} `xml:"CommonPrefixes"`
		}
		if err := xml.Unmarshal(body, &parsed); err != nil {
			return nil, nil, fmt.Errorf("s3: list decode: %w", err)
		}
		for _, p := range parsed.CommonPrefixes {
			dirs = append(dirs, p.Prefix)
		}
		for _, o := range parsed.Contents {
			objs = append(objs, listedObject{Key: o.Key, Size: o.Size})
		}
		if !parsed.IsTruncated || parsed.NextContinuationToken == "" {
			return dirs, objs, nil
		}
		token = parsed.NextContinuationToken
	}
}

// sign adds the AWS SigV4 Authorization header (service "s3").
func (b *Backend) sign(req *http.Request, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	canon := map[string]string{
		"host":                 b.host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		canon["content-type"] = ct
	}
	names := make([]string, 0, len(canon))
	for k := range canon {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k + ":" + canon[k] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + b.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	k := hmacSHA256([]byte("AWS4"+b.secret), dateStamp)
	k = hmacSHA256(k, b.region)
	k = hmacSHA256(k, "s3")
	k = hmacSHA256(k, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(k, stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+b.access+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

// canonicalQuery encodes query params the way SigV4 canonicalizes them
// (sorted keys, strict percent-encoding), and is used as the actual RawQuery
// so the signature always matches what is sent.
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		for _, v := range q[k] {
			parts = append(parts, sigEscape(k)+"="+sigEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// sigEscape percent-encodes per RFC 3986 (SigV4 rules: unreserved = A-Z a-z
// 0-9 - _ . ~).
func sigEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// escapeKey escapes each path segment, keeping "/" separators.
func escapeKey(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = sigEscape(s)
	}
	return strings.Join(segs, "/")
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
