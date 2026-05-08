package lan

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadOrCreateNodeIDIsStable(t *testing.T) {
	dir := t.TempDir()
	id1, err := loadOrCreateNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := loadOrCreateNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("node id changed across calls: %q vs %q", id1, id2)
	}
	// And persisted to disk
	b, err := os.ReadFile(filepath.Join(dir, "node_id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Fatal("node_id file is empty")
	}
}

func TestClusterIDDeterministic(t *testing.T) {
	// SHA-256("abc") = ba7816bf8f01cfea... — first 8 bytes hex = "ba7816bf8f01cfea".
	// A hardcoded value catches accidental changes (random salt, hash swap)
	// that a self-comparison would miss.
	const wantABC = "ba7816bf8f01cfea"
	if got := clusterID([]byte("abc")); got != wantABC {
		t.Fatalf("clusterID(\"abc\") = %q, want %q", got, wantABC)
	}
	if clusterID([]byte("abc")) == clusterID([]byte("xyz")) {
		t.Fatal("different inputs collided")
	}
	if clusterID(nil) != "plaintext" {
		t.Fatalf("expected 'plaintext' for nil CA, got %q", clusterID(nil))
	}
}

func TestLoadOrIssueCertRoundTrip(t *testing.T) {
	caPEM, caKeyPEM := testCA(t)
	dir := t.TempDir()
	cert1, err := loadOrIssueCert(dir, caPEM, caKeyPEM, "node-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(cert1.Certificate) == 0 {
		t.Fatal("empty cert chain")
	}

	// Second call should load the same cert from disk, not generate a new one.
	cert2, err := loadOrIssueCert(dir, caPEM, caKeyPEM, "node-A")
	if err != nil {
		t.Fatal(err)
	}
	if string(cert1.Certificate[0]) != string(cert2.Certificate[0]) {
		t.Fatal("cert was regenerated instead of loaded")
	}

	// Build a TLS config from it; verify it embeds the cert and the CA.
	tc, err := buildTLSConfig(cert1, caPEM)
	if err != nil {
		t.Fatal(err)
	}
	if len(tc.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tc.Certificates))
	}
	if tc.RootCAs == nil || tc.ClientCAs == nil {
		t.Fatal("CA pools not populated")
	}
}

// testCA generates a throwaway CA cert + key as PEM, suitable for tests
// that exercise the embedded-CA path without shipping a real one.
func testCA(t *testing.T) (caPEM, caKeyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return caPEM, caKeyPEM
}
