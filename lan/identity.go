package lan

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// loadOrCreateNodeID returns the persistent NodeID for this data
// directory, generating one on first call. The ID is a random UUID, not
// derived from hardware: that way reflashing the same Pi yields a fresh
// member instead of one that "looks like" the old one to Raft (which
// would refuse to recover with an empty log under the old ID).
func loadOrCreateNodeID(dataDir string) (string, error) {
	p := filepath.Join(dataDir, "node_id")
	if b, err := os.ReadFile(p); err == nil {
		s := strings.TrimSpace(string(b))
		if s != "" {
			return s, nil
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", fmt.Errorf("lan: create data dir: %w", err)
	}
	id := uuid.NewString()
	if err := os.WriteFile(p, []byte(id+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("lan: persist node_id: %w", err)
	}
	return id, nil
}

// clusterID derives a short, deterministic identifier from the embedded
// CA cert. Used to discriminate mDNS announcements so two different
// clusters on the same LAN never merge: same CA → same clusterID →
// peers find each other; different CA → invisible to each other.
func clusterID(caPEM []byte) string {
	if len(caPEM) == 0 {
		return "plaintext"
	}
	sum := sha256.Sum256(caPEM)
	return hex.EncodeToString(sum[:8])
}

// loadOrIssueCert loads a previously-issued leaf certificate from the
// data directory or, on first boot, generates a new ECDSA key pair and
// issues a leaf cert signed by the embedded CA. The resulting cert
// carries SANs for every non-loopback IP on the host plus 127.0.0.1
// and ::1, so peers can dial this node by any of its addresses without
// hitting a SAN mismatch.
func loadOrIssueCert(dataDir string, caPEM, caKeyPEM []byte, nodeID string) (tls.Certificate, error) {
	crtPath := filepath.Join(dataDir, "node.crt")
	keyPath := filepath.Join(dataDir, "node.key")
	if c, err := os.ReadFile(crtPath); err == nil {
		if k, err := os.ReadFile(keyPath); err == nil {
			if cert, err := tls.X509KeyPair(c, k); err == nil {
				return cert, nil
			}
		}
	}

	caCert, caKey, err := parseCAPair(caPEM, caKeyPEM)
	if err != nil {
		return tls.Certificate{}, err
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: gen leaf key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: gen serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"colmena", nodeID, "localhost"},
		IPAddresses:  hostIPs(),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: sign leaf cert: %w", err)
	}

	crtPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: marshal leaf key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	if err := os.WriteFile(crtPath, crtPEM, 0o644); err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: write node.crt: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return tls.Certificate{}, fmt.Errorf("lan: write node.key: %w", err)
	}
	return tls.X509KeyPair(crtPEM, keyPEM)
}

// buildTLSConfig returns the *tls.Config to hand to colmena.Config.TLSConfig.
// Both sides of every Colmena connection (Raft transport on Bind, RPC on
// Bind+1, dial-out from the RPC pool) will use this — symmetric mTLS.
func buildTLSConfig(cert tls.Certificate, caPEM []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("lan: invalid CA PEM")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ServerName:   "colmena",
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func parseCAPair(caPEM, caKeyPEM []byte) (*x509.Certificate, any, error) {
	blk, _ := pem.Decode(caPEM)
	if blk == nil {
		return nil, nil, fmt.Errorf("lan: invalid CA cert PEM")
	}
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("lan: parse CA cert: %w", err)
	}
	blk, _ = pem.Decode(caKeyPEM)
	if blk == nil {
		return nil, nil, fmt.Errorf("lan: invalid CA key PEM")
	}
	key, err := parsePrivateKey(blk.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func parsePrivateKey(der []byte) (any, error) {
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParseECPrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return k, nil
	}
	return nil, fmt.Errorf("lan: unsupported CA key format (want PKCS#8, EC, or PKCS#1)")
}

func hostIPs() []net.IP {
	out := []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP("::1")}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ipnet.IP.IsLoopback() || ipnet.IP.IsMulticast() || ipnet.IP.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ipnet.IP)
	}
	return out
}

// firstNonLoopbackIPv4 returns the host's first routable IPv4 address,
// or 127.0.0.1 if none is available. Used to fill in Advertise when the
// caller didn't provide one.
func firstNonLoopbackIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if v4 := ipnet.IP.To4(); v4 != nil {
			return v4.String()
		}
	}
	return "127.0.0.1"
}
