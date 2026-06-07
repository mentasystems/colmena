package fly

import (
	"context"
	"errors"
	"io"
	"log"
	"net/netip"
	"testing"
	"time"

	"github.com/mentasystems/colmena/cluster"
)

// fakeResolver is a map-driven resolver for discovery tests.
type fakeResolver struct {
	aaaa    map[string][]netip.Addr
	txt     map[string][]string
	aaaaErr error
	txtErr  error
}

func (f *fakeResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	if f.aaaaErr != nil {
		return nil, f.aaaaErr
	}
	a, ok := f.aaaa[host]
	if !ok {
		return nil, errors.New("no such host: " + host)
	}
	return a, nil
}

func (f *fakeResolver) LookupTXT(ctx context.Context, name string) ([]string, error) {
	if f.txtErr != nil {
		return nil, f.txtErr
	}
	return f.txt[name], nil
}

func testDiscovery(t *testing.T, res resolver) *discovery {
	t.Helper()
	cfg := Config{
		NodeID:            "a",
		PrivateIP:         "fdaa:0:1::a",
		Region:            "mad",
		AppName:           "app",
		RaftPort:          9000,
		PeerTTL:           15 * time.Second,
		DiscoveryInterval: time.Hour, // poll loop must not fire during tests
		Resolver:          res,
		LogOutput:         io.Discard,
	}
	d := newDiscovery(cfg, log.New(io.Discard, "", 0))
	d.self = cluster.Peer{NodeID: "a", Advertise: cfg.advertise()}
	d.ctx = context.Background()
	return d
}

func addr(s string) netip.Addr { return netip.MustParseAddr(s) }

func TestDiscoveryResolvesViaMachineDNS(t *testing.T) {
	res := &fakeResolver{
		txt: map[string][]string{
			"vms.app.internal": {"a mad,b mad,c fra"}, // a=self, b=peer in region, c=other region
		},
		aaaa: map[string][]netip.Addr{
			"b.vm.app.internal": {addr("fdaa:0:1::b")},
		},
	}
	d := testDiscovery(t, res)
	d.refresh()

	peers := d.Peers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer (b), got %d: %+v", len(peers), peers)
	}
	if peers[0].NodeID != "b" {
		t.Fatalf("expected peer id b, got %q", peers[0].NodeID)
	}
	if peers[0].Advertise != "[fdaa:0:1::b]:9000" {
		t.Fatalf("expected bracketed IPv6 advertise, got %q", peers[0].Advertise)
	}
}

func TestDiscoveryFallbackRegionAAAA(t *testing.T) {
	res := &fakeResolver{
		txtErr: errors.New("TXT unavailable"),
		aaaa: map[string][]netip.Addr{
			"mad.app.internal": {addr("fdaa:0:1::a"), addr("fdaa:0:1::b")}, // includes self
		},
	}
	d := testDiscovery(t, res)
	d.refresh()

	peers := d.Peers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer (self excluded), got %d: %+v", len(peers), peers)
	}
	if peers[0].NodeID != "fdaa:0:1::b" {
		t.Fatalf("degraded mode keys by IP, got %q", peers[0].NodeID)
	}
	if peers[0].Advertise != "[fdaa:0:1::b]:9000" {
		t.Fatalf("expected bracketed advertise, got %q", peers[0].Advertise)
	}
}

func TestDiscoveryPeerTTLExpiry(t *testing.T) {
	res := &fakeResolver{
		txt:  map[string][]string{"vms.app.internal": {"a mad,b mad"}},
		aaaa: map[string][]netip.Addr{"b.vm.app.internal": {addr("fdaa:0:1::b")}},
	}
	d := testDiscovery(t, res)

	clock := time.Unix(1_000_000, 0)
	d.now = func() time.Time { return clock }
	d.refresh()
	if len(d.Peers()) != 1 {
		t.Fatalf("expected b present after first refresh")
	}

	// b vanishes from DNS; advance past PeerTTL and refresh again.
	delete(res.txt, "vms.app.internal")
	res.txtErr = errors.New("gone")
	res.aaaaErr = errors.New("gone")
	clock = clock.Add(16 * time.Second)
	d.refresh()

	if got := d.Peers(); len(got) != 0 {
		t.Fatalf("expected b aged out after PeerTTL, got %+v", got)
	}
}

func TestDiscoveryResolverErrorNonFatal(t *testing.T) {
	res := &fakeResolver{txtErr: errors.New("boom"), aaaaErr: errors.New("boom")}
	d := testDiscovery(t, res)
	d.refresh() // must not panic
	if got := d.Peers(); len(got) != 0 {
		t.Fatalf("expected no peers on resolver error, got %+v", got)
	}
}

func TestDiscoverySkipsSelfInRegionTXT(t *testing.T) {
	// Only self in our region → no peers.
	res := &fakeResolver{
		txt: map[string][]string{"vms.app.internal": {"a mad,z fra"}},
	}
	d := testDiscovery(t, res)
	d.refresh()
	if got := d.Peers(); len(got) != 0 {
		t.Fatalf("expected no peers (only self in region), got %+v", got)
	}
}
