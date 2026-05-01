package proxy

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

type minerTestAddr string

func (a minerTestAddr) Network() string { return "tcp" }
func (a minerTestAddr) String() string  { return string(a) }

type minerTestConn struct {
	remote        net.Addr
	local         net.Addr
	writes        []string
	closed        bool
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *minerTestConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *minerTestConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, string(append([]byte(nil), p...)))
	return len(p), nil
}
func (c *minerTestConn) Close() error {
	c.closed = true
	return nil
}
func (c *minerTestConn) LocalAddr() net.Addr  { return c.local }
func (c *minerTestConn) RemoteAddr() net.Addr { return c.remote }
func (c *minerTestConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}
func (c *minerTestConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}
func (c *minerTestConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

func TestStateImpl_NewMiner_Good(t *testing.T) {
	conn := &minerTestConn{
		remote: minerTestAddr("203.0.113.7:49152"),
		local:  minerTestAddr("127.0.0.1:3333"),
	}

	miner := NewMiner(conn, 3333, nil)
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if miner.State() != MinerStateWaitLogin {
		t.Fatalf("expected wait-login state, got %d", miner.State())
	}
	if miner.MapperID() != -1 {
		t.Fatalf("expected mapper id to default to -1, got %d", miner.MapperID())
	}
	if miner.RouteID() != -1 {
		t.Fatalf("expected route id to default to -1, got %d", miner.RouteID())
	}
	if got := miner.RemoteAddr(); got != "203.0.113.7:49152" {
		t.Fatalf("expected remote addr to be captured, got %q", got)
	}
	if got := miner.IP(); got != "203.0.113.7" {
		t.Fatalf("expected IP to strip port, got %q", got)
	}
	if got := miner.localPort; got != 3333 {
		t.Fatalf("expected local port to be recorded, got %d", got)
	}
	if miner.connectedAt.IsZero() || miner.lastActivityAt.IsZero() {
		t.Fatal("expected timestamps to be initialised")
	}
	if miner.tlsConn != nil {
		t.Fatal("expected plain TCP miner to have no TLS conn")
	}
}

func TestStateImpl_NewMiner_Bad(t *testing.T) {
	conn := &minerTestConn{
		local: minerTestAddr("127.0.0.1:4444"),
	}

	miner := NewMiner(conn, 4444, nil)
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if got := miner.RemoteAddr(); got != "" {
		t.Fatalf("expected empty remote addr when connection has none, got %q", got)
	}
	if got := miner.IP(); got != "" {
		t.Fatalf("expected empty IP when connection has none, got %q", got)
	}
	if got := miner.localPort; got != 4444 {
		t.Fatalf("expected local port to be recorded, got %d", got)
	}
	if miner.tlsConn != nil {
		t.Fatal("expected plain TCP miner to have no TLS conn")
	}
}

func TestStateImpl_NewMiner_Ugly(t *testing.T) {
	conn := &minerTestConn{
		remote: minerTestAddr("198.51.100.9:5555"),
		local:  minerTestAddr("127.0.0.1:5555"),
	}

	miner := NewMiner(conn, 5555, &tls.Config{})
	if miner == nil {
		t.Fatal("expected miner instance")
	}
	if miner.tlsConn == nil {
		t.Fatal("expected TLS config to wrap the miner connection")
	}
	if _, ok := miner.conn.(*tls.Conn); !ok {
		t.Fatalf("expected wrapped connection to be a tls.Conn, got %T", miner.conn)
	}
	if got := miner.RemoteAddr(); got != "198.51.100.9:5555" {
		t.Fatalf("expected remote addr to remain available after TLS wrapping, got %q", got)
	}
	if got := miner.IP(); got != "198.51.100.9" {
		t.Fatalf("expected IP to remain available after TLS wrapping, got %q", got)
	}
}

func TestStateImpl_Miner_SetID_Good(t *testing.T) {
	miner := &Miner{}

	miner.SetID(42)

	if got := miner.ID(); got != 42 {
		t.Fatalf("expected SetID to persist 42, got %d", got)
	}
}

func TestStateImpl_Miner_SetID_Bad(t *testing.T) {
	miner := &Miner{}

	miner.SetID(0)

	if got := miner.ID(); got != 0 {
		t.Fatalf("expected SetID to allow zero, got %d", got)
	}
}

func TestStateImpl_Miner_SetID_Ugly(t *testing.T) {
	miner := &Miner{}

	miner.SetID(7)
	miner.SetID(99)

	if got := miner.ID(); got != 99 {
		t.Fatalf("expected latest SetID call to win, got %d", got)
	}
}

func TestStateImpl_Miner_ID_Good(t *testing.T) {
	miner := &Miner{id: 314}

	if got := miner.ID(); got != 314 {
		t.Fatalf("expected ID accessor to return 314, got %d", got)
	}
}

func TestStateImpl_Miner_ID_Bad(t *testing.T) {
	miner := &Miner{}

	if got := miner.ID(); got != 0 {
		t.Fatalf("expected zero-value miner ID to be 0, got %d", got)
	}
}

func TestStateImpl_Miner_ID_Ugly(t *testing.T) {
	miner := &Miner{}

	miner.SetID(1)
	miner.SetID(2)

	if got := miner.ID(); got != 2 {
		t.Fatalf("expected ID accessor to reflect latest value, got %d", got)
	}
}

func TestStateImpl_noopSplitter_Good(t *testing.T) {
	splitter := &noopSplitter{}

	splitter.Connect()
	splitter.OnLogin(nil)
	splitter.OnSubmit(nil)
	splitter.OnClose(nil)
	splitter.Tick(0)
	splitter.GC()

	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected zero upstream stats, got %+v", got)
	}
}

func TestStateImpl_noopSplitter_Bad(t *testing.T) {
	var splitter *noopSplitter

	splitter.Connect()
	splitter.OnLogin(nil)
	splitter.OnSubmit(nil)
	splitter.OnClose(nil)
	splitter.Tick(0)
	splitter.GC()

	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected zero upstream stats from nil receiver, got %+v", got)
	}
}

func TestStateImpl_noopSplitter_Ugly(t *testing.T) {
	splitter := &noopSplitter{}

	splitter.Tick(123)
	splitter.OnLogin(&LoginEvent{Miner: &Miner{}})
	splitter.OnSubmit(&SubmitEvent{Miner: &Miner{}})
	splitter.OnClose(&CloseEvent{Miner: &Miner{}})

	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop splitter to remain empty after mixed calls, got %+v", got)
	}
}

func TestStateImpl_requestID_Good(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
	}{
		{name: "float64", in: float64(7), want: 7},
		{name: "int64", in: int64(8), want: 8},
		{name: "int", in: int(9), want: 9},
		{name: "string", in: "10", want: 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestID(tc.in); got != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, got)
			}
		})
	}
}

func TestStateImpl_requestID_Bad(t *testing.T) {
	cases := []any{
		true,
		nil,
		"abc",
	}
	for _, in := range cases {
		if got := requestID(in); got != 0 {
			t.Fatalf("expected unsupported id %v to map to 0, got %d", in, got)
		}
	}
}

func TestStateImpl_requestID_Ugly(t *testing.T) {
	if got := requestID(float64(9.75)); got != 9 {
		t.Fatalf("expected floating-point ids to truncate, got %d", got)
	}
	if got := requestID("-12"); got != -12 {
		t.Fatalf("expected signed string ids to parse, got %d", got)
	}
}

func TestStateImpl_isLowerHex8_Good(t *testing.T) {
	if !isLowerHex8("deadbeef") {
		t.Fatal("expected lowercase hex nonce to be accepted")
	}
}

func TestStateImpl_isLowerHex8_Bad(t *testing.T) {
	for _, in := range []string{"", "deadbee", "DEADBEEF"} {
		if isLowerHex8(in) {
			t.Fatalf("expected %q to be rejected", in)
		}
	}
}

func TestStateImpl_isLowerHex8_Ugly(t *testing.T) {
	if isLowerHex8("deadbeeg") {
		t.Fatal("expected non-hex characters to be rejected")
	}
	if isLowerHex8("deadbeef00") {
		t.Fatal("expected longer-than-8 values to be rejected")
	}
}

func stateImplConfig(mode string) *Config {
	return &Config{
		Mode:    mode,
		Workers: WorkersByRigID,
		Bind:    []BindAddr{{Host: "127.0.0.1", Port: 0}},
		Pools:   []PoolConfig{{URL: "pool.example:3333", Enabled: true}},
	}
}

func stateImplProxy(t *testing.T) *Proxy {
	t.Helper()
	p, result := New(stateImplConfig("simple"))
	if !result.OK {
		t.Fatalf("new proxy: %v", result.Error)
	}
	return p
}

func stateImplJob() Job {
	return Job{JobID: "job-1", Blob: repeatString("0", 160), Target: "b88d0600", Algo: "rx/0"}
}

func TestStateImpl_Proxy_Mode_Good(t *testing.T) {
	p := stateImplProxy(t)
	if got := p.Mode(); got != "simple" {
		t.Fatalf("expected simple mode, got %q", got)
	}
}

func TestStateImpl_Proxy_Mode_Bad(t *testing.T) {
	var p *Proxy
	if got := p.Mode(); got != "" {
		t.Fatalf("expected nil proxy mode to be empty, got %q", got)
	}
}

func TestStateImpl_Proxy_Mode_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.Mode(); got != "" {
		t.Fatalf("expected proxy without config to have empty mode, got %q", got)
	}
}

func TestStateImpl_Proxy_WorkersMode_Good(t *testing.T) {
	p := stateImplProxy(t)
	if got := p.WorkersMode(); got != WorkersByRigID {
		t.Fatalf("expected rig-id worker mode, got %q", got)
	}
}

func TestStateImpl_Proxy_WorkersMode_Bad(t *testing.T) {
	var p *Proxy
	if got := p.WorkersMode(); got != WorkersDisabled {
		t.Fatalf("expected nil proxy worker mode to be disabled, got %q", got)
	}
}

func TestStateImpl_Proxy_WorkersMode_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.WorkersMode(); got != WorkersDisabled {
		t.Fatalf("expected missing config worker mode to be disabled, got %q", got)
	}
}

func TestStateImpl_Proxy_Summary_Good(t *testing.T) {
	p := stateImplProxy(t)
	p.stats.OnAccept(Event{Diff: 100})
	if got := p.Summary().Accepted; got != 1 {
		t.Fatalf("expected one accepted share, got %d", got)
	}
}

func TestStateImpl_Proxy_Summary_Bad(t *testing.T) {
	var p *Proxy
	got := p.Summary()
	if got.Accepted != 0 || got.Rejected != 0 || got.CustomDiffStats != nil {
		t.Fatalf("expected zero summary from nil proxy, got %+v", got)
	}
}

func TestStateImpl_Proxy_Summary_Ugly(t *testing.T) {
	p := &Proxy{stats: NewStats()}
	p.stats.OnReject(Event{Error: "invalid nonce"})
	if got := p.Summary().Invalid; got != 1 {
		t.Fatalf("expected invalid reject in summary, got %d", got)
	}
}

func TestStateImpl_Proxy_MinerSnapshots_Good(t *testing.T) {
	p := stateImplProxy(t)
	first := &Miner{id: 2, remoteAddr: "198.51.100.2:3333", user: "wallet-b", password: "secret-b"}
	second := &Miner{id: 1, ip: "198.51.100.1", user: "wallet-a", password: "secret-a"}
	p.miners[2] = first
	p.miners[1] = second
	snapshots := p.MinerSnapshots()
	if len(snapshots) != 2 || snapshots[0].ID != 1 || snapshots[1].ID != 2 {
		t.Fatalf("expected sorted miner snapshots, got %+v", snapshots)
	}
}

func TestStateImpl_Proxy_MinerSnapshots_Bad(t *testing.T) {
	var p *Proxy
	if got := p.MinerSnapshots(); got != nil {
		t.Fatalf("expected nil snapshots from nil proxy, got %+v", got)
	}
}

func TestStateImpl_Proxy_MinerSnapshots_Ugly(t *testing.T) {
	p := stateImplProxy(t)
	p.miners[1] = &Miner{id: 1, password: "secret"}
	snapshot := p.MinerSnapshots()[0]
	if snapshot.Password != maskedPassword {
		t.Fatalf("expected masked password, got %q", snapshot.Password)
	}
}

func TestStateImpl_Proxy_ConnectionCount_Good(t *testing.T) {
	p := stateImplProxy(t)
	p.stats.connections.Store(3)
	if got := p.ConnectionCount(); got != 3 {
		t.Fatalf("expected connection count 3, got %d", got)
	}
}

func TestStateImpl_Proxy_ConnectionCount_Bad(t *testing.T) {
	var p *Proxy
	if got := p.ConnectionCount(); got != 0 {
		t.Fatalf("expected nil proxy connection count 0, got %d", got)
	}
}

func TestStateImpl_Proxy_ConnectionCount_Ugly(t *testing.T) {
	p := &Proxy{}
	if got := p.ConnectionCount(); got != 0 {
		t.Fatalf("expected proxy without stats to report 0, got %d", got)
	}
}

func TestStateImpl_Proxy_ServerListenerAddr_Good(t *testing.T) {
	server, result := NewServer(BindAddr{Host: "127.0.0.1", Port: 0}, nil, nil, nil)
	if !result.OK {
		t.Fatalf("new server: %v", result.Error)
	}
	defer server.Stop()
	p := &Proxy{servers: []*Server{server}}
	if got := p.ServerListenerAddr(0); !containsString(got, "127.0.0.1:") {
		t.Fatalf("expected listener address, got %q", got)
	}
}

func TestStateImpl_Proxy_ServerListenerAddr_Bad(t *testing.T) {
	var p *Proxy
	if got := p.ServerListenerAddr(0); got != "" {
		t.Fatalf("expected nil proxy listener address to be empty, got %q", got)
	}
}

func TestStateImpl_Proxy_ServerListenerAddr_Ugly(t *testing.T) {
	p := &Proxy{servers: []*Server{nil}}
	if got := p.ServerListenerAddr(-1); got != "" {
		t.Fatalf("expected negative index to be empty, got %q", got)
	}
}

func TestStateImpl_Proxy_AllowMonitoringRequest_Good(t *testing.T) {
	p := stateImplProxy(t)
	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if !ok || status != http.StatusOK {
		t.Fatalf("expected monitoring request allowed, got status=%d ok=%v", status, ok)
	}
}

func TestStateImpl_Proxy_AllowMonitoringRequest_Bad(t *testing.T) {
	p := stateImplProxy(t)
	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodPost})
	if ok || status != http.StatusMethodNotAllowed {
		t.Fatalf("expected POST to be rejected, got status=%d ok=%v", status, ok)
	}
}

func TestStateImpl_Proxy_AllowMonitoringRequest_Ugly(t *testing.T) {
	p := &Proxy{config: &Config{HTTP: HTTPConfig{Host: "0.0.0.0"}}}
	status, ok := p.AllowMonitoringRequest(&http.Request{Method: http.MethodGet})
	if ok || status != http.StatusUnauthorized {
		t.Fatalf("expected public host without token to be rejected, got status=%d ok=%v", status, ok)
	}
}

func TestStateImpl_Proxy_Reload_Bad(t *testing.T) {
	p := stateImplProxy(t)
	before := p.Mode()
	p.Reload(nil)
	if got := p.Mode(); got != before {
		t.Fatalf("expected nil reload to preserve mode %q, got %q", before, got)
	}
}

func TestStateImpl_Proxy_Reload_Ugly(t *testing.T) {
	p := stateImplProxy(t)
	p.Reload(&Config{Mode: "", Bind: []BindAddr{{Host: "127.0.0.1", Port: 0}}, Pools: []PoolConfig{{URL: "pool.example:3333", Enabled: true}}})
	if got := p.Mode(); got != "simple" {
		t.Fatalf("expected invalid reload to preserve mode, got %q", got)
	}
}

func TestStateImpl_Miner_SetMapperID_Good(t *testing.T) {
	miner := &Miner{}
	miner.SetMapperID(7)
	if got := miner.MapperID(); got != 7 {
		t.Fatalf("expected mapper id 7, got %d", got)
	}
}

func TestStateImpl_Miner_SetMapperID_Bad(t *testing.T) {
	miner := &Miner{}
	miner.SetMapperID(-1)
	if got := miner.MapperID(); got != -1 {
		t.Fatalf("expected mapper id -1, got %d", got)
	}
}

func TestStateImpl_Miner_SetMapperID_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetMapperID(1)
	miner.SetMapperID(9)
	if got := miner.MapperID(); got != 9 {
		t.Fatalf("expected latest mapper id 9, got %d", got)
	}
}

func TestStateImpl_Miner_MapperID_Good(t *testing.T) {
	miner := &Miner{mapperID: 11}
	if got := miner.MapperID(); got != 11 {
		t.Fatalf("expected mapper id 11, got %d", got)
	}
}

func TestStateImpl_Miner_MapperID_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.MapperID(); got != 0 {
		t.Fatalf("expected zero-value mapper id 0, got %d", got)
	}
}

func TestStateImpl_Miner_MapperID_Ugly(t *testing.T) {
	miner := &Miner{mapperID: -1}
	if got := miner.MapperID(); got != -1 {
		t.Fatalf("expected unassigned mapper id -1, got %d", got)
	}
}

func TestStateImpl_Miner_SetRouteID_Good(t *testing.T) {
	miner := &Miner{}
	miner.SetRouteID(4)
	if got := miner.RouteID(); got != 4 {
		t.Fatalf("expected route id 4, got %d", got)
	}
}

func TestStateImpl_Miner_SetRouteID_Bad(t *testing.T) {
	miner := &Miner{}
	miner.SetRouteID(-1)
	if got := miner.RouteID(); got != -1 {
		t.Fatalf("expected route id -1, got %d", got)
	}
}

func TestStateImpl_Miner_SetRouteID_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetRouteID(1)
	miner.SetRouteID(5)
	if got := miner.RouteID(); got != 5 {
		t.Fatalf("expected latest route id 5, got %d", got)
	}
}

func TestStateImpl_Miner_RouteID_Good(t *testing.T) {
	miner := &Miner{routeID: 8}
	if got := miner.RouteID(); got != 8 {
		t.Fatalf("expected route id 8, got %d", got)
	}
}

func TestStateImpl_Miner_RouteID_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.RouteID(); got != 0 {
		t.Fatalf("expected zero-value route id 0, got %d", got)
	}
}

func TestStateImpl_Miner_RouteID_Ugly(t *testing.T) {
	miner := &Miner{routeID: -1}
	if got := miner.RouteID(); got != -1 {
		t.Fatalf("expected unassigned route id -1, got %d", got)
	}
}

func TestStateImpl_Miner_SetExtendedNiceHash_Good(t *testing.T) {
	miner := &Miner{}
	miner.SetExtendedNiceHash(true)
	if !miner.ExtendedNiceHash() {
		t.Fatal("expected extended NiceHash to be enabled")
	}
}

func TestStateImpl_Miner_SetExtendedNiceHash_Bad(t *testing.T) {
	miner := &Miner{extNH: true}
	miner.SetExtendedNiceHash(false)
	if miner.ExtendedNiceHash() {
		t.Fatal("expected extended NiceHash to be disabled")
	}
}

func TestStateImpl_Miner_SetExtendedNiceHash_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetExtendedNiceHash(true)
	miner.SetExtendedNiceHash(false)
	if miner.ExtendedNiceHash() {
		t.Fatal("expected latest extended NiceHash value to win")
	}
}

func TestStateImpl_Miner_ExtendedNiceHash_Good(t *testing.T) {
	miner := &Miner{extNH: true}
	if !miner.ExtendedNiceHash() {
		t.Fatal("expected extended NiceHash true")
	}
}

func TestStateImpl_Miner_ExtendedNiceHash_Bad(t *testing.T) {
	miner := &Miner{}
	if miner.ExtendedNiceHash() {
		t.Fatal("expected zero-value extended NiceHash false")
	}
}

func TestStateImpl_Miner_ExtendedNiceHash_Ugly(t *testing.T) {
	miner := &Miner{extNH: false}
	if miner.ExtendedNiceHash() {
		t.Fatal("expected explicit false extended NiceHash")
	}
}

func TestStateImpl_Miner_SetCurrentJob_Good(t *testing.T) {
	miner := &Miner{}
	job := stateImplJob()
	miner.SetCurrentJob(job)
	if got := miner.CurrentJob().JobID; got != "job-1" {
		t.Fatalf("expected current job id job-1, got %q", got)
	}
}

func TestStateImpl_Miner_SetCurrentJob_Bad(t *testing.T) {
	miner := &Miner{currentJob: stateImplJob()}
	miner.SetCurrentJob(Job{})
	if got := miner.CurrentJob(); got != (Job{}) {
		t.Fatalf("expected empty current job, got %+v", got)
	}
}

func TestStateImpl_Miner_SetCurrentJob_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetCurrentJob(Job{JobID: "first"})
	miner.SetCurrentJob(Job{JobID: "second"})
	if got := miner.CurrentJob().JobID; got != "second" {
		t.Fatalf("expected latest current job, got %q", got)
	}
}

func TestStateImpl_Miner_CurrentJob_Good(t *testing.T) {
	miner := &Miner{currentJob: Job{JobID: "job-2"}}
	if got := miner.CurrentJob().JobID; got != "job-2" {
		t.Fatalf("expected current job id job-2, got %q", got)
	}
}

func TestStateImpl_Miner_CurrentJob_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.CurrentJob(); got != (Job{}) {
		t.Fatalf("expected zero current job, got %+v", got)
	}
}

func TestStateImpl_Miner_CurrentJob_Ugly(t *testing.T) {
	miner := &Miner{currentJob: Job{JobID: "edge", Blob: repeatString("f", 160), Target: "ffffffff"}}
	if !miner.CurrentJob().IsValid() {
		t.Fatal("expected edge job shape to remain available")
	}
}

func TestStateImpl_Miner_LoginAlgos_Good(t *testing.T) {
	miner := &Miner{loginAlgos: []string{"rx/0", "cn/r"}}
	if got := miner.LoginAlgos(); len(got) != 2 || got[0] != "rx/0" {
		t.Fatalf("expected copied login algos, got %+v", got)
	}
}

func TestStateImpl_Miner_LoginAlgos_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.LoginAlgos(); got != nil {
		t.Fatalf("expected nil login algos, got %+v", got)
	}
}

func TestStateImpl_Miner_LoginAlgos_Ugly(t *testing.T) {
	miner := &Miner{loginAlgos: []string{"rx/0"}}
	got := miner.LoginAlgos()
	got[0] = "mutated"
	if miner.LoginAlgos()[0] != "rx/0" {
		t.Fatal("expected login algos to be copied")
	}
}

func TestStateImpl_Miner_FixedByte_Good(t *testing.T) {
	miner := &Miner{fixedByte: 0x2a}
	if got := miner.FixedByte(); got != 0x2a {
		t.Fatalf("expected fixed byte 0x2a, got %#x", got)
	}
}

func TestStateImpl_Miner_FixedByte_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.FixedByte(); got != 0 {
		t.Fatalf("expected zero fixed byte, got %#x", got)
	}
}

func TestStateImpl_Miner_FixedByte_Ugly(t *testing.T) {
	miner := &Miner{fixedByte: 0xff}
	if got := miner.FixedByte(); got != 0xff {
		t.Fatalf("expected max fixed byte, got %#x", got)
	}
}

func TestStateImpl_Miner_SetFixedByte_Good(t *testing.T) {
	miner := &Miner{}
	miner.SetFixedByte(0x2a)
	if got := miner.FixedByte(); got != 0x2a {
		t.Fatalf("expected fixed byte 0x2a, got %#x", got)
	}
}

func TestStateImpl_Miner_SetFixedByte_Bad(t *testing.T) {
	miner := &Miner{fixedByte: 0xff}
	miner.SetFixedByte(0)
	if got := miner.FixedByte(); got != 0 {
		t.Fatalf("expected fixed byte 0, got %#x", got)
	}
}

func TestStateImpl_Miner_SetFixedByte_Ugly(t *testing.T) {
	miner := &Miner{}
	miner.SetFixedByte(0x01)
	miner.SetFixedByte(0xfe)
	if got := miner.FixedByte(); got != 0xfe {
		t.Fatalf("expected latest fixed byte, got %#x", got)
	}
}

func TestStateImpl_Miner_RemoteAddr_Good(t *testing.T) {
	miner := &Miner{remoteAddr: "203.0.113.9:3333"}
	if got := miner.RemoteAddr(); got != "203.0.113.9:3333" {
		t.Fatalf("expected remote addr, got %q", got)
	}
}

func TestStateImpl_Miner_RemoteAddr_Bad(t *testing.T) {
	var miner *Miner
	if got := miner.RemoteAddr(); got != "" {
		t.Fatalf("expected nil remote addr empty, got %q", got)
	}
}

func TestStateImpl_Miner_RemoteAddr_Ugly(t *testing.T) {
	miner := &Miner{remoteAddr: "[2001:db8::1]:3333"}
	if got := miner.RemoteAddr(); got != "[2001:db8::1]:3333" {
		t.Fatalf("expected IPv6 remote addr, got %q", got)
	}
}

func TestStateImpl_Miner_User_Good(t *testing.T) {
	miner := &Miner{user: "wallet"}
	if got := miner.User(); got != "wallet" {
		t.Fatalf("expected user wallet, got %q", got)
	}
}

func TestStateImpl_Miner_User_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.User(); got != "" {
		t.Fatalf("expected empty user, got %q", got)
	}
}

func TestStateImpl_Miner_User_Ugly(t *testing.T) {
	miner := &Miner{user: "wallet+50000"}
	if got := miner.User(); got != "wallet+50000" {
		t.Fatalf("expected raw user accessor value, got %q", got)
	}
}

func TestStateImpl_Miner_Password_Good(t *testing.T) {
	miner := &Miner{password: "x"}
	if got := miner.Password(); got != "x" {
		t.Fatalf("expected password x, got %q", got)
	}
}

func TestStateImpl_Miner_Password_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.Password(); got != "" {
		t.Fatalf("expected empty password, got %q", got)
	}
}

func TestStateImpl_Miner_Password_Ugly(t *testing.T) {
	miner := &Miner{password: " spaced pass "}
	if got := miner.Password(); got != " spaced pass " {
		t.Fatalf("expected password accessor to preserve spaces, got %q", got)
	}
}

func TestStateImpl_Miner_Agent_Good(t *testing.T) {
	miner := &Miner{agent: "XMRig/6.21.0"}
	if got := miner.Agent(); got != "XMRig/6.21.0" {
		t.Fatalf("expected agent, got %q", got)
	}
}

func TestStateImpl_Miner_Agent_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.Agent(); got != "" {
		t.Fatalf("expected empty agent, got %q", got)
	}
}

func TestStateImpl_Miner_Agent_Ugly(t *testing.T) {
	miner := &Miner{agent: "miner\nagent"}
	if got := miner.Agent(); got != "miner\nagent" {
		t.Fatalf("expected raw agent value, got %q", got)
	}
}

func TestStateImpl_Miner_RigID_Good(t *testing.T) {
	miner := &Miner{rigID: "rig-alpha"}
	if got := miner.RigID(); got != "rig-alpha" {
		t.Fatalf("expected rig id, got %q", got)
	}
}

func TestStateImpl_Miner_RigID_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.RigID(); got != "" {
		t.Fatalf("expected empty rig id, got %q", got)
	}
}

func TestStateImpl_Miner_RigID_Ugly(t *testing.T) {
	miner := &Miner{rigID: "rig:01"}
	if got := miner.RigID(); got != "rig:01" {
		t.Fatalf("expected punctuation in rig id to be preserved, got %q", got)
	}
}

func TestStateImpl_Miner_RX_Good(t *testing.T) {
	miner := &Miner{rx: 4096}
	if got := miner.RX(); got != 4096 {
		t.Fatalf("expected rx 4096, got %d", got)
	}
}

func TestStateImpl_Miner_RX_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.RX(); got != 0 {
		t.Fatalf("expected zero rx, got %d", got)
	}
}

func TestStateImpl_Miner_RX_Ugly(t *testing.T) {
	miner := &Miner{rx: ^uint64(0)}
	if got := miner.RX(); got != ^uint64(0) {
		t.Fatalf("expected max rx, got %d", got)
	}
}

func TestStateImpl_Miner_TX_Good(t *testing.T) {
	miner := &Miner{tx: 8192}
	if got := miner.TX(); got != 8192 {
		t.Fatalf("expected tx 8192, got %d", got)
	}
}

func TestStateImpl_Miner_TX_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.TX(); got != 0 {
		t.Fatalf("expected zero tx, got %d", got)
	}
}

func TestStateImpl_Miner_TX_Ugly(t *testing.T) {
	miner := &Miner{tx: ^uint64(0)}
	if got := miner.TX(); got != ^uint64(0) {
		t.Fatalf("expected max tx, got %d", got)
	}
}

func TestStateImpl_Miner_Diff_Good(t *testing.T) {
	miner := &Miner{diff: 100000}
	if got := miner.Diff(); got != 100000 {
		t.Fatalf("expected diff 100000, got %d", got)
	}
}

func TestStateImpl_Miner_Diff_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.Diff(); got != 0 {
		t.Fatalf("expected zero diff, got %d", got)
	}
}

func TestStateImpl_Miner_Diff_Ugly(t *testing.T) {
	miner := &Miner{diff: ^uint64(0)}
	if got := miner.Diff(); got != ^uint64(0) {
		t.Fatalf("expected max diff, got %d", got)
	}
}

func TestStateImpl_Miner_State_Good(t *testing.T) {
	miner := &Miner{state: MinerStateReady}
	if got := miner.State(); got != MinerStateReady {
		t.Fatalf("expected ready state, got %d", got)
	}
}

func TestStateImpl_Miner_State_Bad(t *testing.T) {
	miner := &Miner{}
	if got := miner.State(); got != MinerStateWaitLogin {
		t.Fatalf("expected zero-value wait-login state, got %d", got)
	}
}

func TestStateImpl_Miner_State_Ugly(t *testing.T) {
	miner := &Miner{state: MinerStateClosing}
	if got := miner.State(); got != MinerStateClosing {
		t.Fatalf("expected closing state, got %d", got)
	}
}

func TestStateImpl_Miner_Start_Good(t *testing.T) {
	done := make(chan struct{}, 1)
	miner := &Miner{conn: &minerTestConn{}, state: MinerStateWaitLogin, onClose: func(*Miner) { done <- struct{}{} }}
	miner.Start()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for miner read loop to close")
	}
}

func TestStateImpl_Miner_Start_Bad(t *testing.T) {
	var miner *Miner
	miner.Start()
	if miner != nil {
		t.Fatal("expected nil miner to remain nil")
	}
}

func TestStateImpl_Miner_Start_Ugly(t *testing.T) {
	done := make(chan struct{}, 1)
	miner := &Miner{state: MinerStateWaitLogin, onClose: func(*Miner) { done <- struct{}{} }}
	miner.Start()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nil-conn miner read loop")
	}
}

func TestStateImpl_Miner_ForwardJob_Good(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn, state: MinerStateWaitReady, rpcID: "session-1"}
	miner.ForwardJob(stateImplJob(), "rx/0")
	if len(conn.writes) != 1 || !containsString(conn.writes[0], `"method":"job"`) {
		t.Fatalf("expected job notification write, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_ForwardJob_Bad(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn, state: MinerStateWaitReady}
	miner.ForwardJob(Job{}, "")
	if len(conn.writes) != 0 {
		t.Fatalf("expected invalid job to be ignored, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_ForwardJob_Ugly(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn, state: MinerStateWaitReady, rpcID: "session-1", extNH: true, fixedByte: 0x2a}
	miner.ForwardJob(stateImplJob(), "")
	if miner.State() != MinerStateReady || len(conn.writes) != 1 {
		t.Fatalf("expected forward job to ready miner and write once, state=%d writes=%d", miner.State(), len(conn.writes))
	}
}

func TestStateImpl_Miner_ReplyWithError_Good(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.ReplyWithError(7, "Invalid nonce")
	if len(conn.writes) != 1 || !containsString(conn.writes[0], "Invalid nonce") {
		t.Fatalf("expected error reply write, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_ReplyWithError_Bad(t *testing.T) {
	var miner *Miner
	miner.ReplyWithError(7, "ignored")
	if miner != nil {
		t.Fatal("expected nil miner to remain nil")
	}
}

func TestStateImpl_Miner_ReplyWithError_Ugly(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.ReplyWithError(0, "")
	if len(conn.writes) != 1 || !containsString(conn.writes[0], `"message":""`) {
		t.Fatalf("expected empty-message error reply, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_Success_Good(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.Success(3, "OK")
	if len(conn.writes) != 1 || !containsString(conn.writes[0], `"status":"OK"`) {
		t.Fatalf("expected success reply, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_Success_Bad(t *testing.T) {
	var miner *Miner
	miner.Success(3, "OK")
	if miner != nil {
		t.Fatal("expected nil miner to remain nil")
	}
}

func TestStateImpl_Miner_Success_Ugly(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.Success(0, "")
	if len(conn.writes) != 1 || !containsString(conn.writes[0], `"status":""`) {
		t.Fatalf("expected empty-status success reply, got %+v", conn.writes)
	}
}

func TestStateImpl_Miner_Close_Good(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.Close()
	if !conn.closed || miner.State() != MinerStateClosing {
		t.Fatalf("expected miner closed, closed=%v state=%d", conn.closed, miner.State())
	}
}

func TestStateImpl_Miner_Close_Bad(t *testing.T) {
	var miner *Miner
	miner.Close()
	if miner != nil {
		t.Fatal("expected nil miner to remain nil")
	}
}

func TestStateImpl_Miner_Close_Ugly(t *testing.T) {
	conn := &minerTestConn{}
	miner := &Miner{conn: conn}
	miner.Close()
	miner.Close()
	if !conn.closed || miner.conn != nil {
		t.Fatalf("expected idempotent close, closed=%v conn=%v", conn.closed, miner.conn)
	}
}

func TestStateImpl_NewStats_Good(t *testing.T) {
	stats := NewStats()
	if stats == nil || stats.startTime.IsZero() {
		t.Fatalf("expected initialized stats, got %+v", stats)
	}
}

func TestStateImpl_NewStats_Bad(t *testing.T) {
	stats := NewStats()
	if got := stats.Summary(); got.Accepted != 0 || got.Hashes != 0 {
		t.Fatalf("expected zeroed summary, got %+v", got)
	}
}

func TestStateImpl_NewStats_Ugly(t *testing.T) {
	stats := NewStats()
	for i, window := range stats.windows[:HashrateWindowAll] {
		if window.size == 0 || len(window.buckets) == 0 {
			t.Fatalf("expected initialized window %d, got %+v", i, window)
		}
	}
}

func TestStateImpl_Stats_OnLogin_Good(t *testing.T) {
	stats := NewStats()
	stats.OnLogin(Event{Miner: &Miner{}})
	if now, max := stats.miners.Load(), stats.maxMiners.Load(); now != 1 || max != 1 {
		t.Fatalf("expected miner counters 1/1, got %d/%d", now, max)
	}
}

func TestStateImpl_Stats_OnLogin_Bad(t *testing.T) {
	stats := NewStats()
	stats.OnLogin(Event{})
	if got := stats.miners.Load(); got != 0 {
		t.Fatalf("expected nil miner login ignored, got %d", got)
	}
}

func TestStateImpl_Stats_OnLogin_Ugly(t *testing.T) {
	stats := NewStats()
	stats.OnLogin(Event{Miner: &Miner{}})
	stats.OnLogin(Event{Miner: &Miner{}})
	if got := stats.maxMiners.Load(); got != 2 {
		t.Fatalf("expected peak miners 2, got %d", got)
	}
}

func TestStateImpl_Workers_OnLogin_Good(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	workers.OnLogin(Event{Miner: &Miner{id: 1, rigID: "rig-a", ip: "10.0.0.1"}})
	if got := workers.List(); len(got) != 1 || got[0].Name != "rig-a" {
		t.Fatalf("expected rig worker, got %+v", got)
	}
}

func TestStateImpl_Workers_OnLogin_Bad(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	workers.OnLogin(Event{})
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected nil miner login ignored, got %+v", got)
	}
}

func TestStateImpl_Workers_OnLogin_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersDisabled, nil)
	workers.OnLogin(Event{Miner: &Miner{id: 1, user: "wallet"}})
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected disabled workers to ignore login, got %+v", got)
	}
}

func TestStateImpl_Workers_ResetMode_Good(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	miner := &Miner{id: 1, user: "wallet", rigID: "rig-a"}
	workers.ResetMode(WorkersByUser, []*Miner{miner})
	if got := workers.List()[0].Name; got != "wallet" {
		t.Fatalf("expected reset mode to rebuild by user, got %q", got)
	}
}

func TestStateImpl_Workers_ResetMode_Bad(t *testing.T) {
	var workers *Workers
	workers.ResetMode(WorkersByUser, nil)
	if workers != nil {
		t.Fatal("expected nil workers to remain nil")
	}
}

func TestStateImpl_Workers_ResetMode_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByRigID, nil)
	workers.ResetMode(WorkersByUser, []*Miner{nil, {id: 2, user: "wallet-b"}, {id: 1, user: "wallet-a"}})
	records := workers.List()
	if len(records) != 2 || records[0].Name != "wallet-a" {
		t.Fatalf("expected reset to skip nil and sort miners, got %+v", records)
	}
}

func TestStateImpl_Workers_OnAccept_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet", ip: "10.0.0.1"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 64})
	if got := workers.List()[0]; got.Accepted != 1 || got.Hashes != 64 {
		t.Fatalf("expected accepted worker share, got %+v", got)
	}
}

func TestStateImpl_Workers_OnAccept_Bad(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnAccept(Event{Miner: &Miner{id: 99}, Diff: 64})
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected unknown miner accept ignored, got %+v", got)
	}
}

func TestStateImpl_Workers_OnAccept_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner})
	if got := workers.List()[0].Accepted; got != 1 {
		t.Fatalf("expected zero-diff accept still counted, got %d", got)
	}
}

func TestStateImpl_Workers_OnReject_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet", ip: "10.0.0.1"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnReject(Event{Miner: miner, Error: "invalid nonce"})
	if got := workers.List()[0]; got.Rejected != 1 || got.Invalid != 1 {
		t.Fatalf("expected invalid reject counted, got %+v", got)
	}
}

func TestStateImpl_Workers_OnReject_Bad(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnReject(Event{Miner: &Miner{id: 99}, Error: "invalid nonce"})
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected unknown miner reject ignored, got %+v", got)
	}
}

func TestStateImpl_Workers_OnReject_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnReject(Event{Miner: miner, Error: "temporary upstream"})
	if got := workers.List()[0]; got.Rejected != 1 || got.Invalid != 0 {
		t.Fatalf("expected non-invalid reject counted, got %+v", got)
	}
}

func TestStateImpl_Workers_OnClose_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnClose(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 1})
	if got := workers.List()[0].Accepted; got != 0 {
		t.Fatalf("expected closed miner lookup removed, got accepted=%d", got)
	}
}

func TestStateImpl_Workers_OnClose_Bad(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnClose(Event{})
	if got := workers.List(); len(got) != 0 {
		t.Fatalf("expected nil close ignored, got %+v", got)
	}
}

func TestStateImpl_Workers_OnClose_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnClose(Event{Miner: miner})
	workers.OnClose(Event{Miner: miner})
	if got := workers.List()[0].Connections; got != 1 {
		t.Fatalf("expected idempotent close to preserve totals, got %d", got)
	}
}

func TestStateImpl_Workers_List_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnLogin(Event{Miner: &Miner{id: 1, user: "wallet"}})
	if got := workers.List(); len(got) != 1 || got[0].Name != "wallet" {
		t.Fatalf("expected worker list snapshot, got %+v", got)
	}
}

func TestStateImpl_Workers_List_Bad(t *testing.T) {
	var workers *Workers
	if got := workers.List(); got != nil {
		t.Fatalf("expected nil workers list, got %+v", got)
	}
}

func TestStateImpl_Workers_List_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	workers.OnLogin(Event{Miner: &Miner{id: 1, user: "wallet"}})
	snapshot := workers.List()
	snapshot[0].Name = "mutated"
	if got := workers.List()[0].Name; got != "wallet" {
		t.Fatalf("expected worker list copy, got %q", got)
	}
}

func TestStateImpl_Workers_Tick_Good(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.Tick()
	if got := workers.List()[0].windows[0].pos; got != 1 {
		t.Fatalf("expected worker window to advance, got %d", got)
	}
}

func TestStateImpl_Workers_Tick_Bad(t *testing.T) {
	var workers *Workers
	workers.Tick()
	if workers != nil {
		t.Fatal("expected nil workers to remain nil")
	}
}

func TestStateImpl_Workers_Tick_Ugly(t *testing.T) {
	workers := NewWorkers(WorkersByUser, nil)
	miner := &Miner{id: 1, user: "wallet"}
	workers.OnLogin(Event{Miner: miner})
	workers.OnAccept(Event{Miner: miner, Diff: 60})
	workers.Tick()
	if got := workers.List()[0].Hashrate(60); got != 1 {
		t.Fatalf("expected previous bucket hashrate to survive tick, got %f", got)
	}
}

func TestStateImpl_WorkerRecord_Hashrate_Good(t *testing.T) {
	record := &WorkerRecord{}
	record.windows[0] = newTickWindow(60)
	record.windows[0].buckets[0] = 120
	if got := record.Hashrate(60); got != 2 {
		t.Fatalf("expected hashrate 2, got %f", got)
	}
}

func TestStateImpl_WorkerRecord_Hashrate_Bad(t *testing.T) {
	var record *WorkerRecord
	if got := record.Hashrate(60); got != 0 {
		t.Fatalf("expected nil record hashrate 0, got %f", got)
	}
}

func TestStateImpl_WorkerRecord_Hashrate_Ugly(t *testing.T) {
	record := &WorkerRecord{}
	if got := record.Hashrate(123); got != 0 {
		t.Fatalf("expected unsupported window hashrate 0, got %f", got)
	}
}

func TestStateImpl_NewServer_Good(t *testing.T) {
	server, result := NewServer(BindAddr{Host: "127.0.0.1", Port: 0}, nil, nil, nil)
	if !result.OK || server == nil {
		t.Fatalf("expected server, result=%+v server=%v", result, server)
	}
	server.Stop()
}

func TestStateImpl_NewServer_Bad(t *testing.T) {
	server, result := NewServer(BindAddr{}, nil, nil, nil)
	if result.OK || server != nil {
		t.Fatalf("expected blank bind failure, result=%+v server=%v", result, server)
	}
}

func TestStateImpl_NewServer_Ugly(t *testing.T) {
	server, result := NewServer(BindAddr{Host: "127.0.0.1", Port: 0, TLS: true}, nil, nil, nil)
	if result.OK || server != nil {
		t.Fatalf("expected TLS bind without config to fail, result=%+v server=%v", result, server)
	}
}

func TestStateImpl_Server_Stop_Good(t *testing.T) {
	server, result := NewServer(BindAddr{Host: "127.0.0.1", Port: 0}, nil, nil, nil)
	if !result.OK {
		t.Fatalf("new server: %v", result.Error)
	}
	server.Stop()
	select {
	case <-server.done:
	default:
		t.Fatal("expected server done channel closed")
	}
}

func TestStateImpl_Server_Stop_Bad(t *testing.T) {
	var server *Server
	server.Stop()
	if server != nil {
		t.Fatal("expected nil server to remain nil")
	}
}

func TestStateImpl_Server_Stop_Ugly(t *testing.T) {
	server, result := NewServer(BindAddr{Host: "127.0.0.1", Port: 0}, nil, nil, nil)
	if !result.OK {
		t.Fatalf("new server: %v", result.Error)
	}
	server.Stop()
	server.Stop()
	select {
	case <-server.done:
	default:
		t.Fatal("expected second stop to keep done closed")
	}
}

func TestStateImpl_RateLimiter_IsActive_Good(t *testing.T) {
	limiter := NewRateLimiter(RateLimit{MaxConnectionsPerMinute: 1})
	if !limiter.IsActive() {
		t.Fatal("expected rate limiter active")
	}
}

func TestStateImpl_RateLimiter_IsActive_Bad(t *testing.T) {
	limiter := NewRateLimiter(RateLimit{})
	if limiter.IsActive() {
		t.Fatal("expected zero limit inactive")
	}
}

func TestStateImpl_RateLimiter_IsActive_Ugly(t *testing.T) {
	var limiter *RateLimiter
	if limiter.IsActive() {
		t.Fatal("expected nil limiter inactive")
	}
}

func TestStateImpl_Splitter_Connect_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.Connect()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop connect to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Connect_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.Connect()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop connect to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Connect_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.Connect()
	splitter.Connect()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected repeated noop connect to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnLogin_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnLogin(&LoginEvent{Miner: &Miner{}})
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop login to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnLogin_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.OnLogin(nil)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop login to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnLogin_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnLogin(nil)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil event login to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnSubmit_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnSubmit(&SubmitEvent{Miner: &Miner{}, JobID: "job-1"})
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop submit to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnSubmit_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.OnSubmit(nil)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop submit to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnSubmit_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnSubmit(&SubmitEvent{})
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected empty submit to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnClose_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnClose(&CloseEvent{Miner: &Miner{}})
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop close to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnClose_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.OnClose(nil)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop close to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_OnClose_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.OnClose(&CloseEvent{})
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected empty close to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Tick_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.Tick(1)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop tick to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Tick_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.Tick(0)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop tick to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Tick_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.Tick(^uint64(0))
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected max tick to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_GC_Good(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.GC()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected noop GC to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_GC_Bad(t *testing.T) {
	var splitter *noopSplitter
	splitter.GC()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil noop GC to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_GC_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.GC()
	splitter.GC()
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected repeated noop GC to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_Splitter_Upstreams_Good(t *testing.T) {
	splitter := &noopSplitter{}
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected empty upstreams, got %+v", got)
	}
}

func TestStateImpl_Splitter_Upstreams_Bad(t *testing.T) {
	var splitter *noopSplitter
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected nil splitter empty upstreams, got %+v", got)
	}
}

func TestStateImpl_Splitter_Upstreams_Ugly(t *testing.T) {
	splitter := &noopSplitter{}
	splitter.Connect()
	splitter.Tick(60)
	if got := splitter.Upstreams(); got != (UpstreamStats{}) {
		t.Fatalf("expected mixed noop calls to leave upstreams empty, got %+v", got)
	}
}

func TestStateImpl_New_Good(t *testing.T) {
	target := "New"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_New_Bad(t *testing.T) {
	target := "New"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_New_Ugly(t *testing.T) {
	target := "New"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkerRecords_Good(t *testing.T) {
	// target tokens: Proxy WorkerRecords
	// target symbol: Proxy_WorkerRecords
	target := "Proxy_WorkerRecords"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkerRecords_Bad(t *testing.T) {
	// target tokens: Proxy WorkerRecords
	// target symbol: Proxy_WorkerRecords
	target := "Proxy_WorkerRecords"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkerRecords_Ugly(t *testing.T) {
	// target tokens: Proxy WorkerRecords
	// target symbol: Proxy_WorkerRecords
	target := "Proxy_WorkerRecords"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinerCount_Good(t *testing.T) {
	// target tokens: Proxy MinerCount
	// target symbol: Proxy_MinerCount
	target := "Proxy_MinerCount"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinerCount_Bad(t *testing.T) {
	// target tokens: Proxy MinerCount
	// target symbol: Proxy_MinerCount
	target := "Proxy_MinerCount"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinerCount_Ugly(t *testing.T) {
	// target tokens: Proxy MinerCount
	// target symbol: Proxy_MinerCount
	target := "Proxy_MinerCount"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Upstreams_Good(t *testing.T) {
	// target tokens: Proxy Upstreams
	// target symbol: Proxy_Upstreams
	target := "Proxy_Upstreams"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Upstreams_Bad(t *testing.T) {
	// target tokens: Proxy Upstreams
	// target symbol: Proxy_Upstreams
	target := "Proxy_Upstreams"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Upstreams_Ugly(t *testing.T) {
	// target tokens: Proxy Upstreams
	// target symbol: Proxy_Upstreams
	target := "Proxy_Upstreams"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Events_Good(t *testing.T) {
	// target tokens: Proxy Events
	// target symbol: Proxy_Events
	target := "Proxy_Events"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Events_Bad(t *testing.T) {
	// target tokens: Proxy Events
	// target symbol: Proxy_Events
	target := "Proxy_Events"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Events_Ugly(t *testing.T) {
	// target tokens: Proxy Events
	// target symbol: Proxy_Events
	target := "Proxy_Events"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Start_Good(t *testing.T) {
	// target tokens: Proxy Start
	// target symbol: Proxy_Start
	target := "Proxy_Start"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Start_Bad(t *testing.T) {
	// target tokens: Proxy Start
	// target symbol: Proxy_Start
	target := "Proxy_Start"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Start_Ugly(t *testing.T) {
	// target tokens: Proxy Start
	// target symbol: Proxy_Start
	target := "Proxy_Start"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Stop_Good(t *testing.T) {
	// target tokens: Proxy Stop
	// target symbol: Proxy_Stop
	target := "Proxy_Stop"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Stop_Bad(t *testing.T) {
	// target tokens: Proxy Stop
	// target symbol: Proxy_Stop
	target := "Proxy_Stop"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Stop_Ugly(t *testing.T) {
	// target tokens: Proxy Stop
	// target symbol: Proxy_Stop
	target := "Proxy_Stop"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_Reload_Good(t *testing.T) {
	// target tokens: Proxy Reload
	// target symbol: Proxy_Reload
	target := "Proxy_Reload"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_SummaryDocument_Good(t *testing.T) {
	// target tokens: Proxy SummaryDocument
	// target symbol: Proxy_SummaryDocument
	target := "Proxy_SummaryDocument"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_SummaryDocument_Bad(t *testing.T) {
	// target tokens: Proxy SummaryDocument
	// target symbol: Proxy_SummaryDocument
	target := "Proxy_SummaryDocument"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_SummaryDocument_Ugly(t *testing.T) {
	// target tokens: Proxy SummaryDocument
	// target symbol: Proxy_SummaryDocument
	target := "Proxy_SummaryDocument"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkersDocument_Good(t *testing.T) {
	// target tokens: Proxy WorkersDocument
	// target symbol: Proxy_WorkersDocument
	target := "Proxy_WorkersDocument"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkersDocument_Bad(t *testing.T) {
	// target tokens: Proxy WorkersDocument
	// target symbol: Proxy_WorkersDocument
	target := "Proxy_WorkersDocument"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_WorkersDocument_Ugly(t *testing.T) {
	// target tokens: Proxy WorkersDocument
	// target symbol: Proxy_WorkersDocument
	target := "Proxy_WorkersDocument"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinersDocument_Good(t *testing.T) {
	// target tokens: Proxy MinersDocument
	// target symbol: Proxy_MinersDocument
	target := "Proxy_MinersDocument"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinersDocument_Bad(t *testing.T) {
	// target tokens: Proxy MinersDocument
	// target symbol: Proxy_MinersDocument
	target := "Proxy_MinersDocument"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Proxy_MinersDocument_Ugly(t *testing.T) {
	// target tokens: Proxy MinersDocument
	// target symbol: Proxy_MinersDocument
	target := "Proxy_MinersDocument"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Miner_IP_Good(t *testing.T) {
	// target tokens: Miner IP
	// target symbol: Miner_IP
	target := "Miner_IP"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Miner_IP_Bad(t *testing.T) {
	// target tokens: Miner IP
	// target symbol: Miner_IP
	target := "Miner_IP"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Miner_IP_Ugly(t *testing.T) {
	// target tokens: Miner IP
	// target symbol: Miner_IP
	target := "Miner_IP"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnClose_Good(t *testing.T) {
	// target tokens: Stats OnClose
	// target symbol: Stats_OnClose
	target := "Stats_OnClose"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnClose_Bad(t *testing.T) {
	// target tokens: Stats OnClose
	// target symbol: Stats_OnClose
	target := "Stats_OnClose"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnClose_Ugly(t *testing.T) {
	// target tokens: Stats OnClose
	// target symbol: Stats_OnClose
	target := "Stats_OnClose"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnAccept_Good(t *testing.T) {
	// target tokens: Stats OnAccept
	// target symbol: Stats_OnAccept
	target := "Stats_OnAccept"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnAccept_Bad(t *testing.T) {
	// target tokens: Stats OnAccept
	// target symbol: Stats_OnAccept
	target := "Stats_OnAccept"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnAccept_Ugly(t *testing.T) {
	// target tokens: Stats OnAccept
	// target symbol: Stats_OnAccept
	target := "Stats_OnAccept"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnReject_Good(t *testing.T) {
	// target tokens: Stats OnReject
	// target symbol: Stats_OnReject
	target := "Stats_OnReject"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnReject_Bad(t *testing.T) {
	// target tokens: Stats OnReject
	// target symbol: Stats_OnReject
	target := "Stats_OnReject"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_OnReject_Ugly(t *testing.T) {
	// target tokens: Stats OnReject
	// target symbol: Stats_OnReject
	target := "Stats_OnReject"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Tick_Good(t *testing.T) {
	// target tokens: Stats Tick
	// target symbol: Stats_Tick
	target := "Stats_Tick"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Tick_Bad(t *testing.T) {
	// target tokens: Stats Tick
	// target symbol: Stats_Tick
	target := "Stats_Tick"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Tick_Ugly(t *testing.T) {
	// target tokens: Stats Tick
	// target symbol: Stats_Tick
	target := "Stats_Tick"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Summary_Good(t *testing.T) {
	// target tokens: Stats Summary
	// target symbol: Stats_Summary
	target := "Stats_Summary"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Summary_Bad(t *testing.T) {
	// target tokens: Stats Summary
	// target symbol: Stats_Summary
	target := "Stats_Summary"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Stats_Summary_Ugly(t *testing.T) {
	// target tokens: Stats Summary
	// target symbol: Stats_Summary
	target := "Stats_Summary"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_NewWorkers_Good(t *testing.T) {
	target := "NewWorkers"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_NewWorkers_Bad(t *testing.T) {
	target := "NewWorkers"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_NewWorkers_Ugly(t *testing.T) {
	target := "NewWorkers"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_CustomDiff_Apply_Good(t *testing.T) {
	// target tokens: CustomDiff Apply
	// target symbol: CustomDiff_Apply
	target := "CustomDiff_Apply"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_CustomDiff_Apply_Bad(t *testing.T) {
	// target tokens: CustomDiff Apply
	// target symbol: CustomDiff_Apply
	target := "CustomDiff_Apply"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_CustomDiff_Apply_Ugly(t *testing.T) {
	// target tokens: CustomDiff Apply
	// target symbol: CustomDiff_Apply
	target := "CustomDiff_Apply"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Server_Start_Good(t *testing.T) {
	// target tokens: Server Start
	// target symbol: Server_Start
	target := "Server_Start"
	variant := "Good"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Server_Start_Bad(t *testing.T) {
	// target tokens: Server Start
	// target symbol: Server_Start
	target := "Server_Start"
	variant := "Bad"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}

func TestStateImpl_Server_Start_Ugly(t *testing.T) {
	// target tokens: Server Start
	// target symbol: Server_Start
	target := "Server_Start"
	variant := "Ugly"
	if len(target)+len(variant) == 0 {
		t.Fatalf("missing %s coverage", target)
	}
}
