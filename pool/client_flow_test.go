package pool

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type clientListenerSpy struct {
	mu          sync.Mutex
	jobs        []proxy.Job
	results     []resultEvent
	disconnects int
}

type resultEvent struct {
	seq      int64
	accepted bool
	message  string
}

func (s *clientListenerSpy) OnJob(job proxy.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job)
}

func (s *clientListenerSpy) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, resultEvent{seq: sequence, accepted: accepted, message: errorMessage})
}

func (s *clientListenerSpy) OnDisconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disconnects++
}

func startStratumServer(t *testing.T, handler func(net.Conn, *bufio.Reader)) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn, bufio.NewReader(conn))
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func readLine(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return line
}

func TestStratumClient_Connect_Good(t *testing.T) {
	addr, shutdown := startStratumServer(t, func(conn net.Conn, reader *bufio.Reader) {
		loginLine := readLine(t, reader)
		var loginReq struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(loginLine, &loginReq); err != nil {
			t.Fatalf("decode login request: %v", err)
		}
		if loginReq.Method != "login" {
			t.Fatalf("expected login method, got %q", loginReq.Method)
		}

		_, _ = io.WriteString(conn, `{"id":"session-1","result":{"id":"session-1","job":{"blob":"`+strings.Repeat("0", 160)+`","job_id":"job-1","target":"b88d0600","algo":"cn/r","height":7,"seed_hash":"seed","id":"session-1"}}}`+"\n")

		submitLine := readLine(t, reader)
		var submitReq struct {
			Method string `json:"method"`
			Params struct {
				ID     string `json:"id"`
				JobID  string `json:"job_id"`
				Nonce  string `json:"nonce"`
				Result string `json:"result"`
				Algo   string `json:"algo"`
			} `json:"params"`
		}
		if err := json.Unmarshal(submitLine, &submitReq); err != nil {
			t.Fatalf("decode submit request: %v", err)
		}
		if submitReq.Method != "submit" {
			t.Fatalf("expected submit method, got %q", submitReq.Method)
		}
		if submitReq.Params.ID != "session-1" {
			t.Fatalf("expected submit session id to be propagated, got %q", submitReq.Params.ID)
		}
		if submitReq.Params.JobID != "job-1" || submitReq.Params.Nonce != "deadbeef" {
			t.Fatalf("unexpected submit params: %+v", submitReq.Params)
		}

		_, _ = io.WriteString(conn, `{"id":2,"result":{"status":"OK"}}`+"\n")
		time.Sleep(25 * time.Millisecond)
	})
	defer shutdown()

	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{
		URL:   addr,
		User:  "WALLET",
		Pass:  "x",
		RigID: "rig-1",
		Algo:  "cn/r",
	}, spy)

	if result := client.Connect(); !result.OK {
		t.Fatalf("expected connect to succeed, got %v", result.Error)
	}
	client.Login()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotJobs := len(spy.jobs)
		spy.mu.Unlock()
		if gotJobs > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.jobs) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one job notification, got %+v", spy.jobs)
	}
	spy.mu.Unlock()
	if !client.IsActive() {
		t.Fatal("expected client to become active after job notification")
	}

	if seq := client.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r"); seq != 2 {
		t.Fatalf("expected submit sequence 2, got %d", seq)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotResults := len(spy.results)
		spy.mu.Unlock()
		if gotResults > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if len(spy.results) != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one result notification, got %+v", spy.results)
	}
	if !spy.results[0].accepted || spy.results[0].message != "" {
		spy.mu.Unlock()
		t.Fatalf("expected accepted result with no message, got %+v", spy.results[0])
	}
	spy.mu.Unlock()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy.mu.Lock()
		gotDisconnects := spy.disconnects
		spy.mu.Unlock()
		if gotDisconnects > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	spy.mu.Lock()
	if spy.disconnects != 1 {
		spy.mu.Unlock()
		t.Fatalf("expected one disconnect notification, got %d", spy.disconnects)
	}
	spy.mu.Unlock()

	t.Run("tls_fingerprint", func(t *testing.T) {
		addr, fingerprint, shutdownTLS := startTLSStratumServer(t)
		defer shutdownTLS()

		spy := &clientListenerSpy{}
		client := NewStratumClient(proxy.PoolConfig{
			URL:            addr,
			User:           "WALLET",
			Pass:           "x",
			TLS:            true,
			TLSFingerprint: fingerprint,
		}, spy)

		if result := client.Connect(); !result.OK {
			t.Fatalf("expected tls connect to succeed, got %v", result.Error)
		}
		time.Sleep(25 * time.Millisecond)
	})
}

func TestStratumClient_Connect_Bad(t *testing.T) {
	client := NewStratumClient(proxy.PoolConfig{}, nil)
	if result := client.Connect(); result.OK {
		t.Fatal("expected empty pool URL to fail")
	}
}

func TestStratumClient_HandleMessage_Ugly(t *testing.T) {
	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{}, spy)

	client.handleMessage([]byte("not-json"))
	client.handleMessage([]byte(`{"id":1,"error":{"message":"bad"}}`))

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.jobs) != 0 || len(spy.results) != 0 || spy.disconnects != 1 {
		t.Fatalf("expected malformed payloads to be ignored and login error to disconnect once, got jobs=%d results=%d disconnects=%d", len(spy.jobs), len(spy.results), spy.disconnects)
	}
}

func TestStratumClient_HandleMessage_Good(t *testing.T) {
	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{}, spy)

	client.handleMessage([]byte(`{"method":"job","params":{"blob":"` + strings.Repeat("0", 160) + `","job_id":"job-1","target":"b88d0600","algo":"cn/r","height":7,"seed_hash":"seed","id":"session-2"}}`))

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.jobs) != 1 {
		t.Fatalf("expected one job notification, got %d", len(spy.jobs))
	}
	job := spy.jobs[0]
	if job.JobID != "job-1" || job.Algo != "cn/r" || job.ClientID != "session-2" {
		t.Fatalf("unexpected job notification: %+v", job)
	}
	if !client.IsActive() {
		t.Fatal("expected client to become active after a job notification")
	}
}

func TestStratumClient_HandleMessage_Bad(t *testing.T) {
	spy := &clientListenerSpy{}
	client := NewStratumClient(proxy.PoolConfig{}, spy)

	client.handleMessage([]byte(`{"id":"session-1","result":{"id":"session-1"}}`))

	if got := client.SessionID(); got != "session-1" {
		t.Fatalf("expected session id to be recorded, got %q", got)
	}
	if client.IsActive() {
		t.Fatal("expected client to remain inactive until a job arrives")
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.jobs) != 0 || len(spy.results) != 0 || spy.disconnects != 0 {
		t.Fatalf("expected session-only login reply to be ignored by listener, got jobs=%d results=%d disconnects=%d", len(spy.jobs), len(spy.results), spy.disconnects)
	}
}

func TestStratumClient_Connect_Bad_TLSFingerprint(t *testing.T) {
	addr, _, shutdownTLS := startTLSStratumServer(t)
	defer shutdownTLS()

	client := NewStratumClient(proxy.PoolConfig{
		URL:            addr,
		User:           "WALLET",
		Pass:           "x",
		TLS:            true,
		TLSFingerprint: strings.Repeat("0", 64),
	}, nil)

	if result := client.Connect(); result.OK {
		t.Fatal("expected tls fingerprint mismatch to fail connection")
	}
}

func TestStratumClient_Submit_Good(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &StratumClient{
		conn:      clientConn,
		sessionID: "session-1",
		pending:   make(map[int64]struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		line := readLine(t, bufio.NewReader(serverConn))
		var payload struct {
			Method string `json:"method"`
			Params struct {
				ID     string `json:"id"`
				JobID  string `json:"job_id"`
				Nonce  string `json:"nonce"`
				Result string `json:"result"`
				Algo   string `json:"algo"`
			} `json:"params"`
		}
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatalf("decode submit request: %v", err)
		}
		if payload.Method != "submit" {
			t.Fatalf("expected submit method, got %q", payload.Method)
		}
		if payload.Params.ID != "session-1" || payload.Params.JobID != "job-1" || payload.Params.Nonce != "deadbeef" || payload.Params.Result != "HASH64HEX" || payload.Params.Algo != "cn/r" {
			t.Fatalf("unexpected submit params: %+v", payload.Params)
		}
	}()

	if seq := client.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r"); seq != 1 {
		t.Fatalf("expected submit sequence 1, got %d", seq)
	}
	<-done

	if got := len(client.pending); got != 1 {
		t.Fatalf("expected one pending request, got %d", got)
	}
}

func TestStratumClient_Submit_Bad(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &StratumClient{
		conn:      clientConn,
		sessionID: "session-1",
		pending:   make(map[int64]struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		line := readLine(t, bufio.NewReader(serverConn))
		var payload struct {
			Params map[string]any `json:"params"`
		}
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatalf("decode submit request: %v", err)
		}
		if _, ok := payload.Params["algo"]; ok {
			t.Fatalf("expected empty algo to be omitted from submit payload, got %#v", payload.Params["algo"])
		}
	}()

	if seq := client.Submit("job-1", "deadbeef", "HASH64HEX", ""); seq != 1 {
		t.Fatalf("expected submit sequence 1, got %d", seq)
	}
	<-done
}

type failingConn struct{}

func (f failingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f failingConn) Write([]byte) (int, error)        { return 0, errors.New("write failed") }
func (f failingConn) Close() error                     { return nil }
func (f failingConn) LocalAddr() net.Addr              { return nil }
func (f failingConn) RemoteAddr() net.Addr             { return nil }
func (f failingConn) SetDeadline(time.Time) error      { return nil }
func (f failingConn) SetReadDeadline(time.Time) error  { return nil }
func (f failingConn) SetWriteDeadline(time.Time) error { return nil }

func TestStratumClient_Submit_Ugly(t *testing.T) {
	spy := &clientListenerSpy{}
	client := &StratumClient{
		conn:      failingConn{},
		listener:  spy,
		sessionID: "session-1",
		pending:   make(map[int64]struct{}),
	}

	if seq := client.Submit("job-1", "deadbeef", "HASH64HEX", "cn/r"); seq != 1 {
		t.Fatalf("expected submit sequence 1, got %d", seq)
	}
	if len(client.pending) != 0 {
		t.Fatalf("expected failed submit to clear pending entry, got %d", len(client.pending))
	}
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.disconnects != 1 {
		t.Fatalf("expected write failure to notify one disconnect, got %d", spy.disconnects)
	}
}

func startTLSStratumServer(t *testing.T) (string, string, func()) {
	t.Helper()

	cert, fingerprint := mustGenerateSelfSignedCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen tls: %v", err)
	}
	connected := make(chan struct{})
	done := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			_ = tlsConn.Handshake()
		}
		close(connected)
		<-release
		_ = conn.Close()
	}()

	return ln.Addr().String(), fingerprint, func() {
		select {
		case <-connected:
		default:
		}
		close(release)
		_ = ln.Close()
		<-done
	}
}

func mustGenerateSelfSignedCert(t *testing.T) (tls.Certificate, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "pool-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(pemCert, pemKey)
	if err != nil {
		t.Fatalf("load x509 key pair: %v", err)
	}
	sum := sha256.Sum256(der)
	return cert, hex.EncodeToString(sum[:])
}
