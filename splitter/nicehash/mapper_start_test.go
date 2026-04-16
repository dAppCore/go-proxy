package nicehash

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
)

type startCountingStrategy struct {
	mu      sync.Mutex
	connect int
}

func (s *startCountingStrategy) Connect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connect++
}

func (s *startCountingStrategy) Submit(jobID, nonce, result, algo string) int64 {
	return 0
}

func (s *startCountingStrategy) Disconnect() {}

func (s *startCountingStrategy) IsActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connect > 0
}

type discardConn struct{}

func (discardConn) Read([]byte) (int, error)         { return 0, nil }
func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return nil }
func (discardConn) RemoteAddr() net.Addr             { return nil }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

func TestMapper_Start_Good(t *testing.T) {
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)

	mapper.Start()

	if strategy.connect != 1 {
		t.Fatalf("expected one connect call, got %d", strategy.connect)
	}
}

func TestMapper_Start_Bad(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, nil)

	mapper.Start()
}

func TestMapper_Start_Ugly(t *testing.T) {
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)

	mapper.Start()
	mapper.Start()

	if strategy.connect != 1 {
		t.Fatalf("expected Start to be idempotent, got %d connect calls", strategy.connect)
	}
}

func TestMapper_Submit_InvalidJob_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(7)
	strategy := &startCountingStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	mapper.storage.job = proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}

	done := make(chan struct{})
	go func() {
		mapper.Submit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-missing",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 42,
		})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read error reply: %v", err)
	}
	<-done

	var payload struct {
		ID    float64 `json:"id"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("unmarshal error reply: %v", err)
	}
	if payload.ID != 42 {
		t.Fatalf("expected request id 42, got %v", payload.ID)
	}
	if payload.Error.Message != "Invalid job id" {
		t.Fatalf("expected invalid job error, got %q", payload.Error.Message)
	}
	if len(mapper.pending) != 0 {
		t.Fatalf("expected invalid submit not to create a pending entry")
	}
}

func TestMapper_OnResultAccepted_ExpiredUsesPreviousJob(t *testing.T) {
	bus := proxy.NewEventBus()
	events := make(chan proxy.Event, 1)
	bus.Subscribe(proxy.EventAccept, func(e proxy.Event) {
		events <- e
	})

	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(7)
	mapper := NewNonceMapper(1, &proxy.Config{}, &startCountingStrategy{})
	mapper.events = bus
	mapper.storage.job = proxy.Job{JobID: "job-new", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.prevJob = proxy.Job{JobID: "job-old", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.miners[miner.ID()] = miner
	if !mapper.storage.IsValidJobID("job-old") {
		t.Fatal("expected previous job to validate before result handling")
	}
	mapper.pending[9] = SubmitContext{
		RequestID: 42,
		MinerID:   miner.ID(),
		JobID:     "job-old",
		StartedAt: time.Now(),
	}

	mapper.OnResultAccepted(9, true, "")

	if got := mapper.storage.expired; got != 1 {
		t.Fatalf("expected one expired validation, got %d", got)
	}

	select {
	case event := <-events:
		if !event.Expired {
			t.Fatalf("expected expired share to be flagged")
		}
		if event.Job == nil || event.Job.JobID != "job-old" {
			t.Fatalf("expected previous job to be attached, got %+v", event.Job)
		}
	case <-time.After(time.Second):
		t.Fatal("expected accept event")
	}
}

func TestMapper_Submit_ExpiredJobUsesPreviousDifficulty(t *testing.T) {
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(9)

	strategy := &submitCaptureStrategy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	mapper.storage.job = proxy.Job{JobID: "job-new", Blob: strings.Repeat("0", 160), Target: "ffffffff"}
	mapper.storage.prevJob = proxy.Job{JobID: "job-old", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.miners[miner.ID()] = miner

	mapper.Submit(&proxy.SubmitEvent{
		Miner:     miner,
		JobID:     "job-old",
		Nonce:     "deadbeef",
		Result:    "hash",
		RequestID: 88,
	})

	ctx, ok := mapper.pending[strategy.seq]
	if !ok {
		t.Fatal("expected pending submit context for expired job")
	}
	want := mapper.storage.prevJob.DifficultyFromTarget()
	if ctx.Diff != want {
		t.Fatalf("expected previous-job difficulty %d, got %d", want, ctx.Diff)
	}
}

type submitCaptureStrategy struct {
	seq int64
}

func (s *submitCaptureStrategy) Connect() {}

func (s *submitCaptureStrategy) Submit(jobID, nonce, result, algo string) int64 {
	s.seq++
	return s.seq
}

func (s *submitCaptureStrategy) Disconnect() {}

func (s *submitCaptureStrategy) IsActive() bool { return true }

func TestMapper_OnResultAccepted_CustomDiffUsesEffectiveDifficulty(t *testing.T) {
	bus := proxy.NewEventBus()
	events := make(chan proxy.Event, 1)
	bus.Subscribe(proxy.EventAccept, func(e proxy.Event) {
		events <- e
	})

	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(8)
	mapper := NewNonceMapper(1, &proxy.Config{}, &startCountingStrategy{})
	mapper.events = bus
	mapper.storage.job = proxy.Job{JobID: "job-new", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.miners[miner.ID()] = miner
	mapper.pending[10] = SubmitContext{
		RequestID: 77,
		MinerID:   miner.ID(),
		JobID:     "job-new",
		Diff:      25000,
		StartedAt: time.Now(),
	}

	mapper.OnResultAccepted(10, true, "")

	select {
	case event := <-events:
		if event.Diff != 25000 {
			t.Fatalf("expected effective difficulty 25000, got %d", event.Diff)
		}
	case <-time.After(time.Second):
		t.Fatal("expected accept event")
	}
}

func TestMapper_OnResultAccepted_Bad(t *testing.T) {
	bus := proxy.NewEventBus()
	rejects := make(chan proxy.Event, 1)
	bus.Subscribe(proxy.EventReject, func(e proxy.Event) {
		rejects <- e
	})

	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(12)
	mapper := NewNonceMapper(1, &proxy.Config{}, &startCountingStrategy{})
	mapper.events = bus
	mapper.storage.job = proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.miners[miner.ID()] = miner
	mapper.pending[11] = SubmitContext{
		RequestID: 77,
		MinerID:   miner.ID(),
		JobID:     "job-1",
		StartedAt: time.Now(),
	}

	done := make(chan struct{})
	go func() {
		mapper.OnResultAccepted(11, false, "Low difficulty share")
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read reject reply: %v", err)
	}
	<-done

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("decode reject reply: %v", err)
	}
	if payload.Error.Message != "Low difficulty share" {
		t.Fatalf("expected pool rejection to be propagated, got %q", payload.Error.Message)
	}
	if len(mapper.pending) != 0 {
		t.Fatalf("expected reject reply to clear pending entry, got %d", len(mapper.pending))
	}

	select {
	case event := <-rejects:
		if event.Error != "Low difficulty share" {
			t.Fatalf("expected reject event to carry pool error, got %q", event.Error)
		}
		if event.Job == nil || event.Job.JobID != "job-1" {
			t.Fatalf("expected reject event to reference current job, got %+v", event.Job)
		}
	case <-time.After(time.Second):
		t.Fatal("expected reject event")
	}
}

func TestMapper_OnResultAccepted_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &startCountingStrategy{})
	mapper.OnResultAccepted(99, true, "")
	if len(mapper.pending) != 0 {
		t.Fatalf("expected unknown sequence to leave pending map untouched, got %d entries", len(mapper.pending))
	}
}

func TestNonceMapper_Submit_UnavailableStrategy_Bad(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(11)
	mapper := NewNonceMapper(1, &proxy.Config{}, unavailableSubmitStrategy{})
	mapper.storage.job = proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.storage.miners[miner.ID()] = miner

	done := make(chan struct{})
	go func() {
		mapper.Submit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-1",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 99,
		})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read unavailable reply: %v", err)
	}
	<-done

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("decode unavailable reply: %v", err)
	}
	if payload.Error.Message != "Proxy is unavailable, try again later" {
		t.Fatalf("expected unavailable reply, got %q", payload.Error.Message)
	}
	if len(mapper.pending) != 0 {
		t.Fatalf("expected unavailable submit not to create pending work, got %d", len(mapper.pending))
	}
}

func TestNonceSplitter_OnSubmit_Unavailable_Bad(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(12)
	splitter := NewNonceSplitter(&proxy.Config{}, proxy.NewEventBus(), nil)

	done := make(chan struct{})
	go func() {
		splitter.OnSubmit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-1",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 100,
		})
		close(done)
	}()

	line, err := bufio.NewReader(clientConn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read unavailable reply: %v", err)
	}
	<-done

	var payload struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("decode unavailable reply: %v", err)
	}
	if payload.Error.Message != "Proxy is unavailable, try again later" {
		t.Fatalf("expected unavailable reply, got %q", payload.Error.Message)
	}
}

type unavailableSubmitStrategy struct{}

func (unavailableSubmitStrategy) Connect()                                    {}
func (unavailableSubmitStrategy) Submit(string, string, string, string) int64 { return 0 }
func (unavailableSubmitStrategy) Disconnect()                                 {}
func (unavailableSubmitStrategy) IsActive() bool                              { return false }
