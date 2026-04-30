package simple

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

type activeStrategy struct{}

func (a activeStrategy) Connect()                                    {}
func (a activeStrategy) Submit(string, string, string, string) int64 { return 0 }
func (a activeStrategy) Disconnect()                                 {}
func (a activeStrategy) IsActive() bool                              { return true }

type submitRecordingStrategy struct {
	submits int
}

func (s *submitRecordingStrategy) Connect() {}

func (s *submitRecordingStrategy) Submit(string, string, string, string) int64 {
	s.submits++
	return int64(s.submits)
}

func (s *submitRecordingStrategy) Disconnect() {}

func (s *submitRecordingStrategy) IsActive() bool { return true }

func TestSimpleMapper_New_Good(t *testing.T) {
	strategy := activeStrategy{}
	mapper := NewSimpleMapper(7, strategy)

	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.id != 7 {
		t.Fatalf("expected mapper id 7, got %d", mapper.id)
	}
	if mapper.strategy != strategy {
		t.Fatalf("expected strategy to be stored")
	}
	if mapper.pending == nil {
		t.Fatal("expected pending map to be initialised")
	}
}

func TestImpl_SimpleSplitter_OnLogin_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	miner := &proxy.Miner{}
	job := proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper := &SimpleMapper{
		id:         7,
		strategy:   activeStrategy{},
		currentJob: job,
		idleAt:     time.Now(),
	}
	splitter.idle[mapper.id] = mapper

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if miner.RouteID() != mapper.id {
		t.Fatalf("expected reclaimed mapper route id %d, got %d", mapper.id, miner.RouteID())
	}
	if got := miner.CurrentJob().JobID; got != job.JobID {
		t.Fatalf("expected current job to be restored on reuse, got %q", got)
	}
}

func TestImpl_SimpleSplitter_OnLogin_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	miner := &proxy.Miner{}
	expired := &SimpleMapper{
		id:       7,
		strategy: activeStrategy{},
		idleAt:   time.Now().Add(-time.Minute),
	}
	splitter.idle[expired.id] = expired

	splitter.OnLogin(&proxy.LoginEvent{Miner: miner})

	if miner.RouteID() == expired.id {
		t.Fatalf("expected expired mapper not to be reclaimed")
	}
	if miner.RouteID() != 0 {
		t.Fatalf("expected a new mapper to be allocated, got route id %d", miner.RouteID())
	}
	if len(splitter.active) != 1 {
		t.Fatalf("expected one active mapper, got %d", len(splitter.active))
	}
	if len(splitter.idle) != 1 {
		t.Fatalf("expected expired mapper to remain idle until GC, got %d idle mappers", len(splitter.idle))
	}
}

func TestSimpleSplitter_OnSubmit_UsesRouteID_Good(t *testing.T) {
	strategy := &submitRecordingStrategy{}
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, nil)
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(21)
	miner.SetRouteID(7)

	mapper := &SimpleMapper{
		id:         7,
		miner:      miner,
		currentJob: proxy.Job{JobID: "job-1", Blob: "blob", Target: "b88d0600"},
		strategy:   strategy,
		pending:    make(map[int64]submitContext),
	}
	splitter.active[99] = mapper

	splitter.OnSubmit(&proxy.SubmitEvent{
		Miner:     miner,
		JobID:     "job-1",
		Nonce:     "deadbeef",
		Result:    "hash",
		RequestID: 11,
	})

	if strategy.submits != 1 {
		t.Fatalf("expected one submit routed by route id, got %d", strategy.submits)
	}
	if len(mapper.pending) != 1 {
		t.Fatalf("expected routed submit to create one pending entry, got %d", len(mapper.pending))
	}
}

func TestImpl_SimpleSplitter_Upstreams_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: activeStrategy{}}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: activeStrategy{}, idleAt: time.Now()}

	stats := splitter.Upstreams()

	if stats.Active != 1 {
		t.Fatalf("expected one active upstream, got %d", stats.Active)
	}
	if stats.Sleep != 1 {
		t.Fatalf("expected one sleeping upstream, got %d", stats.Sleep)
	}
	if stats.Error != 0 {
		t.Fatalf("expected no error upstreams, got %d", stats.Error)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total upstreams to be 2, got %d", stats.Total)
	}
}

func TestImpl_SimpleSplitter_Upstreams_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	splitter.active[1] = &SimpleMapper{id: 1, strategy: activeStrategy{}, stopped: true}
	splitter.idle[2] = &SimpleMapper{id: 2, strategy: activeStrategy{}, stopped: true, idleAt: time.Now()}

	stats := splitter.Upstreams()

	if stats.Active != 0 {
		t.Fatalf("expected no active upstreams, got %d", stats.Active)
	}
	if stats.Sleep != 0 {
		t.Fatalf("expected no sleeping upstreams, got %d", stats.Sleep)
	}
	if stats.Error != 2 {
		t.Fatalf("expected both upstreams to be counted as error, got %d", stats.Error)
	}
	if stats.Total != 2 {
		t.Fatalf("expected total upstreams to be 2, got %d", stats.Total)
	}
}

func TestSimpleSplitter_Upstreams_RecoveryResetsStopped_Good(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{ReuseTimeout: 30}, nil, func(listener pool.StratumListener) pool.Strategy {
		return activeStrategy{}
	})
	mapper := &SimpleMapper{id: 1, strategy: activeStrategy{}, stopped: true}
	splitter.active[1] = mapper

	before := splitter.Upstreams()
	if before.Error != 1 {
		t.Fatalf("expected disconnected mapper to count as error, got %+v", before)
	}

	mapper.OnJob(proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"})

	after := splitter.Upstreams()
	if after.Active != 1 {
		t.Fatalf("expected recovered mapper to count as active, got %+v", after)
	}
	if after.Error != 0 {
		t.Fatalf("expected recovered mapper not to remain in error, got %+v", after)
	}
}

type discardConn struct{}

func (discardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return nil }
func (discardConn) RemoteAddr() net.Addr             { return nil }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

func TestImpl_SimpleSplitter_OnLogin_Bad(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, func(pool.StratumListener) pool.Strategy { return activeStrategy{} })
	splitter.OnLogin(nil)
	if len(splitter.active) != 0 {
		t.Fatalf("expected nil login event ignored, active=%d", len(splitter.active))
	}
}

func TestImpl_SimpleSplitter_OnSubmit_Good(t *testing.T) {
	strategy := &submitRecordingStrategy{}
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	miner.SetRouteID(7)
	mapper := &SimpleMapper{id: 7, miner: miner, currentJob: proxy.Job{JobID: "job-1"}, strategy: strategy, pending: make(map[int64]submitContext)}
	splitter.active[miner.ID()] = mapper
	splitter.OnSubmit(&proxy.SubmitEvent{Miner: miner, JobID: "job-1", RequestID: 9})
	if strategy.submits != 1 || len(mapper.pending) != 1 {
		t.Fatalf("expected submit forwarded and pending recorded, submits=%d pending=%d", strategy.submits, len(mapper.pending))
	}
}

func TestImpl_SimpleSplitter_OnSubmit_Bad(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)
	splitter.OnSubmit(nil)
	if len(splitter.active) != 0 {
		t.Fatalf("expected nil submit ignored, active=%d", len(splitter.active))
	}
}

func TestImpl_SimpleSplitter_OnSubmit_Ugly(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetRouteID(99)
	splitter.OnSubmit(&proxy.SubmitEvent{Miner: miner, JobID: "job-1", RequestID: 3})
	if miner.TX() == 0 {
		t.Fatal("expected unavailable route submit to reply with an error")
	}
}

func TestImpl_SimpleSplitter_Upstreams_Bad(t *testing.T) {
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)
	splitter.active[1] = &SimpleMapper{strategy: activeStrategy{}, stopped: true}
	stats := splitter.Upstreams()
	if stats.Error != 1 || stats.Total != 1 {
		t.Fatalf("expected stopped active mapper counted as error, got %+v", stats)
	}
}

func TestImpl_SimpleMapper_Submit_Good(t *testing.T) {
	strategy := &submitRecordingStrategy{}
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	mapper := NewSimpleMapper(1, strategy)
	mapper.currentJob = proxy.Job{JobID: "job-1", Target: "b88d0600"}
	mapper.Submit(&proxy.SubmitEvent{Miner: miner, JobID: "job-1", RequestID: 4})
	if len(mapper.pending) != 1 || strategy.submits != 1 {
		t.Fatalf("expected pending submit, pending=%d submits=%d", len(mapper.pending), strategy.submits)
	}
}

func TestImpl_SimpleMapper_Submit_Bad(t *testing.T) {
	mapper := NewSimpleMapper(1, nil)
	mapper.Submit(nil)
	if len(mapper.pending) != 0 {
		t.Fatalf("expected nil submit ignored without pending entries, got %d", len(mapper.pending))
	}
}

func TestImpl_SimpleMapper_Submit_Ugly(t *testing.T) {
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	mapper := NewSimpleMapper(1, &submitRecordingStrategy{})
	mapper.currentJob = proxy.Job{JobID: "current"}
	mapper.prevJob = proxy.Job{JobID: "previous", Target: "b88d0600"}
	mapper.Submit(&proxy.SubmitEvent{Miner: miner, JobID: "previous", RequestID: 5})
	if len(mapper.pending) != 1 {
		t.Fatalf("expected previous job submit accepted, pending=%d", len(mapper.pending))
	}
}

func TestImpl_SimpleMapper_OnJob_Good(t *testing.T) {
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	mapper := NewSimpleMapper(1, activeStrategy{})
	mapper.miner = miner
	job := proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.OnJob(job)
	if mapper.currentJob.JobID != "job-1" || miner.CurrentJob().JobID != "job-1" {
		t.Fatalf("expected job stored and forwarded, mapper=%+v minerJob=%+v", mapper.currentJob, miner.CurrentJob())
	}
}

func TestImpl_SimpleMapper_OnJob_Bad(t *testing.T) {
	mapper := NewSimpleMapper(1, activeStrategy{})
	mapper.OnJob(proxy.Job{})
	if mapper.currentJob.JobID != "" {
		t.Fatalf("expected invalid job ignored, got %+v", mapper.currentJob)
	}
}

func TestImpl_SimpleMapper_OnJob_Ugly(t *testing.T) {
	mapper := NewSimpleMapper(1, activeStrategy{})
	mapper.currentJob = proxy.Job{JobID: "prev", ClientID: "session-1"}
	mapper.OnJob(proxy.Job{JobID: "next", ClientID: "session-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"})
	if mapper.prevJob.JobID != "prev" || mapper.currentJob.JobID != "next" {
		t.Fatalf("expected previous job retained, prev=%+v current=%+v", mapper.prevJob, mapper.currentJob)
	}
}

func TestImpl_SimpleMapper_OnResultAccepted_Good(t *testing.T) {
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	mapper := NewSimpleMapper(1, activeStrategy{})
	mapper.miner = miner
	mapper.currentJob = proxy.Job{JobID: "job-1"}
	mapper.pending[3] = submitContext{RequestID: 9, Diff: 64, JobID: "job-1", StartedAt: time.Now()}
	mapper.OnResultAccepted(3, true, "")
	if len(mapper.pending) != 0 || miner.TX() == 0 {
		t.Fatalf("expected accepted result to clear pending and reply, pending=%d tx=%d", len(mapper.pending), miner.TX())
	}
}

func TestImpl_SimpleMapper_OnDisconnect_Bad(t *testing.T) {
	var mapper *SimpleMapper
	mapper.OnDisconnect()
	if mapper != nil {
		t.Fatal("expected nil mapper to remain nil")
	}
}

func TestImpl_SimpleMapper_OnDisconnect_Ugly(t *testing.T) {
	mapper := NewSimpleMapper(1, activeStrategy{})
	mapper.OnDisconnect()
	mapper.OnDisconnect()
	if !mapper.stopped {
		t.Fatal("expected repeated disconnect to leave mapper stopped")
	}
}

func TestSimpleMapper_OnResultAccepted_Expired(t *testing.T) {
	bus := proxy.NewEventBus()
	events := make(chan proxy.Event, 1)
	var once sync.Once
	bus.Subscribe(proxy.EventAccept, func(e proxy.Event) {
		once.Do(func() {
			events <- e
		})
	})

	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	mapper := &SimpleMapper{
		miner:      miner,
		currentJob: proxy.Job{JobID: "job-new", Blob: "blob-new", Target: "b88d0600"},
		prevJob:    proxy.Job{JobID: "job-old", Blob: "blob-old", Target: "b88d0600"},
		events:     bus,
		pending: map[int64]submitContext{
			7: {RequestID: 9, StartedAt: time.Now(), JobID: "job-old"},
		},
	}

	mapper.OnResultAccepted(7, true, "")

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

func TestSimpleMapper_OnResultAccepted_CustomDiffUsesEffectiveDifficulty(t *testing.T) {
	bus := proxy.NewEventBus()
	events := make(chan proxy.Event, 1)
	var once sync.Once
	bus.Subscribe(proxy.EventAccept, func(e proxy.Event) {
		once.Do(func() {
			events <- e
		})
	})

	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(2)
	job := proxy.Job{JobID: "job-new", Blob: "blob-new", Target: "b88d0600"}
	mapper := &SimpleMapper{
		miner:      miner,
		currentJob: job,
		events:     bus,
		pending: map[int64]submitContext{
			8: {
				RequestID: 10,
				Diff:      25000,
				StartedAt: time.Now(),
				JobID:     "job-new",
			},
		},
	}

	mapper.OnResultAccepted(8, true, "")

	select {
	case event := <-events:
		if event.Diff != 25000 {
			t.Fatalf("expected effective difficulty 25000, got %d", event.Diff)
		}
	case <-time.After(time.Second):
		t.Fatal("expected accept event")
	}
}

func TestImpl_SimpleMapper_OnResultAccepted_Bad(t *testing.T) {
	bus := proxy.NewEventBus()
	rejects := make(chan proxy.Event, 1)
	bus.Subscribe(proxy.EventReject, func(e proxy.Event) {
		rejects <- e
	})

	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(5)
	mapper := &SimpleMapper{
		miner:      miner,
		currentJob: proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"},
		events:     bus,
		pending:    map[int64]submitContext{7: {RequestID: 44, StartedAt: time.Now(), JobID: "job-1"}},
	}

	done := make(chan struct{})
	go func() {
		mapper.OnResultAccepted(7, false, "Low difficulty share")
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

func TestImpl_SimpleMapper_OnResultAccepted_Ugly(t *testing.T) {
	mapper := &SimpleMapper{}
	mapper.OnResultAccepted(999, true, "")
	if len(mapper.pending) != 0 {
		t.Fatalf("expected unknown sequence to leave pending map untouched, got %d entries", len(mapper.pending))
	}
}

func TestSimpleMapper_Submit_UnavailableStrategy_Bad(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(31)
	mapper := NewSimpleMapper(7, unavailableSubmitStrategy{})
	mapper.currentJob = proxy.Job{JobID: "job-1", Blob: strings.Repeat("0", 160), Target: "b88d0600"}
	mapper.miner = miner

	done := make(chan struct{})
	go func() {
		mapper.Submit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-1",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 77,
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

func TestSimpleSplitter_OnSubmit_Unavailable_Bad(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	miner.SetID(32)
	miner.SetRouteID(7)
	splitter := NewSimpleSplitter(&proxy.Config{}, nil, nil)

	done := make(chan struct{})
	go func() {
		splitter.OnSubmit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-1",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 78,
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

func TestSimpleMapper_OnJob_PreservesPreviousJobForSamePoolSession_Good(t *testing.T) {
	mapper := &SimpleMapper{
		currentJob: proxy.Job{JobID: "job-1", Blob: "blob-1", ClientID: "session-a"},
	}

	mapper.OnJob(proxy.Job{JobID: "job-2", Blob: strings.Repeat("0", 160), Target: "b88d0600", ClientID: "session-a"})

	if mapper.currentJob.JobID != "job-2" {
		t.Fatalf("expected current job to roll forward, got %q", mapper.currentJob.JobID)
	}
	if mapper.prevJob.JobID != "job-1" {
		t.Fatalf("expected previous job to remain available within one pool session, got %q", mapper.prevJob.JobID)
	}
}

func TestSimpleMapper_OnJob_ResetsPreviousJobAcrossPoolSessions_Ugly(t *testing.T) {
	mapper := &SimpleMapper{
		currentJob: proxy.Job{JobID: "job-1", Blob: "blob-1", ClientID: "session-a"},
		prevJob:    proxy.Job{JobID: "job-0", Blob: "blob-0", ClientID: "session-a"},
	}

	mapper.OnJob(proxy.Job{JobID: "job-2", Blob: strings.Repeat("0", 160), Target: "b88d0600", ClientID: "session-b"})

	if mapper.currentJob.JobID != "job-2" {
		t.Fatalf("expected current job to advance after session change, got %q", mapper.currentJob.JobID)
	}
	if mapper.prevJob.JobID != "" {
		t.Fatalf("expected previous job history to reset on new pool session, got %q", mapper.prevJob.JobID)
	}
}

func TestSimpleMapper_OnJob_ResetsPreviousJobWithoutClientID_Bad(t *testing.T) {
	mapper := &SimpleMapper{
		currentJob: proxy.Job{JobID: "job-1", Blob: "blob-1"},
		prevJob:    proxy.Job{JobID: "job-0", Blob: "blob-0"},
	}

	mapper.OnJob(proxy.Job{JobID: "job-2", Blob: strings.Repeat("0", 160), Target: "b88d0600"})

	if mapper.currentJob.JobID != "job-2" {
		t.Fatalf("expected current job to advance, got %q", mapper.currentJob.JobID)
	}
	if mapper.prevJob.JobID != "" {
		t.Fatalf("expected previous job history to reset without stable client ids, got %q", mapper.prevJob.JobID)
	}
}

func TestSimpleMapper_Submit_InvalidJob_Good(t *testing.T) {
	minerConn, clientConn := net.Pipe()
	defer minerConn.Close()
	defer clientConn.Close()

	miner := proxy.NewMiner(minerConn, 3333, nil)
	mapper := &SimpleMapper{
		miner:      miner,
		currentJob: proxy.Job{JobID: "job-1", Blob: "blob", Target: "b88d0600"},
		prevJob:    proxy.Job{JobID: "job-0", Blob: "blob", Target: "b88d0600"},
		strategy:   activeStrategy{},
		pending:    make(map[int64]submitContext),
	}

	done := make(chan struct{})
	go func() {
		mapper.Submit(&proxy.SubmitEvent{
			Miner:     miner,
			JobID:     "job-missing",
			Nonce:     "deadbeef",
			Result:    "hash",
			RequestID: 9,
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
	if payload.ID != 9 {
		t.Fatalf("expected request id 9, got %v", payload.ID)
	}
	if payload.Error.Message != "Invalid job id" {
		t.Fatalf("expected invalid job error, got %q", payload.Error.Message)
	}
	if len(mapper.pending) != 0 {
		t.Fatalf("expected invalid submit not to create a pending entry")
	}
}
