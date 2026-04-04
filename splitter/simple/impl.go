package simple

import (
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func init() {
	proxy.RegisterSplitterFactory("simple", func(cfg *proxy.Config, events *proxy.EventBus) proxy.Splitter {
		return NewSimpleSplitter(cfg, events, pool.NewStrategyFactory(cfg))
	})
}

// NewSimpleSplitter creates the passthrough splitter.
func NewSimpleSplitter(cfg *proxy.Config, events *proxy.EventBus, factory pool.StrategyFactory) *SimpleSplitter {
	if factory == nil {
		factory = pool.NewStrategyFactory(cfg)
	}
	return &SimpleSplitter{
		active:  make(map[int64]*SimpleMapper),
		idle:    make(map[int64]*SimpleMapper),
		cfg:     cfg,
		events:  events,
		factory: factory,
	}
}

// Connect establishes any mapper strategies that already exist.
func (s *SimpleSplitter) Connect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, mapper := range s.active {
		if mapper.strategy != nil {
			mapper.strategy.Connect()
		}
	}
	for _, mapper := range s.idle {
		if mapper.strategy != nil {
			mapper.strategy.Connect()
		}
	}
}

// OnLogin creates or reclaims a mapper.
func (s *SimpleSplitter) OnLogin(event *proxy.LoginEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cfg.ReuseTimeout > 0 {
		for id, mapper := range s.idle {
			if mapper.strategy != nil && mapper.strategy.IsActive() {
				delete(s.idle, id)
				mapper.miner = event.Miner
				mapper.idleAt = time.Time{}
				mapper.stopped = false
				s.active[event.Miner.ID()] = mapper
				event.Miner.SetRouteID(mapper.id)
				if mapper.currentJob.IsValid() {
					event.Miner.SetCurrentJob(mapper.currentJob)
				}
				return
			}
		}
	}

	mapper := s.newMapperLocked()
	mapper.miner = event.Miner
	s.active[event.Miner.ID()] = mapper
	event.Miner.SetRouteID(mapper.id)
	if mapper.strategy != nil {
		mapper.strategy.Connect()
	}
}

// OnSubmit forwards the share to the owning mapper.
func (s *SimpleSplitter) OnSubmit(event *proxy.SubmitEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.Lock()
	mapper := s.active[event.Miner.ID()]
	s.mu.Unlock()
	if mapper != nil {
		mapper.Submit(event)
	}
}

// OnClose moves a mapper to the idle pool or stops it.
func (s *SimpleSplitter) OnClose(event *proxy.CloseEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mapper := s.active[event.Miner.ID()]
	if mapper == nil {
		return
	}
	delete(s.active, event.Miner.ID())
	mapper.miner = nil
	mapper.idleAt = time.Now()
	event.Miner.SetRouteID(-1)
	if s.cfg.ReuseTimeout > 0 {
		s.idle[mapper.id] = mapper
		return
	}
	mapper.stopped = true
	if mapper.strategy != nil {
		mapper.strategy.Disconnect()
	}
}

// GC removes expired idle mappers.
func (s *SimpleSplitter) GC() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, mapper := range s.idle {
		if mapper.stopped || (s.cfg.ReuseTimeout > 0 && now.Sub(mapper.idleAt) > time.Duration(s.cfg.ReuseTimeout)*time.Second) {
			if mapper.strategy != nil {
				mapper.strategy.Disconnect()
			}
			delete(s.idle, id)
		}
	}
}

// Tick advances timeout checks in simple mode.
func (s *SimpleSplitter) Tick(ticks uint64) {
	if s == nil {
		return
	}
	strategies := make([]pool.Strategy, 0, len(s.active)+len(s.idle))
	s.mu.Lock()
	for _, mapper := range s.active {
		if mapper != nil && mapper.strategy != nil {
			strategies = append(strategies, mapper.strategy)
		}
	}
	for _, mapper := range s.idle {
		if mapper != nil && mapper.strategy != nil {
			strategies = append(strategies, mapper.strategy)
		}
	}
	s.mu.Unlock()
	for _, strategy := range strategies {
		if ticker, ok := strategy.(interface{ Tick(uint64) }); ok {
			ticker.Tick(ticks)
		}
	}
	s.GC()
}

// Upstreams returns active/idle/error counts.
func (s *SimpleSplitter) Upstreams() proxy.UpstreamStats {
	if s == nil {
		return proxy.UpstreamStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var stats proxy.UpstreamStats
	stats.Active = uint64(len(s.active))
	stats.Sleep = uint64(len(s.idle))
	stats.Total = stats.Active + stats.Sleep
	return stats
}

func (s *SimpleSplitter) newMapperLocked() *SimpleMapper {
	id := s.seq
	s.seq++
	mapper := &SimpleMapper{
		id:      id,
		events:  s.events,
		pending: make(map[int64]submitContext),
	}
	mapper.strategy = s.factory(mapper)
	if mapper.strategy == nil {
		mapper.strategy = s.factory(mapper)
	}
	return mapper
}

// Submit forwards a share to the pool.
func (m *SimpleMapper) Submit(event *proxy.SubmitEvent) {
	if m == nil || event == nil || m.strategy == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := m.strategy.Submit(event.JobID, event.Nonce, event.Result, event.Algo)
	m.pending[seq] = submitContext{RequestID: event.RequestID, StartedAt: time.Now()}
}

// OnJob forwards the latest pool job to the active miner.
func (m *SimpleMapper) OnJob(job proxy.Job) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.currentJob = job
	miner := m.miner
	m.mu.Unlock()
	if miner == nil {
		return
	}
	miner.ForwardJob(job, job.Algo)
}

// OnResultAccepted forwards result status to the miner.
func (m *SimpleMapper) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	ctx, ok := m.pending[sequence]
	if ok {
		delete(m.pending, sequence)
	}
	miner := m.miner
	m.mu.Unlock()
	if !ok || miner == nil {
		return
	}
	latency := uint16(0)
	if !ctx.StartedAt.IsZero() {
		elapsed := time.Since(ctx.StartedAt).Milliseconds()
		if elapsed > int64(^uint16(0)) {
			latency = ^uint16(0)
		} else {
			latency = uint16(elapsed)
		}
	}
	if accepted {
		miner.Success(ctx.RequestID, "OK")
		if m.events != nil {
			job := miner.CurrentJob()
			m.events.Dispatch(proxy.Event{Type: proxy.EventAccept, Miner: miner, Diff: job.DifficultyFromTarget(), Job: &job, Latency: latency})
		}
		return
	}
	miner.ReplyWithError(ctx.RequestID, errorMessage)
	if m.events != nil {
		job := miner.CurrentJob()
		m.events.Dispatch(proxy.Event{Type: proxy.EventReject, Miner: miner, Diff: job.DifficultyFromTarget(), Job: &job, Error: errorMessage, Latency: latency})
	}
}

// OnDisconnect marks the mapper as disconnected.
func (m *SimpleMapper) OnDisconnect() {
	if m == nil {
		return
	}
	m.stopped = true
}
