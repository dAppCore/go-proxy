package nicehash

import (
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func init() {
	proxy.RegisterSplitterFactory("nicehash", func(cfg *proxy.Config, events *proxy.EventBus) proxy.Splitter {
		return NewNonceSplitter(cfg, events, pool.NewStrategyFactory(cfg))
	})
}

// NewNonceSplitter creates a NiceHash splitter.
func NewNonceSplitter(cfg *proxy.Config, events *proxy.EventBus, factory pool.StrategyFactory) *NonceSplitter {
	if factory == nil {
		factory = pool.NewStrategyFactory(cfg)
	}
	return &NonceSplitter{
		byID:            make(map[int64]*NonceMapper),
		cfg:             cfg,
		events:          events,
		strategyFactory: factory,
	}
}

// Connect establishes the first mapper.
func (s *NonceSplitter) Connect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mappers) == 0 {
		s.addMapperLocked()
	}
	for _, mapper := range s.mappers {
		mapper.Start()
	}
}

// OnLogin assigns the miner to a mapper with a free slot.
func (s *NonceSplitter) OnLogin(event *proxy.LoginEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event.Miner.SetExtendedNiceHash(true)
	for _, mapper := range s.mappers {
		if mapper.Add(event.Miner) {
			s.byID[mapper.id] = mapper
			return
		}
	}
	mapper := s.addMapperLocked()
	if mapper != nil {
		_ = mapper.Add(event.Miner)
		s.byID[mapper.id] = mapper
	}
}

// OnSubmit forwards a share to the owning mapper.
func (s *NonceSplitter) OnSubmit(event *proxy.SubmitEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.RLock()
	mapper := s.byID[event.Miner.MapperID()]
	s.mu.RUnlock()
	if mapper != nil {
		mapper.Submit(event)
	}
}

// OnClose releases the miner slot.
func (s *NonceSplitter) OnClose(event *proxy.CloseEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.RLock()
	mapper := s.byID[event.Miner.MapperID()]
	s.mu.RUnlock()
	if mapper != nil {
		mapper.Remove(event.Miner)
	}
}

// GC removes empty mappers that have been idle.
func (s *NonceSplitter) GC() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	next := s.mappers[:0]
	for _, mapper := range s.mappers {
		free, dead, active := mapper.storage.SlotCount()
		if active == 0 && dead == 0 && now.Sub(mapper.lastUsed) > time.Minute {
			if mapper.strategy != nil {
				mapper.strategy.Disconnect()
			}
			delete(s.byID, mapper.id)
			_ = free
			continue
		}
		next = append(next, mapper)
	}
	s.mappers = next
}

// Tick is called once per second.
func (s *NonceSplitter) Tick(ticks uint64) {
	if s == nil {
		return
	}
	strategies := make([]pool.Strategy, 0, len(s.mappers))
	s.mu.RLock()
	for _, mapper := range s.mappers {
		if mapper == nil || mapper.strategy == nil {
			continue
		}
		strategies = append(strategies, mapper.strategy)
	}
	s.mu.RUnlock()
	for _, strategy := range strategies {
		if ticker, ok := strategy.(interface{ Tick(uint64) }); ok {
			ticker.Tick(ticks)
		}
	}
}

// Upstreams returns pool connection counts.
func (s *NonceSplitter) Upstreams() proxy.UpstreamStats {
	if s == nil {
		return proxy.UpstreamStats{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var stats proxy.UpstreamStats
	for _, mapper := range s.mappers {
		if mapper.strategy != nil && mapper.strategy.IsActive() {
			stats.Active++
		} else if mapper.suspended > 0 {
			stats.Error++
		}
	}
	stats.Total = uint64(len(s.mappers))
	return stats
}

func (s *NonceSplitter) addMapperLocked() *NonceMapper {
	id := s.seq
	s.seq++
	mapper := NewNonceMapper(id, s.cfg, nil)
	mapper.events = s.events
	mapper.lastUsed = time.Now()
	mapper.strategy = s.strategyFactory(mapper)
	s.mappers = append(s.mappers, mapper)
	if s.byID == nil {
		s.byID = make(map[int64]*NonceMapper)
	}
	s.byID[mapper.id] = mapper
	mapper.Start()
	return mapper
}

// NewNonceMapper creates a mapper for one upstream connection.
func NewNonceMapper(id int64, cfg *proxy.Config, strategy pool.Strategy) *NonceMapper {
	return &NonceMapper{
		id:       id,
		storage:  NewNonceStorage(),
		strategy: strategy,
		pending:  make(map[int64]SubmitContext),
		cfg:      cfg,
	}
}

// Start connects the mapper's upstream strategy once.
func (m *NonceMapper) Start() {
	if m == nil || m.strategy == nil {
		return
	}
	m.startOnce.Do(func() {
		m.lastUsed = time.Now()
		m.strategy.Connect()
	})
}

// Add assigns a miner to a free slot.
func (m *NonceMapper) Add(miner *proxy.Miner) bool {
	if m == nil || miner == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ok := m.storage.Add(miner)
	if ok {
		miner.SetMapperID(m.id)
		miner.SetExtendedNiceHash(true)
		m.lastUsed = time.Now()
		m.storage.mu.Lock()
		job := m.storage.job
		m.storage.mu.Unlock()
		if job.IsValid() {
			miner.SetCurrentJob(job)
		}
	}
	return ok
}

// Remove marks the miner slot as dead.
func (m *NonceMapper) Remove(miner *proxy.Miner) {
	if m == nil || miner == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storage.Remove(miner)
	miner.SetMapperID(-1)
	m.lastUsed = time.Now()
}

// Submit forwards the share to the pool.
func (m *NonceMapper) Submit(event *proxy.SubmitEvent) {
	if m == nil || event == nil || event.Miner == nil || m.strategy == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	jobID := event.JobID
	m.storage.mu.Lock()
	job := m.storage.job
	m.storage.mu.Unlock()
	if jobID == "" {
		jobID = job.JobID
	}
	valid := m.storage.IsValidJobID(jobID)
	if jobID == "" || !valid {
		m.rejectInvalidJobLocked(event, job)
		return
	}
	seq := m.strategy.Submit(jobID, event.Nonce, event.Result, event.Algo)
	m.pending[seq] = SubmitContext{
		RequestID: event.RequestID,
		MinerID:   event.Miner.ID(),
		JobID:     jobID,
		StartedAt: time.Now(),
	}
	m.lastUsed = time.Now()
}

func (m *NonceMapper) rejectInvalidJobLocked(event *proxy.SubmitEvent, job proxy.Job) {
	event.Miner.ReplyWithError(event.RequestID, "Invalid job id")
	if m.events != nil {
		jobCopy := job
		m.events.Dispatch(proxy.Event{Type: proxy.EventReject, Miner: event.Miner, Job: &jobCopy, Error: "Invalid job id"})
	}
}

// IsActive reports whether the mapper has received a valid job.
func (m *NonceMapper) IsActive() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// OnJob stores the current pool job and broadcasts it to active miners.
func (m *NonceMapper) OnJob(job proxy.Job) {
	if m == nil || !job.IsValid() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storage.SetJob(job)
	m.active = true
	m.suspended = 0
	m.lastUsed = time.Now()
}

// OnResultAccepted correlates a pool result back to the originating miner.
func (m *NonceMapper) OnResultAccepted(sequence int64, accepted bool, errorMessage string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	ctx, ok := m.pending[sequence]
	if ok {
		delete(m.pending, sequence)
	}
	m.storage.mu.Lock()
	miner := m.storage.miners[ctx.MinerID]
	job := m.storage.job
	prevJob := m.storage.prevJob
	m.storage.mu.Unlock()
	_ = m.storage.IsValidJobID(ctx.JobID)
	expired := ctx.JobID != "" && ctx.JobID == prevJob.JobID && ctx.JobID != job.JobID
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
			m.events.Dispatch(proxy.Event{Type: proxy.EventAccept, Miner: miner, Job: &job, Diff: job.DifficultyFromTarget(), Latency: latency, Expired: expired})
		}
		return
	}
	miner.ReplyWithError(ctx.RequestID, errorMessage)
	if m.events != nil {
		m.events.Dispatch(proxy.Event{Type: proxy.EventReject, Miner: miner, Job: &job, Diff: job.DifficultyFromTarget(), Error: errorMessage, Latency: latency})
	}
}

func (m *NonceMapper) OnDisconnect() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = false
	m.suspended++
}

// NewNonceStorage creates an empty slot table.
func NewNonceStorage() *NonceStorage {
	return &NonceStorage{miners: make(map[int64]*proxy.Miner)}
}

// Add finds the next free slot.
func (s *NonceStorage) Add(miner *proxy.Miner) bool {
	if s == nil || miner == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < 256; i++ {
		index := (s.cursor + i) % 256
		if s.slots[index] != 0 {
			continue
		}
		s.slots[index] = miner.ID()
		s.miners[miner.ID()] = miner
		miner.SetFixedByte(uint8(index))
		s.cursor = (index + 1) % 256
		return true
	}
	return false
}

// Remove marks a slot as dead.
func (s *NonceStorage) Remove(miner *proxy.Miner) {
	if s == nil || miner == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := int(miner.FixedByte())
	if index >= 0 && index < len(s.slots) && s.slots[index] == miner.ID() {
		s.slots[index] = -miner.ID()
	}
	delete(s.miners, miner.ID())
}

// SetJob replaces the current job and sends it to active miners.
func (s *NonceStorage) SetJob(job proxy.Job) {
	if s == nil || !job.IsValid() {
		return
	}
	s.mu.Lock()
	s.prevJob = s.job
	if s.prevJob.ClientID != job.ClientID {
		s.prevJob = proxy.Job{}
	}
	s.job = job
	for i := range s.slots {
		if s.slots[i] < 0 {
			s.slots[i] = 0
		}
	}
	miners := make([]*proxy.Miner, 0, len(s.miners))
	for _, miner := range s.miners {
		miners = append(miners, miner)
	}
	s.mu.Unlock()
	for _, miner := range miners {
		miner.ForwardJob(job, job.Algo)
	}
}

// IsValidJobID returns true if the id matches the current or previous job.
func (s *NonceStorage) IsValidJobID(id string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return false
	}
	if id == s.job.JobID {
		return true
	}
	if id == s.prevJob.JobID && s.prevJob.JobID != "" {
		s.expired++
		return true
	}
	return false
}

// SlotCount returns free, dead, and active counts.
func (s *NonceStorage) SlotCount() (free, dead, active int) {
	if s == nil {
		return 0, 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range s.slots {
		switch {
		case slot == 0:
			free++
		case slot < 0:
			dead++
		default:
			active++
		}
	}
	return
}
