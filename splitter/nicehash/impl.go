package nicehash

import (
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func init() {
	proxy.RegisterSplitterFactory("nicehash", func(config *proxy.Config, eventBus *proxy.EventBus) proxy.Splitter {
		return NewNonceSplitter(config, eventBus, pool.NewStrategyFactory(config))
	})
}

// NewNonceSplitter creates a NiceHash splitter.
func NewNonceSplitter(config *proxy.Config, eventBus *proxy.EventBus, factory pool.StrategyFactory) *NonceSplitter {
	if factory == nil {
		factory = pool.NewStrategyFactory(config)
	}
	return &NonceSplitter{
		mapperByID:      make(map[int64]*NonceMapper),
		config:          config,
		events:          eventBus,
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
			s.mapperByID[mapper.id] = mapper
			return
		}
	}
	mapper := s.addMapperLocked()
	if mapper != nil {
		_ = mapper.Add(event.Miner)
		s.mapperByID[mapper.id] = mapper
	}
}

// OnSubmit forwards a share to the owning mapper.
func (s *NonceSplitter) OnSubmit(event *proxy.SubmitEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.RLock()
	mapper := s.mapperByID[event.Miner.MapperID()]
	s.mu.RUnlock()
	if mapper != nil {
		mapper.Submit(event)
		return
	}
	rejectUnavailableSubmit(s.events, event)
}

// OnClose releases the miner slot.
func (s *NonceSplitter) OnClose(event *proxy.CloseEvent) {
	if s == nil || event == nil || event.Miner == nil {
		return
	}
	s.mu.RLock()
	mapper := s.mapperByID[event.Miner.MapperID()]
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
		if mapper == nil || mapper.storage == nil {
			continue
		}
		free, dead, active := mapper.storage.SlotCount()
		if active == 0 && now.Sub(mapper.lastUsed) > time.Minute {
			if mapper.strategy != nil {
				mapper.strategy.Disconnect()
			}
			delete(s.mapperByID, mapper.id)
			_ = free
			_ = dead
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
		} else if mapper.suspended > 0 || !mapper.active {
			stats.Error++
		}
	}
	stats.Total = stats.Active + stats.Sleep + stats.Error
	return stats
}

// Disconnect closes all upstream pool connections and forgets the current mapper set.
func (s *NonceSplitter) Disconnect() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, mapper := range s.mappers {
		if mapper != nil && mapper.strategy != nil {
			mapper.strategy.Disconnect()
		}
	}
	s.mappers = nil
	s.mapperByID = make(map[int64]*NonceMapper)
}

// ReloadPools reconnects each mapper strategy using the updated pool list.
//
//	s.ReloadPools()
func (s *NonceSplitter) ReloadPools() {
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
		if reloadable, ok := strategy.(pool.ReloadableStrategy); ok {
			reloadable.ReloadPools()
		}
	}
}

func (s *NonceSplitter) addMapperLocked() *NonceMapper {
	id := s.nextMapperID
	s.nextMapperID++
	mapper := NewNonceMapper(id, s.config, nil)
	mapper.events = s.events
	mapper.lastUsed = time.Now()
	mapper.strategy = s.strategyFactory(mapper)
	s.mappers = append(s.mappers, mapper)
	if s.mapperByID == nil {
		s.mapperByID = make(map[int64]*NonceMapper)
	}
	s.mapperByID[mapper.id] = mapper
	mapper.Start()
	return mapper
}

// NewNonceMapper creates a mapper for one upstream connection.
func NewNonceMapper(id int64, config *proxy.Config, strategy pool.Strategy) *NonceMapper {
	return &NonceMapper{
		id:       id,
		storage:  NewNonceStorage(),
		strategy: strategy,
		pending:  make(map[int64]SubmitContext),
		config:   config,
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
		rejectUnavailableSubmit(m.events, event)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	jobID := event.JobID
	m.storage.mu.Lock()
	job := m.storage.job
	prevJob := m.storage.prevJob
	m.storage.mu.Unlock()
	if jobID == "" {
		jobID = job.JobID
	}
	valid := m.storage.IsValidJobID(jobID)
	if jobID == "" || !valid {
		m.rejectInvalidJobLocked(event, job)
		return
	}
	submissionJob := job
	if jobID == prevJob.JobID && prevJob.JobID != "" {
		submissionJob = prevJob
	}
	seq := m.strategy.Submit(jobID, event.Nonce, event.Result, event.Algo)
	if seq == 0 {
		m.rejectUnavailableLocked(event, submissionJob)
		return
	}
	m.pending[seq] = SubmitContext{
		RequestID: event.RequestID,
		MinerID:   event.Miner.ID(),
		JobID:     jobID,
		Diff:      proxy.EffectiveShareDifficulty(submissionJob, event.Miner),
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

func (m *NonceMapper) rejectUnavailableLocked(event *proxy.SubmitEvent, job proxy.Job) {
	if event == nil || event.Miner == nil {
		return
	}
	event.Miner.ReplyWithError(event.RequestID, "Proxy is unavailable, try again later")
	if m.events != nil {
		jobCopy := job
		m.events.Dispatch(proxy.Event{
			Type:  proxy.EventReject,
			Miner: event.Miner,
			Job:   &jobCopy,
			Diff:  proxy.EffectiveShareDifficulty(job, event.Miner),
			Error: "Proxy is unavailable, try again later",
		})
	}
}

func rejectUnavailableSubmit(events *proxy.EventBus, event *proxy.SubmitEvent) {
	if event == nil || event.Miner == nil {
		return
	}
	event.Miner.ReplyWithError(event.RequestID, "Proxy is unavailable, try again later")
	if events != nil {
		events.Dispatch(proxy.Event{
			Type:  proxy.EventReject,
			Miner: event.Miner,
			Error: "Proxy is unavailable, try again later",
		})
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
	job, expired := resolveSubmissionJob(ctx.JobID, job, prevJob)
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
			m.events.Dispatch(proxy.Event{Type: proxy.EventAccept, Miner: miner, Job: &job, Diff: ctx.Diff, Latency: latency, Expired: expired})
		}
		return
	}
	miner.ReplyWithError(ctx.RequestID, errorMessage)
	if m.events != nil {
		m.events.Dispatch(proxy.Event{Type: proxy.EventReject, Miner: miner, Job: &job, Diff: ctx.Diff, Error: errorMessage, Latency: latency})
	}
}

func resolveSubmissionJob(jobID string, currentJob, previousJob proxy.Job) (proxy.Job, bool) {
	if jobID != "" && jobID == previousJob.JobID && jobID != currentJob.JobID {
		return previousJob, true
	}
	return currentJob, false
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

// NewNonceStorage creates a 256-slot table ready for round-robin miner allocation.
//
//	storage := nicehash.NewNonceStorage()
func NewNonceStorage() *NonceStorage {
	return &NonceStorage{miners: make(map[int64]*proxy.Miner)}
}

// Add assigns the next free slot, such as 0x2a, to one miner.
//
//	ok := storage.Add(&proxy.Miner{})
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

// Remove marks one miner's slot as dead until the next SetJob call.
//
//	storage.Remove(miner)
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

// SetJob broadcasts one pool job to all active miners and clears dead slots.
//
//	storage.SetJob(proxy.Job{Blob: strings.Repeat("0", 160), JobID: "job-1"})
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

// IsValidJobID accepts the current job, or the immediately previous one after a pool roll.
//
//	if !storage.IsValidJobID("job-1") { return }
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

// SlotCount returns free, dead, and active slot counts such as 254, 1, 1.
//
//	free, dead, active := storage.SlotCount()
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
