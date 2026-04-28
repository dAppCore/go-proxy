package nicehash

import (
	"strings"
	"testing"
	"time"

	"dappco.re/go/proxy"
	"dappco.re/go/proxy/pool"
)

func TestNonceStorage_NewNonceStorage_Good(t *testing.T) {
	storage := NewNonceStorage()
	if storage == nil {
		t.Fatal("expected storage")
	}
	free, dead, active := storage.SlotCount()
	if free != 256 || dead != 0 || active != 0 {
		t.Fatalf("expected empty storage, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestImpl_NonceStorage_IsValidJobID_Bad(t *testing.T) {
	storage := NewNonceStorage()
	if storage.IsValidJobID("") {
		t.Fatal("expected empty job id to be invalid")
	}
	if storage.IsValidJobID("missing") {
		t.Fatal("expected unknown job id to be invalid")
	}
}

func TestImpl_NonceStorage_IsValidJobID_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	blob := strings.Repeat("0", 160)
	storage.SetJob(proxy.Job{JobID: "job-1", Blob: blob, Target: "b88d0600", ClientID: "session-1"})
	storage.SetJob(proxy.Job{JobID: "job-2", Blob: blob, Target: "b88d0600", ClientID: "session-1"})
	if !storage.IsValidJobID("job-1") {
		t.Fatal("expected previous job id to be accepted")
	}
	if storage.expired != 1 {
		t.Fatalf("expected expired counter to increment, got %d", storage.expired)
	}
}

func TestImpl_NonceStorage_SlotCount_Good(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !storage.Add(miner) {
		t.Fatal("expected miner to occupy a slot")
	}
	free, dead, active := storage.SlotCount()
	if free != 255 || dead != 0 || active != 1 {
		t.Fatalf("expected one active slot, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestImpl_NonceStorage_SlotCount_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !storage.Add(miner) {
		t.Fatal("expected miner to occupy a slot")
	}
	storage.Remove(miner)
	free, dead, active := storage.SlotCount()
	if free != 255 || dead != 1 || active != 0 {
		t.Fatalf("expected one dead slot after removal, got free=%d dead=%d active=%d", free, dead, active)
	}
}

func nicehashImplJob(id string) proxy.Job {
	return proxy.Job{JobID: id, Blob: strings.Repeat("0", 160), Target: "b88d0600", ClientID: "session-1"}
}

func TestImpl_NonceMapper_Start_Good(t *testing.T) {
	strategy := &nicehashStrategySpy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	mapper.Start()
	if strategy.connects != 1 || mapper.lastUsed.IsZero() {
		t.Fatalf("expected mapper start to connect once, connects=%d lastUsed=%v", strategy.connects, mapper.lastUsed)
	}
}

func TestImpl_NonceMapper_Start_Bad(t *testing.T) {
	var mapper *NonceMapper
	mapper.Start()
	if mapper != nil {
		t.Fatal("expected nil mapper to remain nil")
	}
}

func TestImpl_NonceMapper_Start_Ugly(t *testing.T) {
	strategy := &nicehashStrategySpy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	mapper.Start()
	mapper.Start()
	if strategy.connects != 1 {
		t.Fatalf("expected start once guard, connects=%d", strategy.connects)
	}
}

func TestImpl_NonceMapper_Add_Good(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.storage.SetJob(nicehashImplJob("job-1"))
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !mapper.Add(miner) || miner.MapperID() != 7 || !miner.ExtendedNiceHash() {
		t.Fatalf("expected miner assigned to mapper, mapperID=%d extNH=%v", miner.MapperID(), miner.ExtendedNiceHash())
	}
}

func TestImpl_NonceMapper_Add_Bad(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, &nicehashStrategySpy{})
	if mapper.Add(nil) {
		t.Fatal("expected nil miner add to fail")
	}
}

func TestImpl_NonceMapper_Add_Ugly(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, &nicehashStrategySpy{})
	for i := 1; i <= 256; i++ {
		miner := &proxy.Miner{}
		miner.SetID(int64(i))
		if !mapper.Add(miner) {
			t.Fatalf("expected slot %d to be assigned", i)
		}
	}
	extra := &proxy.Miner{}
	extra.SetID(300)
	if mapper.Add(extra) {
		t.Fatal("expected full mapper add to fail")
	}
}

func TestImpl_NonceMapper_Remove_Good(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, &nicehashStrategySpy{})
	miner := &proxy.Miner{}
	miner.SetID(1)
	mapper.Add(miner)
	mapper.Remove(miner)
	if miner.MapperID() != -1 {
		t.Fatalf("expected mapper id reset after remove, got %d", miner.MapperID())
	}
}

func TestImpl_NonceMapper_Remove_Bad(t *testing.T) {
	mapper := NewNonceMapper(7, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.Remove(nil)
	if !mapper.lastUsed.IsZero() {
		t.Fatalf("expected nil remove to leave mapper untouched, lastUsed=%v", mapper.lastUsed)
	}
}

func TestImpl_NonceMapper_Remove_Ugly(t *testing.T) {
	var mapper *NonceMapper
	mapper.Remove(&proxy.Miner{})
	if mapper != nil {
		t.Fatal("expected nil mapper remove to remain nil")
	}
}

func TestImpl_NonceMapper_Submit_Good(t *testing.T) {
	strategy := &nicehashStrategySpy{}
	mapper := NewNonceMapper(1, &proxy.Config{}, strategy)
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	mapper.Add(miner)
	mapper.storage.SetJob(nicehashImplJob("job-1"))
	mapper.Submit(&proxy.SubmitEvent{Miner: miner, JobID: "job-1", RequestID: 11})
	if strategy.submits != 1 || len(mapper.pending) != 1 {
		t.Fatalf("expected pending nonce submit, submits=%d pending=%d", strategy.submits, len(mapper.pending))
	}
}

func TestImpl_NonceMapper_Submit_Bad(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.Submit(nil)
	if len(mapper.pending) != 0 {
		t.Fatalf("expected nil submit ignored, pending=%d", len(mapper.pending))
	}
}

func TestImpl_NonceMapper_Submit_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	mapper.Add(miner)
	mapper.storage.SetJob(nicehashImplJob("current"))
	mapper.storage.SetJob(nicehashImplJob("previous"))
	mapper.Submit(&proxy.SubmitEvent{Miner: miner, JobID: "current", RequestID: 12})
	if len(mapper.pending) != 1 {
		t.Fatalf("expected current job submit recorded, pending=%d", len(mapper.pending))
	}
}

func TestImpl_NonceMapper_IsActive_Good(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnJob(nicehashImplJob("job-1"))
	if !mapper.IsActive() {
		t.Fatal("expected mapper active after valid job")
	}
}

func TestImpl_NonceMapper_IsActive_Bad(t *testing.T) {
	var mapper *NonceMapper
	if mapper.IsActive() {
		t.Fatal("expected nil mapper inactive")
	}
}

func TestImpl_NonceMapper_IsActive_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnDisconnect()
	if mapper.IsActive() {
		t.Fatal("expected disconnected mapper inactive")
	}
}

func TestImpl_NonceMapper_OnJob_Good(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnJob(nicehashImplJob("job-1"))
	if mapper.storage.job.JobID != "job-1" || !mapper.active {
		t.Fatalf("expected job stored and mapper active, job=%+v active=%v", mapper.storage.job, mapper.active)
	}
}

func TestImpl_NonceMapper_OnJob_Bad(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnJob(proxy.Job{})
	if mapper.storage.job.JobID != "" || mapper.active {
		t.Fatalf("expected invalid job ignored, job=%+v active=%v", mapper.storage.job, mapper.active)
	}
}

func TestImpl_NonceMapper_OnJob_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.suspended = 2
	mapper.OnJob(nicehashImplJob("job-1"))
	if mapper.suspended != 0 || mapper.lastUsed.IsZero() {
		t.Fatalf("expected job to clear suspension and update last used, mapper=%+v", mapper)
	}
}

func TestImpl_NonceMapper_OnResultAccepted_Good(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	mapper.Add(miner)
	mapper.storage.SetJob(nicehashImplJob("job-1"))
	mapper.pending[3] = SubmitContext{RequestID: 13, MinerID: miner.ID(), JobID: "job-1", Diff: 64, StartedAt: time.Now()}
	mapper.OnResultAccepted(3, true, "")
	if len(mapper.pending) != 0 || miner.TX() == 0 {
		t.Fatalf("expected accepted result to reply and clear pending, pending=%d tx=%d", len(mapper.pending), miner.TX())
	}
}

func TestImpl_NonceMapper_OnResultAccepted_Bad(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnResultAccepted(99, true, "")
	if len(mapper.pending) != 0 {
		t.Fatalf("expected unknown sequence ignored, pending=%d", len(mapper.pending))
	}
}

func TestImpl_NonceMapper_OnResultAccepted_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	miner := proxy.NewMiner(discardConn{}, 3333, nil)
	miner.SetID(1)
	mapper.Add(miner)
	mapper.storage.SetJob(nicehashImplJob("job-1"))
	mapper.pending[4] = SubmitContext{RequestID: 14, MinerID: miner.ID(), JobID: "job-1", Diff: 32, StartedAt: time.Now()}
	mapper.OnResultAccepted(4, false, "rejected")
	if len(mapper.pending) != 0 || miner.TX() == 0 {
		t.Fatalf("expected rejected result to reply and clear pending, pending=%d tx=%d", len(mapper.pending), miner.TX())
	}
}

func TestImpl_NonceMapper_OnDisconnect_Bad(t *testing.T) {
	var mapper *NonceMapper
	mapper.OnDisconnect()
	if mapper != nil {
		t.Fatal("expected nil mapper to remain nil")
	}
}

func TestImpl_NonceMapper_OnDisconnect_Ugly(t *testing.T) {
	mapper := NewNonceMapper(1, &proxy.Config{}, &nicehashStrategySpy{})
	mapper.OnDisconnect()
	mapper.OnDisconnect()
	if mapper.suspended != 2 || mapper.active {
		t.Fatalf("expected repeated disconnect to count suspensions, mapper=%+v", mapper)
	}
}

func TestImpl_NewNonceStorage_Bad(t *testing.T) {
	storage := NewNonceStorage()
	if storage.IsValidJobID("missing") {
		t.Fatal("expected new storage to reject unknown job")
	}
}

func TestImpl_NewNonceStorage_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	if ok := storage.Add(nil); ok {
		t.Fatal("expected new storage to reject nil miner")
	}
}

func TestImpl_NonceStorage_Add_Good(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	if !storage.Add(miner) || miner.FixedByte() != 0 {
		t.Fatalf("expected first miner in slot 0, fixedByte=%d", miner.FixedByte())
	}
}

func TestImpl_NonceStorage_Add_Bad(t *testing.T) {
	storage := NewNonceStorage()
	if storage.Add(nil) {
		t.Fatal("expected nil miner add to fail")
	}
}

func TestImpl_NonceStorage_Add_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	for i := 1; i <= 256; i++ {
		miner := &proxy.Miner{}
		miner.SetID(int64(i))
		if !storage.Add(miner) {
			t.Fatalf("expected miner %d to fit", i)
		}
	}
	extra := &proxy.Miner{}
	extra.SetID(300)
	if storage.Add(extra) {
		t.Fatal("expected storage full add to fail")
	}
}

func TestImpl_NonceStorage_Remove_Good(t *testing.T) {
	storage := NewNonceStorage()
	miner := &proxy.Miner{}
	miner.SetID(1)
	storage.Add(miner)
	storage.Remove(miner)
	_, dead, active := storage.SlotCount()
	if dead != 1 || active != 0 {
		t.Fatalf("expected removed miner to leave dead slot, dead=%d active=%d", dead, active)
	}
}

func TestImpl_NonceStorage_Remove_Bad(t *testing.T) {
	storage := NewNonceStorage()
	storage.Remove(nil)
	free, dead, active := storage.SlotCount()
	if free != 256 || dead != 0 || active != 0 {
		t.Fatalf("expected nil remove ignored, free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestImpl_NonceStorage_Remove_Ugly(t *testing.T) {
	var storage *NonceStorage
	storage.Remove(&proxy.Miner{})
	if storage != nil {
		t.Fatal("expected nil storage remove to remain nil")
	}
}

func TestImpl_NonceStorage_SetJob_Good(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(nicehashImplJob("job-1"))
	if storage.job.JobID != "job-1" {
		t.Fatalf("expected current job stored, got %+v", storage.job)
	}
}

func TestImpl_NonceStorage_SetJob_Bad(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(proxy.Job{})
	if storage.job.JobID != "" {
		t.Fatalf("expected invalid job ignored, got %+v", storage.job)
	}
}

func TestImpl_NonceStorage_SetJob_Ugly(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(nicehashImplJob("job-1"))
	storage.SetJob(nicehashImplJob("job-2"))
	if storage.prevJob.JobID != "job-1" || storage.job.JobID != "job-2" {
		t.Fatalf("expected previous job retained, prev=%+v current=%+v", storage.prevJob, storage.job)
	}
}

func TestImpl_NonceStorage_IsValidJobID_Good(t *testing.T) {
	storage := NewNonceStorage()
	storage.SetJob(nicehashImplJob("job-1"))
	if !storage.IsValidJobID("job-1") {
		t.Fatal("expected current job id valid")
	}
}

func TestImpl_NonceStorage_SlotCount_Bad(t *testing.T) {
	var storage *NonceStorage
	free, dead, active := storage.SlotCount()
	if free != 0 || dead != 0 || active != 0 {
		t.Fatalf("expected nil storage slot counts zero, free=%d dead=%d active=%d", free, dead, active)
	}
}

func TestImpl_NonceSplitter_Disconnect_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.Disconnect()
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestImpl_NonceSplitter_Disconnect_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{}, nil, func(pool.StratumListener) pool.Strategy { return spy })
	splitter.Connect()
	splitter.Disconnect()
	splitter.Disconnect()
	if len(splitter.mappers) != 0 || spy.disconnects != 1 {
		t.Fatalf("expected disconnect to clear mappers once, mappers=%d disconnects=%d", len(splitter.mappers), spy.disconnects)
	}
}

func TestImpl_NonceSplitter_Tick_Bad(t *testing.T) {
	var splitter *NonceSplitter
	splitter.Tick(1)
	if splitter != nil {
		t.Fatal("expected nil splitter to remain nil")
	}
}

func TestImpl_NonceSplitter_Tick_Ugly(t *testing.T) {
	spy := &nicehashStrategySpy{}
	splitter := NewNonceSplitter(&proxy.Config{}, nil, func(pool.StratumListener) pool.Strategy { return spy })
	splitter.Connect()
	splitter.Tick(^uint64(0))
	if spy.ticks != 1 {
		t.Fatalf("expected tick forwarded once, got %d", spy.ticks)
	}
}
