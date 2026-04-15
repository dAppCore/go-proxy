package simple

import (
	"testing"

	"dappco.re/go/proxy"
)

func TestSimpleMapperFile_NewSimpleMapper_Good(t *testing.T) {
	mapper := NewSimpleMapper(7, activeStrategy{})
	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.id != 7 {
		t.Fatalf("expected mapper id 7, got %d", mapper.id)
	}
	if mapper.pending == nil {
		t.Fatal("expected pending map to be initialised")
	}
}

func TestSimpleMapperFile_NewSimpleMapper_Bad(t *testing.T) {
	mapper := NewSimpleMapper(0, nil)
	if mapper == nil {
		t.Fatal("expected mapper")
	}
	if mapper.strategy != nil {
		t.Fatal("expected nil strategy to be preserved")
	}
}

func TestSimpleMapperFile_NewSimpleMapper_Ugly(t *testing.T) {
	mapper := NewSimpleMapper(1, activeStrategy{})
	if mapper.idleAt.IsZero() == false {
		t.Fatal("expected new mapper to start active")
	}
}

func TestSimpleMapperFile_OnDisconnect_Good(t *testing.T) {
	mapper := NewSimpleMapper(7, activeStrategy{})
	mapper.OnDisconnect()
	if !mapper.stopped {
		t.Fatal("expected mapper to be marked stopped")
	}
}

func TestSimpleMapperFile_OnDisconnect_Bad(t *testing.T) {
	var mapper *SimpleMapper
	mapper.OnDisconnect()
}

func TestSimpleMapperFile_OnDisconnect_Ugly(t *testing.T) {
	mapper := NewSimpleMapper(7, activeStrategy{})
	mapper.OnDisconnect()
	mapper.OnDisconnect()
	if !mapper.stopped {
		t.Fatal("expected repeated disconnects to remain stopped")
	}
}

func TestSimpleMapperFile_OnJob_ResetsPreviousJob_Good(t *testing.T) {
	mapper := NewSimpleMapper(7, activeStrategy{})
	mapper.currentJob = proxy.Job{JobID: "job-1", ClientID: "session-1"}
	mapper.OnJob(proxy.Job{JobID: "job-2", ClientID: "session-1"})
	if mapper.prevJob.JobID != "job-1" {
		t.Fatalf("expected previous job to be retained, got %q", mapper.prevJob.JobID)
	}
}
