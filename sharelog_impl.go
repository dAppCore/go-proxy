package proxy

import (
	"os"
	"strings"
	"sync"
	"time"
)

type shareLogSink struct {
	path string
	file *os.File
	mu   sync.Mutex
}

func newShareLogSink(path string) *shareLogSink {
	return &shareLogSink{path: path}
}

func (l *shareLogSink) SetPath(path string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.path == path {
		return
	}
	l.path = path
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

func (l *shareLogSink) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

func (l *shareLogSink) OnAccept(e Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeLine("ACCEPT", e.Miner.User(), e.Diff, e.Latency, "")
}

func (l *shareLogSink) OnReject(e Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeLine("REJECT", e.Miner.User(), 0, 0, e.Error)
}

func (l *shareLogSink) writeLine(kind, user string, diff uint64, latency uint16, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if strings.TrimSpace(l.path) == "" {
		return
	}
	if l.file == nil {
		file, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		l.file = file
	}
	var builder strings.Builder
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteByte(' ')
	builder.WriteString(kind)
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogField(user))
	switch kind {
	case "ACCEPT":
		builder.WriteString("  diff=")
		builder.WriteString(formatUint(diff))
		builder.WriteString("  latency=")
		builder.WriteString(formatUint(uint64(latency)))
		builder.WriteString("ms")
	case "REJECT":
		builder.WriteString("  reason=\"")
		builder.WriteString(sanitizeLogField(reason))
		builder.WriteString("\"")
	}
	builder.WriteByte('\n')
	_, _ = l.file.WriteString(builder.String())
}
