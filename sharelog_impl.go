package proxy

import (
	"io"
	"time"

	core "dappco.re/go"
)

type shareLogSink struct {
	path string
	file io.WriteCloser
	mu   core.Mutex
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
		if err := l.file.Close(); err != nil {
			// best-effort close before reopening the configured path
		}
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
		if err := l.file.Close(); err != nil {
			// best-effort close; the sink is reset either way
		}
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
	if trimString(l.path) == "" {
		return
	}
	if l.file == nil {
		file := openAppendFile(l.path)
		if file == nil {
			return
		}
		l.file = file
	}
	builder := newBuilder()
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteByte(' ')
	builder.WriteString(kind)
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
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
	if _, err := l.file.Write([]byte(builder.String())); err != nil {
		return
	}
}
