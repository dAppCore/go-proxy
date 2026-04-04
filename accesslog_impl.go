package proxy

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type accessLogSink struct {
	path string
	file *os.File
	mu   sync.Mutex
}

func newAccessLogSink(path string) *accessLogSink {
	return &accessLogSink{path: path}
}

func (l *accessLogSink) SetPath(path string) {
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

func (l *accessLogSink) Close() {
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

func (l *accessLogSink) OnLogin(e Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeConnectLine(e.Miner.IP(), e.Miner.User(), e.Miner.Agent())
}

func (l *accessLogSink) OnClose(e Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeCloseLine(e.Miner.IP(), e.Miner.User(), e.Miner.RX(), e.Miner.TX())
}

func (l *accessLogSink) writeConnectLine(ip, user, agent string) {
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
	builder.WriteString("CONNECT")
	builder.WriteString("  ")
	builder.WriteString(ip)
	builder.WriteString("  ")
	builder.WriteString(user)
	builder.WriteString("  ")
	builder.WriteString(agent)
	builder.WriteByte('\n')
	_, _ = l.file.WriteString(builder.String())
}

func (l *accessLogSink) writeCloseLine(ip, user string, rx, tx uint64) {
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
	builder.WriteString("CLOSE")
	builder.WriteString("  ")
	builder.WriteString(ip)
	builder.WriteString("  ")
	builder.WriteString(user)
	builder.WriteString("  rx=")
	builder.WriteString(formatUint(rx))
	builder.WriteString("  tx=")
	builder.WriteString(formatUint(tx))
	builder.WriteByte('\n')
	_, _ = l.file.WriteString(builder.String())
}

func formatUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}
