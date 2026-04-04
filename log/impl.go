package log

import (
	"os"
	"strconv"
	"strings"
	"time"

	"dappco.re/go/core/proxy"
)

// NewAccessLog creates an append-only access log.
func NewAccessLog(path string) *AccessLog {
	return &AccessLog{path: path}
}

// OnLogin writes a CONNECT line.
func (l *AccessLog) OnLogin(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeLine("CONNECT", e.Miner.IP(), e.Miner.User(), e.Miner.Agent(), 0, 0)
}

// OnClose writes a CLOSE line with byte counts.
func (l *AccessLog) OnClose(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeLine("CLOSE", e.Miner.IP(), e.Miner.User(), "", e.Miner.RX(), e.Miner.TX())
}

// NewShareLog creates an append-only share log.
func NewShareLog(path string) *ShareLog {
	return &ShareLog{path: path}
}

// OnAccept writes an ACCEPT line.
func (l *ShareLog) OnAccept(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeAcceptLine(e.Miner.User(), e.Diff, uint64(e.Latency))
}

// OnReject writes a REJECT line.
func (l *ShareLog) OnReject(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeRejectLine(e.Miner.User(), e.Error)
}

func (l *AccessLog) writeLine(kind, ip, user, agent string, rx, tx uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureFile(); err != nil {
		return
	}
	var builder strings.Builder
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteByte(' ')
	builder.WriteString(kind)
	builder.WriteString("  ")
	builder.WriteString(ip)
	builder.WriteString("  ")
	builder.WriteString(user)
	if agent != "" {
		builder.WriteString("  ")
		builder.WriteString(agent)
	}
	if rx > 0 || tx > 0 {
		builder.WriteString("  rx=")
		builder.WriteString(strconv.FormatUint(rx, 10))
		builder.WriteString("  tx=")
		builder.WriteString(strconv.FormatUint(tx, 10))
	}
	builder.WriteByte('\n')
	_, _ = l.f.WriteString(builder.String())
}

func (l *ShareLog) writeAcceptLine(user string, diff uint64, latency uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureFile(); err != nil {
		return
	}
	var builder strings.Builder
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteString(" ACCEPT")
	builder.WriteString("  ")
	builder.WriteString(user)
	builder.WriteString("  diff=")
	builder.WriteString(strconv.FormatUint(diff, 10))
	builder.WriteString("  latency=")
	builder.WriteString(strconv.FormatUint(latency, 10))
	builder.WriteString("ms")
	builder.WriteByte('\n')
	_, _ = l.f.WriteString(builder.String())
}

func (l *ShareLog) writeRejectLine(user, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureFile(); err != nil {
		return
	}
	var builder strings.Builder
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteString(" REJECT  ")
	builder.WriteString(user)
	builder.WriteString("  reason=\"")
	builder.WriteString(reason)
	builder.WriteString("\"\n")
	_, _ = l.f.WriteString(builder.String())
}

func (l *AccessLog) ensureFile() error {
	if l.f != nil {
		return nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = f
	return nil
}

func (l *ShareLog) ensureFile() error {
	if l.f != nil {
		return nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = f
	return nil
}
