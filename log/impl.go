package log

import (
	"os"
	"strconv"
	"strings"
	"time"

	"dappco.re/go/proxy"
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
	l.writeConnectLine(e.Miner.IP(), e.Miner.User(), e.Miner.Agent())
}

// OnClose writes a CLOSE line with byte counts.
func (l *AccessLog) OnClose(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeCloseLine(e.Miner.IP(), e.Miner.User(), e.Miner.RX(), e.Miner.TX())
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

func (accessLog *AccessLog) writeConnectLine(ip, user, agent string) {
	accessLog.mu.Lock()
	defer accessLog.mu.Unlock()
	if err := accessLog.ensureFile(); err != nil {
		return
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
	_, _ = accessLog.f.WriteString(builder.String())
}

func (accessLog *AccessLog) writeCloseLine(ip, user string, rx, tx uint64) {
	accessLog.mu.Lock()
	defer accessLog.mu.Unlock()
	if err := accessLog.ensureFile(); err != nil {
		return
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
	builder.WriteString(strconv.FormatUint(rx, 10))
	builder.WriteString("  tx=")
	builder.WriteString(strconv.FormatUint(tx, 10))
	builder.WriteByte('\n')
	_, _ = accessLog.f.WriteString(builder.String())
}

func (shareLog *ShareLog) writeAcceptLine(user string, diff uint64, latency uint64) {
	shareLog.mu.Lock()
	defer shareLog.mu.Unlock()
	if err := shareLog.ensureFile(); err != nil {
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
	_, _ = shareLog.f.WriteString(builder.String())
}

func (shareLog *ShareLog) writeRejectLine(user, reason string) {
	shareLog.mu.Lock()
	defer shareLog.mu.Unlock()
	if err := shareLog.ensureFile(); err != nil {
		return
	}
	var builder strings.Builder
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteString(" REJECT  ")
	builder.WriteString(user)
	builder.WriteString("  reason=\"")
	builder.WriteString(reason)
	builder.WriteString("\"\n")
	_, _ = shareLog.f.WriteString(builder.String())
}

func (accessLog *AccessLog) ensureFile() error {
	if accessLog.f != nil {
		return nil
	}
	f, err := os.OpenFile(accessLog.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	accessLog.f = f
	return nil
}

func (shareLog *ShareLog) ensureFile() error {
	if shareLog.f != nil {
		return nil
	}
	f, err := os.OpenFile(shareLog.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	shareLog.f = f
	return nil
}
