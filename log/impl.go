package log

import (
	"os"
	"strconv"
	"strings"
	"time"

	"dappco.re/go/proxy"
)

// NewAccessLog creates an append-only access log.
//
//	al := log.NewAccessLog("/var/log/proxy-access.log")
//	defer al.Close()
func NewAccessLog(path string) *AccessLog {
	return &AccessLog{path: path}
}

// Close releases the underlying file handle if the log has been opened.
//
//	al := log.NewAccessLog("/var/log/proxy-access.log")
//	defer al.Close()
func (l *AccessLog) Close() {
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

// OnLogin writes `2026-04-04T12:00:00Z CONNECT  10.0.0.1  WALLET  XMRig/6.21.0`.
func (l *AccessLog) OnLogin(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeConnectLine(e.Miner.IP(), e.Miner.User(), e.Miner.Agent())
}

// OnClose writes `2026-04-04T12:00:00Z CLOSE  10.0.0.1  WALLET  rx=512  tx=4096`.
func (l *AccessLog) OnClose(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeCloseLine(e.Miner.IP(), e.Miner.User(), e.Miner.RX(), e.Miner.TX())
}

// NewShareLog creates an append-only share log.
//
//	sl := log.NewShareLog("/var/log/proxy-shares.log")
//	defer sl.Close()
func NewShareLog(path string) *ShareLog {
	return &ShareLog{path: path}
}

// Close releases the underlying file handle if the log has been opened.
//
//	sl := log.NewShareLog("/var/log/proxy-shares.log")
//	defer sl.Close()
func (l *ShareLog) Close() {
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

// OnAccept writes `2026-04-04T12:00:00Z ACCEPT  WALLET  diff=100000  latency=82ms`.
func (l *ShareLog) OnAccept(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeAcceptLine(e.Miner.User(), e.Diff, uint64(e.Latency))
}

// OnReject writes `2026-04-04T12:00:00Z REJECT  WALLET  reason="Invalid nonce"`.
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
	_, _ = accessLog.file.WriteString(builder.String())
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
	_, _ = accessLog.file.WriteString(builder.String())
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
	_, _ = shareLog.file.WriteString(builder.String())
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
	_, _ = shareLog.file.WriteString(builder.String())
}

func (accessLog *AccessLog) ensureFile() error {
	if accessLog.file != nil {
		return nil
	}
	f, err := os.OpenFile(accessLog.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	accessLog.file = f
	return nil
}

func (shareLog *ShareLog) ensureFile() error {
	if shareLog.file != nil {
		return nil
	}
	f, err := os.OpenFile(shareLog.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	shareLog.file = f
	return nil
}
