package proxylog

import (
	"strconv"
	"time"
	"unicode"

	core "dappco.re/go"
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
		if err := l.file.Close(); err != nil {
			// best-effort close; the log is reset either way
		}
		l.file = nil
	}
}

// OnLogin writes a connect line such as:
//
//	al.OnLogin(proxy.Event{Miner: &proxy.Miner{}})
//	// 2026-04-04T12:00:00Z CONNECT  10.0.0.1  WALLET  XMRig/6.21.0
func (l *AccessLog) OnLogin(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeConnectLine(e.Miner.IP(), e.Miner.User(), e.Miner.Agent())
}

// OnClose writes a close line such as:
//
//	al.OnClose(proxy.Event{Miner: &proxy.Miner{}})
//	// 2026-04-04T12:00:00Z CLOSE  10.0.0.1  WALLET  rx=512  tx=4096
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
		if err := l.file.Close(); err != nil {
			// best-effort close; the log is reset either way
		}
		l.file = nil
	}
}

// OnAccept writes an accept line such as:
//
//	sl.OnAccept(proxy.Event{Miner: &proxy.Miner{}, Diff: 100000, Latency: 82})
//	// 2026-04-04T12:00:00Z ACCEPT  WALLET  diff=100000  latency=82ms
func (l *ShareLog) OnAccept(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeAcceptLine(e.Miner.User(), e.Diff, uint64(e.Latency))
}

// OnReject writes a reject line such as:
//
//	sl.OnReject(proxy.Event{Miner: &proxy.Miner{}, Error: "Invalid nonce"})
//	// 2026-04-04T12:00:00Z REJECT  WALLET  reason="Invalid nonce"
func (l *ShareLog) OnReject(e proxy.Event) {
	if l == nil || e.Miner == nil {
		return
	}
	l.writeRejectLine(e.Miner.User(), e.Error)
}

func (accessLog *AccessLog) writeConnectLine(ip, user, agent string) {
	accessLog.mu.Lock()
	defer accessLog.mu.Unlock()
	if r := accessLog.ensureFile(); !r.OK {
		return
	}
	builder := core.NewBuilder()
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteByte(' ')
	builder.WriteString("CONNECT")
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(ip))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(agent))
	builder.WriteByte('\n')
	if _, err := accessLog.file.Write([]byte(builder.String())); err != nil {
		return
	}
}

func (accessLog *AccessLog) writeCloseLine(ip, user string, rx, tx uint64) {
	accessLog.mu.Lock()
	defer accessLog.mu.Unlock()
	if r := accessLog.ensureFile(); !r.OK {
		return
	}
	builder := core.NewBuilder()
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteByte(' ')
	builder.WriteString("CLOSE")
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(ip))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  rx=")
	builder.WriteString(strconv.FormatUint(rx, 10))
	builder.WriteString("  tx=")
	builder.WriteString(strconv.FormatUint(tx, 10))
	builder.WriteByte('\n')
	if _, err := accessLog.file.Write([]byte(builder.String())); err != nil {
		return
	}
}

func (shareLog *ShareLog) writeAcceptLine(user string, diff uint64, latency uint64) {
	shareLog.mu.Lock()
	defer shareLog.mu.Unlock()
	if r := shareLog.ensureFile(); !r.OK {
		return
	}
	builder := core.NewBuilder()
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteString(" ACCEPT")
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  diff=")
	builder.WriteString(strconv.FormatUint(diff, 10))
	builder.WriteString("  latency=")
	builder.WriteString(strconv.FormatUint(latency, 10))
	builder.WriteString("ms")
	builder.WriteByte('\n')
	if _, err := shareLog.file.Write([]byte(builder.String())); err != nil {
		return
	}
}

func (shareLog *ShareLog) writeRejectLine(user, reason string) {
	shareLog.mu.Lock()
	defer shareLog.mu.Unlock()
	if r := shareLog.ensureFile(); !r.OK {
		return
	}
	builder := core.NewBuilder()
	builder.WriteString(time.Now().UTC().Format(time.RFC3339))
	builder.WriteString(" REJECT  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  reason=\"")
	builder.WriteString(sanitizeLogField(reason))
	builder.WriteString("\"\n")
	if _, err := shareLog.file.Write([]byte(builder.String())); err != nil {
		return
	}
}

func (accessLog *AccessLog) ensureFile() core.Result {
	if accessLog.file != nil {
		return core.Ok(nil)
	}
	file := core.New().Fs().Append(accessLog.path)
	if !file.OK || file.Value == nil {
		if err, ok := file.Value.(error); ok {
			return core.Fail(err)
		}
		return core.Ok(nil)
	}
	accessLog.file = file.Value.(interface {
		Write([]byte) (int, error)
		Close() error
	})
	return core.Ok(nil)
}

func (shareLog *ShareLog) ensureFile() core.Result {
	if shareLog.file != nil {
		return core.Ok(nil)
	}
	file := core.New().Fs().Append(shareLog.path)
	if !file.OK || file.Value == nil {
		if err, ok := file.Value.(error); ok {
			return core.Fail(err)
		}
		return core.Ok(nil)
	}
	shareLog.file = file.Value.(interface {
		Write([]byte) (int, error)
		Close() error
	})
	return core.Ok(nil)
}

func sanitizeLogField(value string) string {
	if value == "" {
		return ""
	}
	builder := core.NewBuilder()
	builder.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '\\' || r == '"':
			builder.WriteByte('\\')
			builder.WriteRune(r)
		case r == '\n' || r == '\r' || r == '\t' || r == '\u2028' || r == '\u2029' || unicode.IsControl(r):
			builder.WriteByte(' ')
		default:
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func sanitizeLogColumnField(value string) string {
	if value == "" {
		return ""
	}
	builder := core.NewBuilder()
	builder.Grow(len(value))
	for _, r := range value {
		switch {
		case r == '\\' || r == '"':
			builder.WriteByte('\\')
			builder.WriteRune(r)
		case r == '\n' || r == '\r' || r == '\t' || unicode.IsSpace(r) || unicode.IsControl(r):
			builder.WriteByte('_')
		default:
			builder.WriteRune(r)
		}
	}
	return builder.String()
}
