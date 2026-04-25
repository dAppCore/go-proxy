package proxy

import (
	"io"
	"strconv"
	"time"
	"unicode"

	core "dappco.re/go/core"
)

type accessLogSink struct {
	path string
	file io.WriteCloser
	mu   core.Mutex
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
	builder.WriteString("CONNECT")
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(ip))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(agent))
	builder.WriteByte('\n')
	_, _ = l.file.Write([]byte(builder.String()))
}

func (l *accessLogSink) writeCloseLine(ip, user string, rx, tx uint64) {
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
	builder.WriteString("CLOSE")
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(ip))
	builder.WriteString("  ")
	builder.WriteString(sanitizeLogColumnField(user))
	builder.WriteString("  rx=")
	builder.WriteString(formatUint(rx))
	builder.WriteString("  tx=")
	builder.WriteString(formatUint(tx))
	builder.WriteByte('\n')
	_, _ = l.file.Write([]byte(builder.String()))
}

func formatUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func sanitizeLogField(value string) string {
	if value == "" {
		return ""
	}
	builder := newBuilder()
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
	builder := newBuilder()
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
