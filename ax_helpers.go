package proxy

import (
	"io"
	"unicode"

	core "dappco.re/go/core"
)

var proxyCoreRuntime = core.New()

func proxyFileSystem() *core.Fs {
	return proxyCoreRuntime.Fs()
}

func jsonMarshalString(value any) string {
	return core.JSONMarshalString(value)
}

func jsonUnmarshalBytes(data []byte, target any) bool {
	return core.JSONUnmarshalString(string(data), target).OK
}

func jsonUnmarshalString(data string, target any) bool {
	return core.JSONUnmarshalString(data, target).OK
}

func trimString(value string) string {
	return core.Trim(value)
}

func lowerString(value string) string {
	return core.Lower(value)
}

func containsString(value, needle string) bool {
	return core.Contains(value, needle)
}

func splitStringN(value, separator string, count int) []string {
	return core.SplitN(value, separator, count)
}

func equalFoldString(left, right string) bool {
	return lowerString(left) == lowerString(right)
}

func lastIndexByte(value string, needle byte) int {
	for index := len(value) - 1; index >= 0; index-- {
		if value[index] == needle {
			return index
		}
	}
	return -1
}

func splitFieldsBySeparators(value string) []string {
	fields := make([]string, 0, 4)
	start := -1
	for index, r := range value {
		if r == ',' || r == ':' || r == ';' || r == '|' || unicode.IsSpace(r) {
			if start >= 0 {
				fields = append(fields, value[start:index])
				start = -1
			}
			continue
		}
		if start < 0 {
			start = index
		}
	}
	if start >= 0 {
		fields = append(fields, value[start:])
	}
	return fields
}

func valueString(value any) string {
	text, _ := value.(string)
	return text
}

func valueStringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if ok {
				values = append(values, text)
			}
		}
		return values
	default:
		return nil
	}
}

func valueUint64(value any) uint64 {
	switch typed := value.(type) {
	case uint64:
		return typed
	case uint32:
		return uint64(typed)
	case int:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	case int64:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	case float64:
		if typed < 0 {
			return 0
		}
		return uint64(typed)
	default:
		return 0
	}
}

func valueMap(value any) map[string]any {
	mapValue, _ := value.(map[string]any)
	return mapValue
}

func openAppendFile(path string) io.WriteCloser {
	result := proxyFileSystem().Append(path)
	if !result.OK || result.Value == nil {
		return nil
	}
	writer, _ := result.Value.(io.WriteCloser)
	return writer
}

type textBuilder interface {
	WriteString(string) (int, error)
	WriteByte(byte) error
	WriteRune(rune) (int, error)
	Grow(int)
	String() string
}

func newBuilder() textBuilder {
	return core.NewBuilder()
}
