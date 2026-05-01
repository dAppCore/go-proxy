package pool

import core "dappco.re/go"

func jsonMarshalString(value any) string {
	return core.JSONMarshalString(value)
}

func jsonUnmarshalBytes(data []byte, target any) bool {
	return core.JSONUnmarshalString(string(data), target).OK
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

func repeatString(value string, count int) string {
	if count <= 0 || value == "" {
		return ""
	}
	builder := core.NewBuilder()
	for i := 0; i < count; i++ {
		builder.WriteString(value)
	}
	return builder.String()
}

func equalFoldString(left, right string) bool {
	return lowerString(left) == lowerString(right)
}

func valueString(value any) string {
	text, _ := value.(string)
	return text
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
