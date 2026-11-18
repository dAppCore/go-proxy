package integration_test

import core "github.com/dappcore/go"

func repeatString(value string, count int) string {
	if count <= 0 || value == "" {
		return ""
	}
	builder := core.NewBuilder()
	for range count {
		builder.WriteString(value)
	}
	return builder.String()
}
