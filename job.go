package proxy

// Job holds one pool work unit and its metadata.
//
// j := proxy.Job{Blob: "0707d5ef...b01", JobID: "4BiGm3/RgGQzgkTI", Target: "b88d0600", Algo: "cn/r"}
type Job struct {
	Blob     string // hex-encoded block template (160 hex chars = 80 bytes)
	JobID    string // pool-assigned identifier
	Target   string // 8-char hex little-endian uint32 difficulty target
	Algo     string // algorithm e.g. "cn/r", "rx/0"; "" if not negotiated
	Height   uint64 // block height (0 if pool did not provide)
	SeedHash string // RandomX seed hash hex (empty if not RandomX)
	ClientID string // pool session ID that issued this job (for stale detection)
}
