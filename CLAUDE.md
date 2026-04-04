# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CryptoNote stratum mining proxy library with NiceHash nonce-splitting (256-slot), simple passthrough mode, pool failover, TLS, HTTP monitoring API, and per-IP rate limiting. Module: `dappco.re/go/core/proxy`

## Spec

The complete RFC lives at `docs/RFC.md` (1475 lines). Read it fully before implementing any feature. AX design principles live at `.core/reference/RFC-025-AGENT-EXPERIENCE.md`.

## Commands

```bash
go test ./...                        # Run all tests
go test -v -run TestJob_BlobWithFixedByte_Good ./... # Run single test
go test -race ./...                  # Race detector (must pass before commit)
go test -cover ./...                 # Coverage
go test -bench=. -benchmem ./...     # Benchmarks
go vet ./...                         # Vet
```

## Architecture

**Event-driven proxy.** The `Proxy` orchestrator owns the tick loop (1s), TCP servers, splitter, stats, workers, and optional HTTP API. Events flow through a synchronous `EventBus`.

**Two splitter modes:**
- `nicehash` — NonceSplitter partitions 32-bit nonce space via fixed byte 39. Up to 256 miners per upstream pool connection.
- `simple` — SimpleSplitter creates one upstream per miner, with optional reuse pool on disconnect.

**Per-miner goroutines.** Each accepted TCP connection runs a read loop in its own goroutine. Writes serialised by `Miner.sendMu`.

**File layout:**
```
proxy.go                      # Proxy orchestrator — tick loop, listeners, splitter, stats
config.go                     # Config struct, JSON unmarshal, hot-reload watcher
server.go                     # TCP server — accepts connections, applies rate limiter
miner.go                      # Miner state machine — one per connection
job.go                        # Job value type — blob, job_id, target, algo, height
worker.go                     # Worker aggregate — rolling hashrate, share counts
stats.go                      # Stats aggregate — global counters, hashrate windows
events.go                     # Event bus — LoginEvent, AcceptEvent, SubmitEvent, CloseEvent
splitter/nicehash/splitter.go # NonceSplitter — owns mapper pool, routes miners
splitter/nicehash/mapper.go   # NonceMapper — one upstream connection, owns NonceStorage
splitter/nicehash/storage.go  # NonceStorage — 256-slot table, fixed-byte allocation
splitter/simple/splitter.go   # SimpleSplitter — passthrough, upstream reuse pool
splitter/simple/mapper.go     # SimpleMapper — one upstream per miner group
pool/client.go                # StratumClient — outbound pool TCP/TLS connection
pool/strategy.go              # FailoverStrategy — primary + ordered fallbacks
log/access.go                 # AccessLog — connection open/close lines
log/share.go                  # ShareLog — accept/reject lines per share
api/router.go                 # HTTP handlers — /1/summary, /1/workers, /1/miners
```

## Banned Imports

| Banned Import | Use Instead |
|---------------|-------------|
| `fmt` | `core.Sprintf`, `core.Print` |
| `log` | `core.Print`, `core.Error` |
| `errors` | `core.E(scope, message, cause)` |
| `os` | `c.Fs()` |
| `os/exec` | `c.Process()` |
| `strings` | `core.Contains`, `core.TrimPrefix`, etc. |
| `path/filepath` | `core.JoinPath`, `core.PathBase` |
| `encoding/json` | `core.JSONMarshalString`, `core.JSONUnmarshalString` |

## Coding Standards

- **UK English** in all code, comments, docs (colour, behaviour, serialise, organisation)
- **Strict error handling**: all errors use `core.E(scope, message, cause)` — never `fmt.Errorf` or `errors.New`
- **Comments as usage examples** with concrete values, not prose descriptions
- **Type hints**: all parameters and return types declared
- **Test naming**: `TestFilename_Function_{Good,Bad,Ugly}` — all three mandatory per function
- **Race detector**: `go test -race ./...` must pass before commit
- **Conventional commits**: `type(scope): description`
- **Co-Author**: `Co-Authored-By: Virgil <virgil@lethean.io>`
- **Licence**: EUPL-1.2

## Test Conventions

- Test names follow `TestFilename_Function_{Good,Bad,Ugly}`
- Good = happy path, Bad = expected failures, Ugly = edge cases and recovery
- Use `require` for preconditions, `assert` for verifications (`testify`)
- All three categories mandatory per function under test
