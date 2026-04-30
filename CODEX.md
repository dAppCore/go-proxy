# CODEX.md

This repository uses `CLAUDE.md` as the detailed source of truth for working conventions.
This file exists so agent workflows that expect `CODEX.md` can resolve the repo rules directly.

## Core Conventions

- Read `docs/RFC.md` before changing behaviour.
- Preserve existing user changes in the worktree.
- Prefer `rg` for search and `apply_patch` for edits.
- Keep names predictable and comments example-driven.
- Run `go test ./...` and `go test -race ./...` before committing when practical.
- Commit with a conventional message and include the required co-author line when requested by repo policy.

