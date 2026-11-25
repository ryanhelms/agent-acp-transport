# Upstream sources

The connection, line framing, notification and request barriers, cancellation, numeric ID canonicalization and the three connection test files derive from github.com/coder/acp-go-sdk v0.20.0, licensed under Apache-2.0. The original license is included as LICENSE. Exact module checksums are recorded in go.sum.

This distribution changes the Go package name to transport, adds request-specific receipts and selected-session inbound receipts, and removes peer-controlled content from connection diagnostics. It reuses the upstream public JSON-RPC error type.
