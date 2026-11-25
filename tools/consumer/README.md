# Standalone transport consumer tests

This module exercises the released transport over paired in-memory streams through its public Go API. It uses Go 1.21 or later. From this directory, point Go at the local release proxy and run the race checks. Set the path to the release directory that contains the manifest and the `go/` directory.

```sh
export ACP_TRANSPORT_RELEASE=/tmp/agent-acp-transport-0.1.0
export GOPROXY="file://$ACP_TRANSPORT_RELEASE/go,$(go env GOPROXY)"
export GONOPROXY=none
export GONOSUMDB=agent-acp.local/transport
go mod download
go test -mod=readonly -race -count=5 -timeout=30s ./...
```

When working offline with the upstream ACP dependency cached, use `GOPROXY="file://$ACP_TRANSPORT_RELEASE/go,off"`. The pinned version and checksums allow this suite to run in a separate checkout.

The tests exercise genuine peer errors, interruption before and after a matched reply, concurrent permissions with identical params, exact IDs, selected-session notification capture, scanner-buffer lifetime, and incomplete outbound frames. The allocation benchmark adds a 32-byte or 128 KiB unknown field to otherwise identical responses and reports allocation cost for both request methods.

```sh
go test -mod=readonly -run '^$' -bench BenchmarkExternalReceiptCopy -benchmem -benchtime=300ms -count=3
```
