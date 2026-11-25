# Agent ACP Transport

A standalone Go implementation of the ACP stdio connection. It derives its JSON-RPC framing, cancellation and notification ordering from `github.com/coder/acp-go-sdk` v0.20.0. You can use this module in a Go client or adapter independently of Agent Commander and the Agent ACP session SDK. The module uses the Apache License 2.0. [License](LICENSE)

Requires Go 1.21 or later. [Upstream provenance](UPSTREAM.md)

## Installing from a local release

The module path is `agent-acp.local/transport`. Its release version is independent of the session SDK. The local release directory contains a Go module proxy and a manifest with SHA-256 digests. Prepend its absolute `go/` directory as a file URL to the consumer's Go proxy list. For example, if the release is in `/tmp/agent-acp-transport-0.1.0`, run the following in your Go module.

```sh
GONOPROXY=none GONOSUMDB=agent-acp.local/transport \
  GOPROXY="file:///tmp/agent-acp-transport-0.1.0/go,$(go env GOPROXY)" \
  go get agent-acp.local/transport@v0.1.0
```

Compare the downloaded module and go.mod hashes with your independently obtained release pins and keep them in your application's `go.sum`. The upstream ACP SDK supplies shared error and handler types.

## Selected conversation traffic

Use `NewConnection` and `SendRequest` for ordinary ACP requests. Call `SendRequestWithReceipt` when an application needs the matched peer response to a particular request. Its additional receipt identifies the frame that the peer actually sent. The receipt preserves the wire spelling of the JSON-RPC ID and the original JSON envelope. Inspect it for both successful and failed requests. A cancellation, disconnect or write failure before any response is matched returns a nil receipt.

For incoming requests and notifications, construct the connection with `NewConnectionWithOptions`. Select the desired methods and the already-bound session. Retrieve the receipt from inside the handler using its context, method and that session ID. Enable `OmitPlainNotifications` for consumers that only need extended notifications and scoped requests. A JSON-RPC ID is transport provenance. Use your application's own correlation and authorization policy before answering a request.

A client can inspect a single prompt response with the following helper. Its caller supplies the selected session’s prompt parameters and a function that consumes the private receipt.

```go
import (
    "context"
    "encoding/json"

    "agent-acp.local/transport"
)

func prompt(ctx context.Context, conn *transport.Connection, params json.RawMessage,
    inspect func(*transport.Receipt)) (json.RawMessage, error) {
    result, receipt, err := transport.SendRequestWithReceipt[json.RawMessage](conn, ctx, "session/prompt", params)
    if receipt != nil {
        inspect(receipt)
    }
    return result, err
}
```

The receipt follows the same ordering as the typed response, including earlier notifications from the peer. If cancellation or EOF interrupts delivery after the response was matched, or if the result cannot decode into the requested type, the caller still receives the original receipt. Use `errors.As` with `*transport.DeliveryError` to identify a local delivery failure. The original frame's `error` or `result` member identifies the peer's response. Each captured frame is subject to the connection's 10 MiB framing limit.

## Building the release

```sh
go test -race ./...
python3 tools/release.py 0.1.0 /tmp/agent-acp-transport-0.1.0 --epoch 1795429236
```

Two builds with the same source and epoch produce the same proxy files and manifest. Choose a new output directory for each release. The release tool writes the module, version metadata and ZIP into staging and publishes the completed directory with one rename.
