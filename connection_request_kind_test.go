package transport

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestInboundRequestContextExcludesNotificationsAndNullIDs(t *testing.T) {
	input, peer := io.Pipe()
	defer input.Close()
	defer peer.Close()
	classified := make(chan struct {
		method  string
		request bool
	}, 3)
	NewConnection(func(ctx context.Context, method string, _ json.RawMessage) (any, *RequestError) {
		classified <- struct {
			method  string
			request bool
		}{method, IsInboundRequest(ctx)}
		return struct{}{}, nil
	}, io.Discard, input)
	for _, frame := range []string{
		`{"jsonrpc":"2.0","method":"notification"}`,
		`{"jsonrpc":"2.0","id":null,"method":"null"}`,
		`{"jsonrpc":"2.0","id":1,"method":"request"}`,
	} {
		if _, err := io.WriteString(peer, frame+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string]bool{"notification": false, "null": false, "request": true}
	for range want {
		select {
		case got := <-classified:
			if expected, ok := want[got.method]; !ok || got.request != expected {
				t.Fatalf("wrong message classification: %+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("message handler did not run")
		}
	}
}
