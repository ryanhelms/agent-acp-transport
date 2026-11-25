package consumer_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	tr "agent-acp.local/transport"
)

func TestWireErrorVersusLocal(t *testing.T) {
	for _, code := range []int{-32603, -32800, -32077} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			conn := tr.NewConnection(nil, client, client)
			exact := fmt.Sprintf(" { \"jsonrpc\":\"2.0\", \"id\" : 1e+0, \"error\":{ \"code\":%d,\"message\":\"test\",\"data\":null,\"extra\":9007199254740993 }, \"vendor\":[null,false] } ", code)
			go func() {
				_, e := bufio.NewReader(peer).ReadBytes('\n')
				if e == nil {
					fmt.Fprintln(peer, exact)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, r, e := tr.SendRequestWithReceipt[json.RawMessage](conn, ctx, "session/prompt", map[string]string{"sessionId": "chosen"})
			var ae *tr.RequestError
			if !errors.As(e, &ae) || ae.Code != code || r == nil || string(r.ID) != "1e+0" || string(r.Envelope) != exact {
				t.Fatalf("wire error: %v %#v", e, r)
			}
		})
	}
	client, peer := net.Pipe()
	defer client.Close()
	conn := tr.NewConnection(nil, client, client)
	go func() { bufio.NewReader(peer).ReadBytes('\n'); peer.Close() }()
	_, r, e := tr.SendRequestWithReceipt[int](conn, context.Background(), "session/prompt", nil)
	if e == nil || r != nil {
		t.Fatalf("disconnect invented source: %v %#v", e, r)
	}
	client2, peer2 := net.Pipe()
	defer client2.Close()
	peer2.Close()
	conn2 := tr.NewConnection(nil, client2, client2)
	_, r, e = tr.SendRequestWithReceipt[int](conn2, context.Background(), "session/prompt", nil)
	if e == nil || r != nil {
		t.Fatalf("write failure invented source: %v %#v", e, r)
	}
	client3, peer3 := net.Pipe()
	defer client3.Close()
	defer peer3.Close()
	conn3 := tr.NewConnection(nil, client3, client3)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { bufio.NewReader(peer3).ReadBytes('\n'); cancel() }()
	_, r, e = tr.SendRequestWithReceipt[int](conn3, ctx, "session/prompt", nil)
	if e == nil || r != nil {
		t.Fatalf("local cancellation invented source: %v %#v", e, r)
	}
}
func TestInboundScopeAndStockDefault(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			r, w := io.Pipe()
			defer w.Close()
			defer r.Close()
			type got struct {
				method  string
				receipt *tr.Receipt
				wrong   bool
			}
			calls := make(chan got, 10)
			var selections atomic.Int64
			h := func(ctx context.Context, method string, raw json.RawMessage) (any, *tr.RequestError) {
				var p struct{ SessionID string }
				json.Unmarshal(raw, &p)
				calls <- got{method, tr.InboundReceipt(ctx, method, p.SessionID), tr.InboundReceipt(ctx, method, "wrong") != nil || tr.InboundReceipt(ctx, "other", p.SessionID) != nil}
				return true, nil
			}
			if enabled {
				tr.NewConnectionWithOptions(h, io.Discard, r, tr.Options{InboundMethods: []string{"session/update", "session/request_permission"}, SelectInbound: func(m, id string) bool { selections.Add(1); return id == "chosen" }, OmitPlainNotifications: true})
			} else {
				tr.NewConnection(h, io.Discard, r)
			}
			samples := []struct {
				method, fields, params, id string
				captured                   bool
			}{
				{"initialize", "", `{"sessionId":"chosen","secrets":"never collect"}`, "", false},
				{"session/update", "", `{"sessionId":"chosen","_meta":{"inner":true}}`, "", false},
				{"session/update", `,"vendor":{"wide":9007199254740993}`, `{"sessionId":"chosen"}`, "", true},
				{"session/update", `,"vendor":true`, `{"sessionId":"foreign"}`, "", false},
				{"session/update", `,"vendor":true`, `{"sessionId":"foreign","sessionId":"chosen"}`, "", false},
				{"session/request_permission", `,"id":"A\u0031"`, `{"sessionId":"chosen","requestId":"local-vendor"}`, `"A\u0031"`, true},
				{"session/request_permission", `,"id":"A2"`, `{"sessionId":"chosen","requestId":"local-vendor"}`, `"A2"`, true},
			}
			var refs []*tr.Receipt
			var lines []string
			for _, s := range samples {
				wire := fmt.Sprintf(`{ "jsonrpc":"2.0", "method":%q,"params":%s%s }`, s.method, s.params, s.fields)
				fmt.Fprintln(w, wire)
				wantMethod := s.method
				if enabled && s.params == `{"sessionId":"foreign","sessionId":"chosen"}` {
					// A subsequent notification fences the ordered handler queue.
					wantMethod = "consumer/fence"
					fmt.Fprintln(w, `{"jsonrpc":"2.0","method":"consumer/fence","params":{}}`)
				}
				select {
				case g := <-calls:
					if g.method != wantMethod || g.wrong || (g.receipt != nil) != (enabled && s.captured) {
						t.Fatalf("cross-scope or missing: %s", wire)
					}
					if g.receipt != nil {
						if string(g.receipt.ID) != s.id || string(g.receipt.Envelope) != wire {
							t.Fatal(g.receipt)
						}
						refs = append(refs, g.receipt)
						lines = append(lines, wire)
					}
				case <-time.After(time.Second):
					t.Fatal("handler not called")
				}
			}
			for i, p := range refs {
				if string(p.Envelope) != lines[i] {
					t.Fatal("scanner recycled a captured frame")
				}
			}
			wantSelections := int64(0)
			if enabled {
				wantSelections = 4
			}
			if n := selections.Load(); n != wantSelections {
				t.Fatalf("selector called %d times, want %d", n, wantSelections)
			}
		})
	}
}
func BenchmarkExternalReceiptCopy(b *testing.B) {
	for _, size := range []int{32, 128 << 10} {
		for _, capture := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/receipt=%v", size, capture), func(b *testing.B) {
				client, peer := net.Pipe()
				defer client.Close()
				defer peer.Close()
				conn := tr.NewConnection(nil, client, client)
				suffix := append(bytes.Repeat([]byte("z"), size), '"', '}', '\n')
				go func() {
					r := bufio.NewReader(peer)
					for {
						line, e := r.ReadBytes('\n')
						if e != nil {
							return
						}
						var arg struct{ ID json.RawMessage }
						if json.Unmarshal(line, &arg) != nil {
							return
						}
						prefix := append([]byte(`{"jsonrpc":"2.0","id":`), arg.ID...)
						prefix = append(prefix, []byte(`,"result":1,"padding":"`)...)
						if _, e = peer.Write(prefix); e != nil {
							return
						}
						if _, e = peer.Write(suffix); e != nil {
							return
						}
					}
				}()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if capture {
						v, r, err := tr.SendRequestWithReceipt[int](conn, context.Background(), "work", nil)
						if v != 1 || r == nil || err != nil {
							b.Fatal(err)
						}
					} else {
						v, err := tr.SendRequest[int](conn, context.Background(), "work", nil)
						if v != 1 || err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func TestConcurrentPermissionReceipts(t *testing.T) {
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	reader, output := io.Pipe()
	defer reader.Close()
	defer output.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	const first = `"req\u0031"`
	const second = `9007199254740993`
	type call struct {
		receipt *tr.Receipt
		release chan struct{}
	}
	started := make(chan call, 2)
	h := func(callCtx context.Context, method string, params json.RawMessage) (any, *tr.RequestError) {
		receipt := tr.InboundReceipt(callCtx, method, "chosen")
		release := make(chan struct{})
		started <- call{receipt, release}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return true, nil
	}
	tr.NewConnectionWithOptions(h, output, input, tr.Options{InboundMethods: []string{"session/request_permission"}, SelectInbound: func(m, s string) bool { return s == "chosen" }})
	// Identical ACP params on both calls leave only each RPC's wire ID to distinguish them.
	frame := func(id string) string {
		return `{"jsonrpc":"2.0","id":` + id + `,"method":"session/request_permission","params":{"sessionId":"chosen","options":[]}}`
	}
	for _, id := range []string{first, second} {
		if _, err := fmt.Fprintln(writer, frame(id)); err != nil {
			t.Fatal(err)
		}
	}
	waiting := map[string]chan struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case next := <-started:
			if next.receipt == nil {
				t.Fatal("missing scoped receipt")
			}
			id := string(next.receipt.ID)
			if string(next.receipt.Envelope) != frame(id) {
				t.Fatal("receipt and request disagree")
			}
			waiting[id] = next.release
		case <-ctx.Done():
			t.Fatal("concurrent permission handlers did not both start")
		}
	}
	replies := bufio.NewReader(reader)
	for _, id := range []string{second, first} {
		release, ok := waiting[id]
		if !ok {
			t.Fatal("request lost its original id")
		}
		close(release)
		line, err := replies.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			ID     json.RawMessage
			Result bool
		}
		if err = json.Unmarshal(line, &response); err != nil || string(response.ID) != id || !response.Result {
			t.Fatalf("reply to %s: %s", id, line)
		}
	}
}
