package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testPeer struct {
	conn *Connection
	in   *io.PipeWriter
	out  *inboundBarrierTestWire
}

func newTestPeer(t *testing.T, opts Options, h MethodHandler) testPeer {
	t.Helper()
	r, w := io.Pipe()
	out := newInboundBarrierTestWire()
	c := NewConnectionWithOptions(h, out, r, opts)
	t.Cleanup(func() { _ = w.Close(); <-c.Done(); _ = r.Close() })
	return testPeer{c, w, out}
}

func (p testPeer) write(t *testing.T, s string) {
	t.Helper()
	if _, err := io.WriteString(p.in, s+"\n"); err != nil {
		t.Fatal(err)
	}
}

func awaitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
	}
	var zero T
	return zero
}

type callResult struct {
	value   json.RawMessage
	receipt *Receipt
	err     error
}

func startCall(p testPeer, ctx context.Context, method string) <-chan callResult {
	ch := make(chan callResult, 1)
	go func() {
		v, r, e := SendRequestWithReceipt[json.RawMessage](p.conn, ctx, method, nil)
		ch <- callResult{v, r, e}
	}()
	return ch
}

func TestMatchedResponseReceipt(t *testing.T) {
	for _, code := range []int{0, -32603, -32800, -32020, 75} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			p := newTestPeer(t, Options{}, nil)
			c := startCall(p, context.Background(), "session/prompt")
			awaitValue(t, p.out.messages)
			p.write(t, `{"jsonrpc":"2.0","id":200,"result":{"ignored":true}}`)
			body := `"result": {"number": 12345678901234567890123456789, "spelling":"\u0041"}`
			if code != 0 {
				body = fmt.Sprintf(`"error": {"code":%d,"message":"fixture", "data":null, "further":[1,2]}`, code)
			}
			frame := `  {"id": 1e0, "jsonrpc":"2.0", "outer":{"n":12345678901234567890}, ` + body + `}  `
			p.write(t, frame)
			got := awaitValue(t, c)
			if got.receipt == nil || string(got.receipt.Envelope) != frame || string(got.receipt.ID) != "1e0" {
				t.Fatalf("wrong receipt %#v", got.receipt)
			}
			if code != 0 {
				var rpc *RequestError
				if !errors.As(got.err, &rpc) || rpc.Code != code || string(rpc.Data) != "null" {
					t.Fatalf("wrong peer error: %v", got.err)
				}
			} else if got.err != nil || !bytes.Contains(got.value, []byte("12345678901234567890123456789")) {
				t.Fatal(got.err, string(got.value))
			}
		})
	}
}

func TestAmbiguousAndUnmatchedDoNotConsumeSelectedRequest(t *testing.T) {
	p := newTestPeer(t, Options{}, nil)
	probe := make(chan error, 1)
	go func() {
		_, err := SendRequest[json.RawMessage](p.conn, context.Background(), "initialize", nil)
		probe <- err
	}()
	awaitValue(t, p.out.messages)
	call := startCall(p, context.Background(), "session/prompt")
	awaitValue(t, p.out.messages)
	p.write(t, `{"jsonrpc":"2.0","id":1,"result":{"setupOnly":true}}`)
	if err := awaitValue(t, probe); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		`{"id":1,"id":2,"result":"ambiguous"}`, `{"ID":2,"result":"case alias"}`,
		`{"id":2,"result":"two","error":{"code":10,"message":"ambiguous"}}`,
	} {
		p.write(t, line)
	}
	frame := `{"jsonrpc":"2.0", "id":2, "result":"selected"}`
	p.write(t, frame)
	got := awaitValue(t, call)
	if got.err != nil || got.receipt == nil || string(got.receipt.Envelope) != frame {
		t.Fatalf("wrong matched reply %#v %v", got.receipt, got.err)
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestLocalFailureHasNoReceipt(t *testing.T) {
	for _, source := range []string{"eof", "cancel", "write", "params"} {
		t.Run(source, func(t *testing.T) {
			p := newTestPeer(t, Options{}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if source == "write" || source == "params" {
				p.conn.w = failedWriter{}
				var params any
				if source == "params" {
					params = make(chan int)
				}
				_, r, err := SendRequestWithReceipt[json.RawMessage](p.conn, ctx, "test", params)
				if r != nil || err == nil {
					t.Fatal(r, err)
				}
				return
			}
			c := startCall(p, ctx, "test")
			awaitValue(t, p.out.messages)
			switch source {
			case "eof":
				_ = p.in.Close()
			case "cancel":
				cancel()
			}
			got := awaitValue(t, c)
			if got.err == nil || got.receipt != nil {
				t.Fatal(got)
			}
		})
	}
}

func TestScannerLimit(t *testing.T) {
	reader := strings.NewReader(strings.Repeat("x", 10<<20))
	c := NewConnection(nil, io.Discard, reader)
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("oversize input did not close connection")
	}
}

func TestScopedInboundReceipts(t *testing.T) {
	method := "session/update"
	const params = `{"sessionId":"selected","value":{"nested":[{"escaped":"} ] \" { "}]}}`
	type incoming struct {
		receipt *Receipt
		wrong   bool
	}
	arrivals := make(chan incoming, 2)
	var checks int
	p := newTestPeer(t, Options{InboundMethods: []string{method}, OmitPlainNotifications: true, SelectInbound: func(m, id string) bool { checks++; return id == "selected" }}, func(ctx context.Context, m string, _ json.RawMessage) (any, *RequestError) {
		arrivals <- incoming{InboundReceipt(ctx, m, "selected"), InboundReceipt(ctx, "wrong", "selected") != nil || InboundReceipt(ctx, m, "other") != nil}
		return nil, nil
	})
	for _, tc := range []struct {
		frame, id string
		capture   bool
	}{
		{`{"jsonrpc":"2.0","method":"session/update","params":` + params + `}`, "", false},
		{`{"jsonrpc":"2.0","method":"initialize","params":` + params + `,"secret":"setup"}`, "", false},
		{`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other"},"foreign":1}`, "", false},
		{`{"jsonrpc":"2.0","id":null,"method":"session/update","params":` + params + `}`, "null", true},
		{` {"jsonrpc":"2.0","method":"session/update","params":` + params + `, "outer":{"future": 1234567890123456789}} `, "", true},
		{`{"params":{"sessio\u006eId":"selected"}, "metho\u0064":"session/update", "id":"s\u002d01","jsonrpc":"2.0"}`, `"s\u002d01"`, true},
		{`{"jsonrpc":"2.0","id":9007199254740993,"method":"session/update","params":` + params + `}`, "9007199254740993", true},
	} {
		p.write(t, tc.frame)
		got := awaitValue(t, arrivals)
		if got.wrong || (got.receipt != nil) != tc.capture {
			t.Fatalf("wrong scope for %s", tc.frame)
		}
		if tc.capture && (string(got.receipt.Envelope) != tc.frame || string(got.receipt.ID) != tc.id) {
			t.Fatalf("bad spelling: %s", tc.frame)
		}
	}
	if checks != 5 {
		t.Fatalf("selector called %d times", checks)
	}
	if InboundReceipt(context.Background(), method, "selected") != nil {
		t.Fatal("receipt escaped its handler context")
	}
}

func TestAmbiguousInboundScopeIsRejectedBeforeDispatch(t *testing.T) {
	const valid = `{"sessionId":"selected"}`
	for _, frame := range []string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other","sessionId":"selected"}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other","SessionId":"selected"},"alias":1}`,
		`{"jsonrpc":"2.0","method":"setup","method":"session/update","params":` + valid + `,"duplicate":1}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other"},"Params":` + valid + `,"alias":1}`,
		`{"jsonrpc":"2.0","method":"session/update","id":8,"id":9,"params":` + valid + `}`,
	} {
		var dispatched atomic.Int32
		p := newTestPeer(t, Options{InboundMethods: []string{"session/update"}, OmitPlainNotifications: true, SelectInbound: func(m, id string) bool { return true }}, func(context.Context, string, json.RawMessage) (any, *RequestError) {
			dispatched.Add(1)
			return nil, nil
		})
		p.write(t, frame)
		_ = p.in.Close()
		<-p.conn.inboundCtx.Done()
		if dispatched.Load() != 0 {
			t.Errorf("ambiguous input dispatched: %s", frame)
		}
	}
}

func TestStandaloneCanCapturePlainNotification(t *testing.T) {
	out := make(chan *Receipt, 1)
	p := newTestPeer(t, Options{InboundMethods: []string{"m"}, SelectInbound: func(m, id string) bool { return id == "s" }}, func(ctx context.Context, m string, _ json.RawMessage) (any, *RequestError) {
		out <- InboundReceipt(ctx, m, "s")
		return nil, nil
	})
	frame := `{"jsonrpc":"2.0","method":"m","params":{"sessionId":"s"}}`
	p.write(t, frame)
	r := awaitValue(t, out)
	if r == nil || string(r.Envelope) != frame || r.ID != nil {
		t.Fatal(r)
	}
}

func TestMatchedReceiptFollowsNotificationWatermark(t *testing.T) {
	prior, after := make(chan struct{}), make(chan struct{})
	priorStarted, laterStarted := make(chan struct{}), make(chan struct{})
	p := newTestPeer(t, Options{}, func(ctx context.Context, m string, _ json.RawMessage) (any, *RequestError) {
		switch m {
		case "prior":
			close(priorStarted)
			<-prior
		case "after":
			close(laterStarted)
			<-after
		}
		return nil, nil
	})
	defer close(after)
	call := startCall(p, context.Background(), "run")
	awaitValue(t, p.out.messages)
	p.write(t, `{"jsonrpc":"2.0","method":"prior"}`)
	awaitValue(t, priorStarted)
	p.write(t, `{"jsonrpc":"2.0","id":1,"result":null}`)
	p.write(t, `{"jsonrpc":"2.0","method":"after"}`)
	select {
	case <-call:
		t.Fatal("receipt crossed earlier notification")
	default:
	}
	close(prior)
	awaitValue(t, laterStarted)
	got := awaitValue(t, call)
	if got.receipt == nil || got.err != nil {
		t.Fatal(got)
	}
}

type lockedLog struct {
	mu sync.Mutex
	bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.Buffer.Write(p)
}
func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.Buffer.String() }

func TestDiagnosticsNeverLogFrameValues(t *testing.T) {
	var log lockedLog
	in := strings.NewReader("not-json-PRIVATE\n" + `{"hello":"PRIVATE"}` + "\n" + `{"id":["PRIVATE"],"method":"PRIVATE"}` + "\n" + `{"method":"PRIVATE"}` + "\n")
	c := NewConnectionWithOptions(func(context.Context, string, json.RawMessage) (any, *RequestError) {
		return nil, NewInternalError("PRIVATE")
	}, io.Discard, in, Options{Logger: slog.New(slog.NewTextHandler(&log, nil))})
	select {
	case <-c.inboundCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("did not drain")
	}
	if strings.Contains(log.String(), "PRIVATE") || !strings.Contains(log.String(), "failed to parse incoming message") {
		t.Fatal("unexpected diagnostic content")
	}
}

func TestExplicitNullIDIsAnsweredAndInvalidScopeGetsProtocolError(t *testing.T) {
	calls := make(chan *Receipt, 1)
	p := newTestPeer(t, Options{InboundMethods: []string{"m"}, SelectInbound: func(m, id string) bool { return id == "s" }}, func(ctx context.Context, m string, _ json.RawMessage) (any, *RequestError) {
		calls <- InboundReceipt(ctx, m, "s")
		return nil, nil
	})
	p.write(t, `{"jsonrpc":"2.0","id":null,"method":"m","params":{"sessionId":"s"}}`)
	got := awaitValue(t, calls)
	if got == nil || string(got.ID) != "null" {
		t.Fatalf("lost explicit null: %#v", got)
	}
	var reply anyMessage
	if err := json.Unmarshal(awaitValue(t, p.out.messages), &reply); err != nil || string(reply.ID) != "null" || string(reply.Result) != "null" {
		t.Fatalf("wrong reply: %#v %v", reply, err)
	}
	p.write(t, `{"jsonrpc":"2.0","id":"valid","method":"m","params":{"sessionId":"s","SessionId":"other"}}`)
	if err := json.Unmarshal(awaitValue(t, p.out.messages), &reply); err != nil || string(reply.ID) != `"valid"` || reply.Error == nil || reply.Error.Code != -32600 {
		t.Fatalf("wrong protocol error: %#v %v", reply, err)
	}
	select {
	case <-calls:
		t.Fatal("ambiguous scope dispatched")
	default:
	}
}

func TestMatchedReplySurvivesImmediateEOF(t *testing.T) {
	for i := 0; i < 20; i++ {
		p := newTestPeer(t, Options{}, nil)
		call := startCall(p, context.Background(), "run")
		awaitValue(t, p.out.messages)
		p.write(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`)
		_ = p.in.Close()
		got := awaitValue(t, call)
		if got.err != nil || got.receipt == nil || string(got.value) != `"ok"` {
			t.Fatalf("lost matched reply on EOF: %v", got.err)
		}
	}
}

func TestAcceptedReceiptSurvivesLocalBarrierFailure(t *testing.T) {
	for _, end := range []string{"cancel", "disconnect"} {
		t.Run(end, func(t *testing.T) {
			held := make(chan struct{})
			started := make(chan struct{})
			p := newTestPeer(t, Options{}, func(context.Context, string, json.RawMessage) (any, *RequestError) {
				close(started)
				<-held
				return nil, nil
			})
			defer close(held)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			call := startCall(p, ctx, "run")
			awaitValue(t, p.out.messages)
			p.write(t, `{"jsonrpc":"2.0","method":"prior"}`)
			awaitValue(t, started)
			frame := `{"jsonrpc":"2.0","id":1,"error":{"code":-32800,"message":"wire","extra":true}}`
			p.write(t, frame)
			p.write(t, "") // Reading a subsequent line fences the completed match.
			if end == "cancel" {
				cancel()
			} else {
				_ = p.in.Close()
			}
			got := awaitValue(t, call)
			var local *DeliveryError
			if got.value != nil || got.receipt == nil || string(got.receipt.Envelope) != frame || !errors.As(got.err, &local) {
				t.Fatalf("matched receipt or local error lost: %#v %v", got.receipt, got.err)
			}
		})
	}
}
