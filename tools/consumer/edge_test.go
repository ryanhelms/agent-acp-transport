package consumer_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	tr "agent-acp.local/transport"
)

func TestObservedReplyInterruptedDuringEarlierNotification(t *testing.T) {
	for _, interrupt := range []string{"cancel", "disconnect"} {
		t.Run(interrupt, func(t *testing.T) {
			input, peerOutput := io.Pipe()
			peerInput, output := io.Pipe()
			defer input.Close()
			defer peerOutput.Close()
			defer peerInput.Close()
			defer output.Close()
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			conn := tr.NewConnection(func(ctx context.Context, m string, p json.RawMessage) (any, *tr.RequestError) {
				close(started)
				<-release
				return nil, nil
			}, output, input)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			type outcome struct {
				r *tr.Receipt
				e error
			}
			result := make(chan outcome, 1)
			go func() {
				_, r, e := tr.SendRequestWithReceipt[int](conn, ctx, "session/prompt", nil)
				result <- outcome{r, e}
			}()
			if _, e := bufio.NewReader(peerInput).ReadBytes('\n'); e != nil {
				t.Fatal(e)
			}
			if _, e := fmt.Fprintln(peerOutput, `{"jsonrpc":"2.0","method":"update","params":{}}`); e != nil {
				t.Fatal(e)
			}
			<-started
			exact := `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"native reply","vendor":9007199254740993}}`
			if _, e := fmt.Fprintln(peerOutput, exact); e != nil {
				t.Fatal(e)
			}
			// Serving an independent read from io.Pipe proves the previous frame was dispatched.
			if _, e := fmt.Fprintln(peerOutput, " "); e != nil {
				t.Fatal(e)
			}
			if interrupt == "cancel" {
				cancel()
			} else {
				peerOutput.Close()
			}
			select {
			case got := <-result:
				var local *tr.DeliveryError
				if !errors.As(got.e, &local) {
					t.Fatalf("expected local delivery failure, got %T", got.e)
				}
				if got.r == nil || string(got.r.Envelope) != exact {
					t.Errorf("matched peer envelope unavailable after %s during notification barrier; local error %T", interrupt, got.e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("stalled")
			}
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
func TestPartialWriter(t *testing.T) {
	in, out := io.Pipe()
	defer in.Close()
	defer out.Close()
	c := tr.NewConnection(nil, shortWriter{}, in)
	err := c.SendNotification(context.Background(), "work", nil)
	var local *tr.RequestError
	if !errors.As(err, &local) || !bytes.Contains(local.Data, []byte(io.ErrShortWrite.Error())) {
		t.Errorf("partial frame reported as delivered or unexplained: %v", err)
	}
}
