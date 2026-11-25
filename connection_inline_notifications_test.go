package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestInlineNotificationsDrainReplayBurst(t *testing.T) {
	const records = 2048
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	timeout := time.Now().Add(10 * time.Second)
	if err := peer.SetDeadline(timeout); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), timeout)
	defer cancel()

	var updates atomic.Int64
	connection := NewConnectionWithOptions(func(_ context.Context, method string, raw json.RawMessage) (any, *RequestError) {
		switch method {
		case "session/update":
			var record struct{ Index int64 }
			if err := json.Unmarshal(raw, &record); err != nil || record.Index != updates.Load()+1 {
				return nil, NewInvalidParams("out-of-order update")
			}
			updates.Add(1)
			return nil, nil
		case "session/request_permission":
			return updates.Load(), nil
		default:
			return nil, NewMethodNotFound(method)
		}
	}, client, client, Options{HandleNotificationsInline: true})
	result := make(chan error, 1)
	go func() {
		n, err := SendRequest[int](connection, ctx, "session/load", nil)
		if err == nil && n != records {
			err = fmt.Errorf("load returned %d, want %d", n, records)
		}
		result <- err
	}()

	reader, writer := json.NewDecoder(peer), bufio.NewWriter(peer)
	var load struct{ ID json.RawMessage }
	if err := reader.Decode(&load); err != nil || len(load.ID) == 0 {
		t.Fatalf("missing load request: %v", err)
	}
	write := func(format string, args ...any) {
		t.Helper()
		if _, err := fmt.Fprintf(writer, format, args...); err != nil {
			t.Fatal(err)
		}
	}
	flush := func() {
		t.Helper()
		if err := writer.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for index := 1; index <= records; index++ {
		write("{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"index\":%d}}\n", index)
		if index == records/2 {
			write("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/request_permission\"}\n")
			flush()
			var answer struct{ ID, Result int }
			if err := reader.Decode(&answer); err != nil || answer.ID != 1 || answer.Result != index {
				t.Fatalf("peer request must see all preceding replay events: %+v, %v", answer, err)
			}
		}
	}
	write("{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":%d}\n", load.ID, records)
	flush()
	select {
	case err := <-result:
		if err != nil || updates.Load() != records {
			t.Fatalf("load did not retain the full ordered replay: %d, %v", updates.Load(), err)
		}
	case <-ctx.Done():
		t.Fatal(errors.New("no response to session/load"))
	}
}
