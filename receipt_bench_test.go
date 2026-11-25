package transport

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func ordinaryFrame(n int) ([]byte, anyMessage) {
	raw := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"selected","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"` + strings.Repeat("t", n) + `"}}}}`)
	var msg anyMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		panic(err)
	}
	return raw, msg
}

func TestOrdinaryReceiptRoutingAllocatesNothing(t *testing.T) {
	frame, msg := ordinaryFrame(8192)
	c := &Connection{inboundMethods: map[string]struct{}{"session/update": {}}, selectInbound: func(string, string) bool { panic("plain update selected") }, omitPlainNotifications: true}
	allocs := testing.AllocsPerRun(100, func() {
		r, ok := c.selectFrame(&msg, frame)
		if r != nil || !ok {
			panic("plain receipt captured")
		}
	})
	if allocs != 0 {
		t.Fatalf("plain receipt routing allocated %.2f per update", allocs)
	}
}

func BenchmarkOrdinaryReceiptRouting(b *testing.B) {
	for _, size := range []int{64, 64 << 10} {
		frame, msg := ordinaryFrame(size)
		for _, tc := range []struct {
			name string
			conn *Connection
		}{
			{"disabled", &Connection{}},
			{"sdk-filtered", &Connection{inboundMethods: map[string]struct{}{"session/update": {}}, selectInbound: func(string, string) bool { return true }, omitPlainNotifications: true}},
			{"full-receipt", &Connection{inboundMethods: map[string]struct{}{"session/update": {}}, selectInbound: func(string, string) bool { return true }}},
		} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, size), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					r, ok := tc.conn.selectFrame(&msg, frame)
					if !ok || (r != nil) != (tc.name == "full-receipt") {
						b.Fatal("unexpected receipt")
					}
				}
			})
		}
	}
}
