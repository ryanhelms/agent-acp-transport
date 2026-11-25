package transport

import (
	"context"
	"io"
	"testing"
)

type shortRequestWriter struct{}

func (shortRequestWriter) Write(value []byte) (int, error) { return len(value) / 2, nil }

func TestRequestWriteObservationRejectsPartialWrite(t *testing.T) {
	read, closeRead := io.Pipe()
	defer closeRead.Close()
	seen := false
	opts := Options{AfterRequestWrite: func(_ string) { seen = true }}
	c := NewConnectionWithOptions(nil, shortRequestWriter{}, read, opts)
	if _, err := SendRequest[struct{}](c, context.Background(), "session/prompt", nil); err == nil {
		t.Fatal("short frame reported as sent")
	}
	if seen {
		t.Fatal("partial request reported as a completed write")
	}
}

// Measure creation and teardown of one initialized receive path. The stream
// ends without an ACP frame to isolate the reader's initial buffer overhead.
func BenchmarkIdleReceiveBuffer(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		c := NewConnection(nil, io.Discard, emptyReader{})
		<-c.Done()
	}
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
