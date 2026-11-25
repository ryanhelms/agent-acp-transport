// Package transport implements an ACP-compatible newline-delimited JSON-RPC connection.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
)

// Options configures the receive loop before any peer bytes are read.
type Options struct {
	// Logger receives fixed diagnostic categories and bounded queue counts.
	// When nil, diagnostics go to slog.Default().
	Logger *slog.Logger
	// InboundMethods lists session-scoped methods eligible for receipts. Ambiguous
	// or missing routing members on these methods suppress handler dispatch.
	InboundMethods []string
	// SelectInbound is called on the receive goroutine with routing strings only.
	// It must return promptly, be concurrency-safe with the application, and avoid
	// waiting on operations that need the same reader to make progress.
	SelectInbound func(method, sessionID string) bool
	// OmitPlainNotifications skips receipts for notifications with just jsonrpc:"2.0",
	// method and params. Requests and extended notifications remain eligible.
	OmitPlainNotifications bool
	// HandleNotificationsInline uses the reader's own backpressure instead of
	// queueing notifications. Use only for bounded passive handlers. A handler
	// must not perform writes, issue RPCs, or wait for work that needs the reader.
	HandleNotificationsInline bool
}

// Receipt contains owned JSON from a single accepted peer frame. Envelope keeps
// the original spelling and spacing, excluding the newline delimiter. ID keeps
// the original JSON string, number or explicit null and is nil for a notification.
type Receipt struct {
	ID       json.RawMessage
	Envelope json.RawMessage
}

// DeliveryError reports a local notification-barrier or result-decoding
// failure after a real peer response was matched. Its companion Receipt still
// contains that response. A peer's JSON-RPC error is returned as *RequestError.
type DeliveryError struct{ Err error }

func (e *DeliveryError) Error() string { return "matched response delivery: " + e.Err.Error() }
func (e *DeliveryError) Unwrap() error { return e.Err }

type selectedReceipt struct {
	method    string
	sessionID string
	value     *Receipt
}

type receiptContextKey struct{}

// InboundReceipt returns a receipt only in the matching handler context and
// only when its method and verified session ID agree with the capture selection.
func InboundReceipt(ctx context.Context, method, sessionID string) *Receipt {
	s, _ := ctx.Value(receiptContextKey{}).(*selectedReceipt)
	if s == nil || s.method != method || s.sessionID != sessionID {
		return nil
	}
	return s.value
}

func copyReceipt(msg *anyMessage, line []byte) *Receipt {
	r := &Receipt{Envelope: bytes.Clone(line)}
	if msg.ID != nil {
		r.ID = bytes.Clone(msg.ID)
	}
	return r
}

func (c *Connection) selectFrame(msg *anyMessage, line []byte) (*selectedReceipt, bool) {
	if _, eligible := c.inboundMethods[msg.Method]; !eligible {
		return nil, true
	}
	fields, ok := inspectEnvelope(line)
	if !ok || fields.mask&(methodKey|paramsKey) != methodKey|paramsKey || fields.mask&(resultKey|errorKey) != 0 {
		return nil, false
	}
	var session json.RawMessage
	// Strict validation also protects ordinary notifications dispatched without a receipt:
	// aliases or duplicate routing members must never change the selected session.
	ok = members(msg.Params, func(key, value []byte) bool {
		if !bytes.EqualFold(key, []byte("sessionId")) {
			return true
		}
		if session != nil || !bytes.Equal(key, []byte("sessionId")) || len(value) < 3 || value[0] != '"' {
			return false
		}
		session = value
		return true
	})
	if !ok || session == nil {
		return nil, false
	}
	if len(msg.ID) > 0 && !validID(msg.ID) {
		return nil, false
	}
	if c.omitPlainNotifications && fields.plainNotification() {
		return nil, true
	}
	var sessionID string
	if json.Unmarshal(session, &sessionID) != nil || !c.selectInbound(msg.Method, sessionID) {
		return nil, true
	}
	return &selectedReceipt{method: msg.Method, sessionID: sessionID, value: copyReceipt(msg, line)}, true
}

const (
	versionKey uint8 = 1 << iota
	idKey
	methodKey
	paramsKey
	resultKey
	errorKey
)

type envelopeFields struct {
	mask     uint8
	unknown  bool
	version2 bool
}

func (f envelopeFields) plainNotification() bool {
	return !f.unknown && f.version2 && f.mask == versionKey|methodKey|paramsKey
}

var routeKeys = [...]string{"jsonrpc", "id", "method", "params", "result", "error"}

func inspectEnvelope(line []byte) (envelopeFields, bool) {
	var f envelopeFields
	ok := members(line, func(key, value []byte) bool {
		for i, name := range routeKeys {
			if !bytes.EqualFold(key, []byte(name)) {
				continue
			}
			bit := uint8(1 << i)
			// encoding/json accepts case aliases and last-wins duplicates.
			// They cannot identify a single unambiguous original for capture.
			if !bytes.Equal(key, []byte(name)) || f.mask&bit != 0 {
				return false
			}
			f.mask |= bit
			if bit == versionKey {
				f.version2 = bytes.Equal(value, []byte(`"2.0"`))
			}
			return true
		}
		f.unknown = true
		return true
	})
	return f, ok
}

// validID is used only on an already validated, nonempty JSON value.
func validID(id []byte) bool {
	return id[0] == '"' || id[0] == '-' || (id[0] >= '0' && id[0] <= '9') || bytes.Equal(id, []byte("null"))
}

// hasSingleID allows a protocol error for malformed session routing without
// ever selecting one of two conflicting IDs for the reply.
func hasSingleID(line []byte) bool {
	seen := false
	ok := members(line, func(key, value []byte) bool {
		if !bytes.EqualFold(key, []byte("id")) {
			return true
		}
		if seen || !bytes.Equal(key, []byte("id")) || !validID(value) {
			return false
		}
		seen = true
		return true
	})
	return ok && seen
}

func unambiguousReply(line []byte) bool {
	f, ok := inspectEnvelope(line)
	return ok && f.mask&idKey != 0 && f.mask&(methodKey|paramsKey) == 0 &&
		(f.mask&(resultKey|errorKey) == resultKey || f.mask&(resultKey|errorKey) == errorKey)
}

// members visits immediate members of an object already validated by encoding/json
// in the frame decoder. Values are borrowed spans; nested bodies are skipped without
// copying or tokenizing into interface values. Decoding a key only allocates for an escape.
func members(raw []byte, visit func(key, value []byte) bool) bool {
	s := bytes.TrimSpace(raw)
	if len(s) < 2 || s[0] != '{' {
		return false
	}
	i := skipSpace(s, 1)
	for i < len(s) && s[i] != '}' {
		if s[i] != '"' {
			return false
		}
		end := skipString(s, i)
		key := s[i+1 : end-1]
		if bytes.IndexByte(key, '\\') >= 0 {
			var decoded string
			if json.Unmarshal(s[i:end], &decoded) != nil {
				return false
			}
			key = []byte(decoded)
		}
		i = skipSpace(s, end)
		if i >= len(s) || s[i] != ':' {
			return false
		}
		i = skipSpace(s, i+1)
		if i >= len(s) {
			return false
		}
		end = skipValue(s, i)
		if !visit(key, s[i:end]) {
			return false
		}
		i = skipSpace(s, end)
		if i < len(s) && s[i] == ',' {
			i = skipSpace(s, i+1)
		}
	}
	return i == len(s)-1 && s[i] == '}'
}

func skipSpace(s []byte, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\n' || s[i] == '\t' || s[i] == '\r') {
		i++
	}
	return i
}

func skipString(s []byte, start int) int {
	// JSON was validated by the frame decoder. Jump directly to a quote; only
	// a quote preceded by an odd run of backslashes is part of the string.
	for start++; start < len(s); {
		n := bytes.IndexByte(s[start:], '"')
		if n < 0 {
			break
		}
		quote := start + n
		back := quote
		for back > start && s[back-1] == '\\' {
			back--
		}
		if (quote-back)%2 == 0 {
			return quote + 1
		}
		start = quote + 1
	}
	return len(s)
}

func skipValue(s []byte, i int) int {
	switch s[i] {
	case '"':
		return skipString(s, i)
	case '{', '[':
		depth := 1
		for i++; i < len(s); i++ {
			switch s[i] {
			case '"':
				i = skipString(s, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
	default:
		for ; i < len(s); i++ {
			switch s[i] {
			case ',', '}', ']', ' ', '\n', '\r', '\t':
				return i
			}
		}
	}
	return len(s)
}
