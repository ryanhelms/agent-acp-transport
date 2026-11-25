package transport

import (
	"context"
	"errors"
	acp "github.com/coder/acp-go-sdk"
)

// RequestError is the ACP SDK's JSON-RPC error, so method handlers can use either package.
type RequestError = acp.RequestError

func NewMethodNotFound(method string) *RequestError { return acp.NewMethodNotFound(method) }
func NewInternalError(data any) *RequestError       { return acp.NewInternalError(data) }
func NewInvalidParams(data any) *RequestError       { return acp.NewInvalidParams(data) }
func NewRequestCancelled(data any) *RequestError    { return acp.NewRequestCancelled(data) }

func toReqErr(err error) *RequestError {
	if err == nil {
		return nil
	}
	if rpc, ok := err.(*RequestError); ok {
		return rpc
	}
	if errors.Is(err, context.Canceled) {
		return NewRequestCancelled(map[string]any{"error": err.Error()})
	}
	return NewInternalError(map[string]any{"error": err.Error()})
}
