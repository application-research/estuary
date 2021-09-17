package drpc

import (
	"encoding/json"

	"go.opentelemetry.io/otel/trace"
)

/*
TraceCarrier is required to Marshal and Unmarshall trace.SpanContext across RPC boundaries since the
OpenTelemetry spec dictates trace.SpanContext must be read only, and therefore does not implement a
JSON unmarshaller.
Context: https://github.com/open-telemetry/opentelemetry-go/issues/1927#issuecomment-842663910
*/

// NewTraceCarrier accepts a trace.SpanContext and returns a TraceCarrier.
func NewTraceCarrier(sc trace.SpanContext) *TraceCarrier {
	if sc.IsValid() {
		return &TraceCarrier{
			TraceID: sc.TraceID(),
			SpanID:  sc.SpanID(),
			Remote:  sc.IsRemote(),
		}
	}
	return nil
}

// TraceCarrier is a wrapper that allows trace.SpanContext's to be round-tripped through JSON.
type TraceCarrier struct {
	TraceID trace.TraceID `json:"traceID"`
	SpanID  trace.SpanID  `json:"spanID"`
	Remote  bool          `json:"remote"`
}

//MarshalJSON converts TraceCarrier to a trace.SpanContext and marshals it to JSON.
func (c *TraceCarrier) MarshalJSON() ([]byte, error) {
	return c.AsSpanContext().MarshalJSON()
}

// UnmarshalJSON unmarshalls a serialized trace.SpanContext to a TraceCarrier.
func (c *TraceCarrier) UnmarshalJSON(b []byte) error {
	var data traceCarrierInfo
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	var err error
	c.TraceID, err = trace.TraceIDFromHex(data.TraceID)
	if err != nil {
		return err
	}
	c.SpanID, err = trace.SpanIDFromHex(data.SpanID)
	if err != nil {
		return err
	}
	c.Remote = data.Remote

	return nil
}

// AsSpanContext converts TraceCarrier to a trace.SpanContext.
func (c *TraceCarrier) AsSpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: c.TraceID,
		SpanID:  c.SpanID,
		Remote:  c.Remote,
	})
}

// carrierInfo is a helper used to deserialize a SpanContext from JSON.
type traceCarrierInfo struct {
	TraceID string
	SpanID  string
	Remote  bool
}
