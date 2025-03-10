package ingestion

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"telemy/storage"
)

// TracesHandler handles OTLP traces ingestion
type TracesHandler struct {
	store *storage.PromTSDBStore
}

// NewTracesHandler creates a new traces handler
func NewTracesHandler(store *storage.PromTSDBStore) *TracesHandler {
	return &TracesHandler{
		store: store,
	}
}

// HandleHTTP handles OTLP HTTP traces ingestion
func (h *TracesHandler) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse traces
	var traces OTLPTracesRequest
	if err := json.Unmarshal(body, &traces); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing traces: %v", err), http.StatusBadRequest)
		return
	}

	// Process traces
	err = h.processTraces(&traces)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing traces: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

// RegisterGRPC registers the gRPC service
func (h *TracesHandler) RegisterGRPC(server *grpc.Server) {
	// For now, we'll just stub this out
	// We would need to implement the OTLP gRPC traces service here
	// This would require generating the OTLP protobuf code
}

// processTraces processes OTLP traces and stores them in the TSDB
func (h *TracesHandler) processTraces(traces *OTLPTracesRequest) error {
	// Process each resource spans
	for _, rs := range traces.ResourceSpans {
		// Extract resource attributes as labels
		resourceLabels := make(map[string]string)
		for _, attr := range rs.Resource.Attributes {
			resourceLabels[attr.Key] = attr.Value.StringValue
		}

		// Process each instrumentation library spans
		for _, ils := range rs.ScopeSpans {
			// Process each span
			for i := range ils.Spans {
				span := &ils.Spans[i]
				// Extract span attributes as labels
				spanLabels := copyLabels(resourceLabels)
				for _, attr := range span.Attributes {
					spanLabels[attr.Key] = attr.Value.StringValue
				}

				// Add span metadata as labels
				spanLabels["service.name"] = resourceLabels["service.name"]
				spanLabels["span.name"] = span.Name
				spanLabels["span.kind"] = spanKindToString(span.Kind)
				spanLabels["trace.id"] = span.TraceID
				spanLabels["span.id"] = span.SpanID
				if span.ParentSpanID != "" {
					spanLabels["parent.span.id"] = span.ParentSpanID
				}

				// Create a data point for the span duration
				startTime := time.Unix(0, span.StartTimeUnixNano)
				endTime := time.Unix(0, span.EndTimeUnixNano)
				duration := endTime.Sub(startTime).Microseconds()

				durationPoint := &storage.DataPoint{
					Timestamp: startTime,
					Value:     float64(duration),
					Labels:    spanLabels,
				}

				// Store the span duration
				if err := h.store.StoreMetric(durationPoint); err != nil {
					return fmt.Errorf("error storing span duration: %w", err)
				}

				// Process span events
				for _, event := range span.Events {
					eventLabels := copyLabels(spanLabels)
					eventLabels["event.name"] = event.Name
					eventTimeUnixNano := event.TimeUnixNano
					if eventTimeUnixNano == 0 {
						eventTimeUnixNano = span.StartTimeUnixNano
					}

					// Store each event attribute as a separate data point
					for _, attr := range event.Attributes {
						eventAttrLabels := copyLabels(eventLabels)
						eventAttrLabels["event.attr.key"] = attr.Key

						// Convert the attribute value to a float64
						var value float64
						switch {
						case attr.Value.DoubleValue != 0:
							value = attr.Value.DoubleValue
						case attr.Value.IntValue != 0:
							value = float64(attr.Value.IntValue)
						case attr.Value.BoolValue:
							value = 1.0
						default:
							// Skip string values or use a hash
							continue
						}

						eventPoint := &storage.DataPoint{
							Timestamp: time.Unix(0, eventTimeUnixNano),
							Value:     value,
							Labels:    eventAttrLabels,
						}

						if err := h.store.StoreMetric(eventPoint); err != nil {
							return fmt.Errorf("error storing span event: %w", err)
						}
					}
				}

				// Process span status
				statusLabels := copyLabels(spanLabels)
				statusLabels["status.code"] = strconv.Itoa(int(span.Status.Code))
				if span.Status.Message != "" {
					statusLabels["status.message"] = span.Status.Message
				}

				statusPoint := &storage.DataPoint{
					Timestamp: endTime,
					Value:     float64(span.Status.Code),
					Labels:    statusLabels,
				}

				if err := h.store.StoreMetric(statusPoint); err != nil {
					return fmt.Errorf("error storing span status: %w", err)
				}
			}
		}
	}

	return nil
}

// spanKindToString converts a span kind to a string representation
func spanKindToString(kind SpanKind) string {
	switch kind {
	case SpanKindUnspecified:
		return "UNSPECIFIED"
	case SpanKindInternal:
		return "INTERNAL"
	case SpanKindServer:
		return "SERVER"
	case SpanKindClient:
		return "CLIENT"
	case SpanKindProducer:
		return "PRODUCER"
	case SpanKindConsumer:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

// SpanKind represents the span kind
type SpanKind int32

const (
	SpanKindUnspecified SpanKind = 0
	SpanKindInternal    SpanKind = 1
	SpanKindServer      SpanKind = 2
	SpanKindClient      SpanKind = 3
	SpanKindProducer    SpanKind = 4
	SpanKindConsumer    SpanKind = 5
)

// StatusCode represents the span status code
type StatusCode int32

const (
	StatusCodeUnset StatusCode = 0
	StatusCodeOk    StatusCode = 1
	StatusCodeError StatusCode = 2
)

// OTLPTracesRequest represents an OTLP traces request
type OTLPTracesRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

// ResourceSpans represents a resource spans
type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
}

// ScopeSpans represents instrumentation library spans
type ScopeSpans struct {
	Scope Scope  `json:"scope"`
	Spans []Span `json:"spans"`
}

// Span represents a span
type Span struct {
	TraceID           string      `json:"traceId"`
	SpanID            string      `json:"spanId"`
	ParentSpanID      string      `json:"parentSpanId,omitempty"`
	Name              string      `json:"name"`
	Kind              SpanKind    `json:"kind"`
	StartTimeUnixNano int64       `json:"startTimeUnixNano"`
	EndTimeUnixNano   int64       `json:"endTimeUnixNano"`
	Attributes        []Attribute `json:"attributes"`
	Events            []SpanEvent `json:"events"`
	Links             []SpanLink  `json:"links"`
	Status            Status      `json:"status"`
}

// SpanEvent represents a span event
type SpanEvent struct {
	TimeUnixNano int64       `json:"timeUnixNano"`
	Name         string      `json:"name"`
	Attributes   []Attribute `json:"attributes"`
}

// SpanLink represents a span link
type SpanLink struct {
	TraceID    string      `json:"traceId"`
	SpanID     string      `json:"spanId"`
	Attributes []Attribute `json:"attributes"`
}

// Status represents a span status
type Status struct {
	Code    StatusCode `json:"code"`
	Message string     `json:"message"`
}
