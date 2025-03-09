package ingestion

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"telemy/storage"
)

// LogsHandler handles OTLP logs ingestion
type LogsHandler struct {
	store *storage.BadgerStore
}

// NewLogsHandler creates a new logs handler
func NewLogsHandler(store *storage.BadgerStore) *LogsHandler {
	return &LogsHandler{
		store: store,
	}
}

// HandleHTTP handles OTLP HTTP logs ingestion
func (h *LogsHandler) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse logs
	var logs OTLPLogsRequest
	if err := json.Unmarshal(body, &logs); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing logs: %v", err), http.StatusBadRequest)
		return
	}

	// Process logs
	err = h.processLogs(&logs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing logs: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

// RegisterGRPC registers the gRPC service
func (h *LogsHandler) RegisterGRPC(server *grpc.Server) {
	// For now, we'll just stub this out
	// We would need to implement the OTLP gRPC logs service here
	// This would require generating the OTLP protobuf code
}

// processLogs processes OTLP logs and stores them in the BadgerStore
func (h *LogsHandler) processLogs(logs *OTLPLogsRequest) error {
	// Process each resource logs
	for _, rl := range logs.ResourceLogs {
		// Extract resource attributes as labels
		resourceLabels := make(map[string]string)
		for _, attr := range rl.Resource.Attributes {
			resourceLabels[attr.Key] = attr.Value.StringValue
		}

		// Process each instrumentation library logs
		for _, ill := range rl.ScopeLogs {
			// Process each log record
			for i := range ill.LogRecords {
				record := &ill.LogRecords[i]
				// Extract log attributes as labels
				logLabels := copyLabels(resourceLabels)
				for _, attr := range record.Attributes {
					logLabels[attr.Key] = attr.Value.StringValue
				}

				// Create a log entry
				entry := &storage.LogEntry{
					Timestamp: time.Unix(0, record.TimeUnixNano),
					Level:     h.severityToLevel(record.SeverityNumber),
					Message:   record.Body.StringValue,
					Labels:    logLabels,
				}

				// Store the log entry
				if err := h.store.StoreLog(entry); err != nil {
					return fmt.Errorf("error storing log entry: %w", err)
				}
			}
		}
	}

	return nil
}

// severityToLevel converts an OTLP severity number to a log level string
func (h *LogsHandler) severityToLevel(severity SeverityNumber) string {
	switch severity {
	case SeverityNumberTrace:
		return "TRACE"
	case SeverityNumberDebug:
		return "DEBUG"
	case SeverityNumberInfo:
		return "INFO"
	case SeverityNumberWarn:
		return "WARN"
	case SeverityNumberError:
		return "ERROR"
	case SeverityNumberFatal:
		return "FATAL"
	default:
		return "INFO"
	}
}

// SeverityNumber represents the severity of a log record
type SeverityNumber int32

const (
	SeverityNumberUnspecified SeverityNumber = 0
	SeverityNumberTrace       SeverityNumber = 1
	SeverityNumberDebug       SeverityNumber = 5
	SeverityNumberInfo        SeverityNumber = 9
	SeverityNumberWarn        SeverityNumber = 13
	SeverityNumberError       SeverityNumber = 17
	SeverityNumberFatal       SeverityNumber = 21
)

// OTLPLogsRequest represents an OTLP logs request
type OTLPLogsRequest struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

// ResourceLogs represents a resource logs
type ResourceLogs struct {
	Resource  Resource    `json:"resource"`
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

// ScopeLogs represents instrumentation library logs
type ScopeLogs struct {
	Scope      Scope       `json:"scope"`
	LogRecords []LogRecord `json:"logRecords"`
}

// LogRecord represents a log record
type LogRecord struct {
	TimeUnixNano           int64          `json:"timeUnixNano"`
	SeverityNumber         SeverityNumber `json:"severityNumber"`
	SeverityText           string         `json:"severityText"`
	Body                   AnyValue       `json:"body"`
	Attributes             []Attribute    `json:"attributes"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount"`
	Flags                  uint32         `json:"flags"`
	TraceID                string         `json:"traceId"`
	SpanID                 string         `json:"spanId"`
}

// AnyValue represents a any value
type AnyValue struct {
	StringValue string  `json:"stringValue,omitempty"`
	IntValue    int64   `json:"intValue,omitempty"`
	DoubleValue float64 `json:"doubleValue,omitempty"`
	BoolValue   bool    `json:"boolValue,omitempty"`
}
