package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"
)

// Simple test client that sends metrics, logs, and traces to Telemy
// without using the OpenTelemetry SDK

const (
	metricsEndpoint = "http://localhost:4318/v1/metrics"
	logsEndpoint    = "http://localhost:4318/v1/logs"
	tracesEndpoint  = "http://localhost:4318/v1/traces"
)

var (
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func main() {
	fmt.Println("Starting simple Telemy test client...")
	fmt.Println("Sending data to:")
	fmt.Println("  Metrics: " + metricsEndpoint)
	fmt.Println("  Logs:    " + logsEndpoint)
	fmt.Println("  Traces:  " + tracesEndpoint)
	fmt.Println("\nPress Ctrl+C to stop")

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Start sending telemetry in separate goroutines
	stopChan := make(chan struct{})

	go sendMetrics(stopChan)
	go sendLogs(stopChan)
	go sendTraces(stopChan)

	// Wait for interrupt signal
	<-sigChan
	fmt.Println("\nShutting down...")
	close(stopChan)
	time.Sleep(100 * time.Millisecond) // Give goroutines time to finish
	fmt.Println("Shutdown complete.")
}

// sendMetrics sends CPU and memory metrics
func sendMetrics(stopChan <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Generate random CPU and memory values
			cpuValue := rnd.Float64() * 100
			memoryValue := 100 + rnd.Float64()*900

			// Create metrics payload with lowerCamelCase field names
			payload := map[string]interface{}{
				"resourceMetrics": []map[string]interface{}{
					{
						"resource": map[string]interface{}{
							"attributes": []map[string]interface{}{
								{
									"key": "service.name",
									"value": map[string]interface{}{
										"stringValue": "test-service",
									},
								},
							},
						},
						"scopeMetrics": []map[string]interface{}{
							{
								"scope": map[string]interface{}{
									"name":    "telemy-test",
									"version": "1.0.0",
								},
								"metrics": []map[string]interface{}{
									{
										"name":        "cpu_usage",
										"description": "CPU usage in percent",
										"unit":        "%",
										"gauge": map[string]interface{}{
											"dataPoints": []map[string]interface{}{
												{
													"timeUnixNano": time.Now().UnixNano(),
													"asDouble":     cpuValue,
													"attributes": []map[string]interface{}{
														{
															"key": "host",
															"value": map[string]interface{}{
																"stringValue": "test-server",
															},
														},
													},
												},
											},
										},
									},
									{
										"name":        "memory_usage",
										"description": "Memory usage in MB",
										"unit":        "MB",
										"gauge": map[string]interface{}{
											"dataPoints": []map[string]interface{}{
												{
													"timeUnixNano": time.Now().UnixNano(),
													"asDouble":     memoryValue,
													"attributes": []map[string]interface{}{
														{
															"key": "host",
															"value": map[string]interface{}{
																"stringValue": "test-server",
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}

			// Add Content-Type header for JSON format
			headers := map[string]string{
				"Content-Type": "application/json",
			}

			// Send metrics
			err := sendJSON(metricsEndpoint, payload, headers)
			if err != nil {
				log.Printf("Error sending metrics: %v", err)
			} else {
				fmt.Printf("Sent metrics: CPU=%.2f%%, Memory=%.2fMB\n", cpuValue, memoryValue)
			}
		}
	}
}

// sendLogs sends log entries
func sendLogs(stopChan <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	logLevels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	logMessages := []string{
		"User logged in",
		"Database connection established",
		"Failed to process request",
		"Cache miss",
		"Rate limit exceeded",
		"Authentication failed",
		"Payment processed successfully",
		"API request received",
		"Background job completed",
		"Configuration loaded",
	}

	severityMap := map[string]int{
		"DEBUG": 5,
		"INFO":  9,
		"WARN":  13,
		"ERROR": 17,
	}

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Generate random log entry
			level := logLevels[rnd.Intn(len(logLevels))]
			message := logMessages[rnd.Intn(len(logMessages))]

			// Create logs payload with lowerCamelCase field names
			payload := map[string]interface{}{
				"resourceLogs": []map[string]interface{}{
					{
						"resource": map[string]interface{}{
							"attributes": []map[string]interface{}{
								{
									"key": "service.name",
									"value": map[string]interface{}{
										"stringValue": "test-service",
									},
								},
							},
						},
						"scopeLogs": []map[string]interface{}{
							{
								"scope": map[string]interface{}{
									"name":    "telemy-test",
									"version": "1.0.0",
								},
								"logRecords": []map[string]interface{}{
									{
										"timeUnixNano":   time.Now().UnixNano(),
										"severityNumber": severityMap[level],
										"severityText":   level,
										"body": map[string]interface{}{
											"stringValue": message,
										},
										"attributes": []map[string]interface{}{
											{
												"key": "event.type",
												"value": map[string]interface{}{
													"stringValue": "log",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}

			// Add Content-Type header for JSON format
			headers := map[string]string{
				"Content-Type": "application/json",
			}

			// Send logs
			err := sendJSON(logsEndpoint, payload, headers)
			if err != nil {
				log.Printf("Error sending logs: %v", err)
			} else {
				fmt.Printf("Sent log: [%s] %s\n", level, message)
			}
		}
	}
}

// sendTraces sends trace data
func sendTraces(stopChan <-chan struct{}) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	operations := []string{
		"GET /api/users",
		"POST /api/orders",
		"PUT /api/products",
		"DELETE /api/items",
		"GET /api/dashboard",
	}

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Generate random trace
			operation := operations[rnd.Intn(len(operations))]
			traceID := generateID(16)
			spanID := generateID(8)
			startTime := time.Now().Add(-time.Duration(rnd.Intn(500)) * time.Millisecond)
			endTime := startTime.Add(time.Duration(50+rnd.Intn(200)) * time.Millisecond)

			// Create child spans
			numChildren := 1 + rnd.Intn(3)
			childSpans := make([]map[string]interface{}, numChildren)

			for i := 0; i < numChildren; i++ {
				childStartTime := startTime.Add(time.Duration(10+rnd.Intn(50)) * time.Millisecond)
				childEndTime := childStartTime.Add(time.Duration(20+rnd.Intn(100)) * time.Millisecond)

				childSpans[i] = map[string]interface{}{
					"traceId":           traceID,
					"spanId":            generateID(8),
					"parentSpanId":      spanID,
					"name":              fmt.Sprintf("%s - child %d", operation, i),
					"kind":              2, // SERVER
					"startTimeUnixNano": childStartTime.UnixNano(),
					"endTimeUnixNano":   childEndTime.UnixNano(),
					"attributes": []map[string]interface{}{
						{
							"key": "child.index",
							"value": map[string]interface{}{
								"intValue": i,
							},
						},
					},
					"events": []map[string]interface{}{
						{
							"timeUnixNano": childStartTime.Add(time.Duration(10) * time.Millisecond).UnixNano(),
							"name":         "processing",
							"attributes": []map[string]interface{}{
								{
									"key": "duration_ms",
									"value": map[string]interface{}{
										"intValue": 20 + rnd.Intn(100),
									},
								},
							},
						},
					},
					"status": map[string]interface{}{
						"code": 1, // OK
					},
				}
			}

			// Create traces payload with lowerCamelCase field names
			payload := map[string]interface{}{
				"resourceSpans": []map[string]interface{}{
					{
						"resource": map[string]interface{}{
							"attributes": []map[string]interface{}{
								{
									"key": "service.name",
									"value": map[string]interface{}{
										"stringValue": "test-service",
									},
								},
							},
						},
						"scopeSpans": []map[string]interface{}{
							{
								"scope": map[string]interface{}{
									"name":    "telemy-test",
									"version": "1.0.0",
								},
								"spans": append([]map[string]interface{}{
									{
										"traceId":           traceID,
										"spanId":            spanID,
										"name":              operation,
										"kind":              2, // SERVER
										"startTimeUnixNano": startTime.UnixNano(),
										"endTimeUnixNano":   endTime.UnixNano(),
										"attributes": []map[string]interface{}{
											{
												"key": "http.method",
												"value": map[string]interface{}{
													"stringValue": operation[:4],
												},
											},
											{
												"key": "http.url",
												"value": map[string]interface{}{
													"stringValue": operation[5:],
												},
											},
										},
										"status": map[string]interface{}{
											"code": 1, // OK
										},
									},
								}, childSpans...),
							},
						},
					},
				},
			}

			// Add Content-Type header for JSON format
			headers := map[string]string{
				"Content-Type": "application/json",
			}

			// Send traces
			err := sendJSON(tracesEndpoint, payload, headers)
			if err != nil {
				log.Printf("Error sending traces: %v", err)
			} else {
				fmt.Printf("Sent trace: %s with %d child spans\n", operation, numChildren)
			}
		}
	}
}

// sendJSON sends a JSON payload to the specified endpoint
func sendJSON(endpoint string, payload interface{}, headers map[string]string) error {
	// Marshal payload to JSON
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Send request
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// generateID generates a random ID of the specified length
func generateID(length int) string {
	const charset = "0123456789abcdef"
	b := make([]byte, length*2)
	for i := range b {
		b[i] = charset[rnd.Intn(len(charset))]
	}
	return string(b)
}
