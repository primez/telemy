# Simple Telemy Test Client

This is a simple test client for Telemy that sends metrics, logs, and traces to the Telemy server without using the OpenTelemetry SDK. It's designed to be easy to run and doesn't have any external dependencies.

## Features

- Sends CPU and memory metrics every second
- Sends random log entries every 2 seconds
- Sends trace data with parent and child spans every 3 seconds
- All data is sent directly to Telemy's HTTP endpoints

## Requirements

- Go 1.21 or later
- A running Telemy server (default: http://localhost:4318)

## Running the Test Client

1. Make sure Telemy is running and accessible at http://localhost:8080
2. Navigate to this directory
3. Run the test client:

```bash
go run main.go
```

4. You should see output in the console showing the data being sent
5. Check the Telemy dashboard to see the incoming data
6. Press Ctrl+C to stop the test client

## Endpoints Used

- Metrics: http://localhost:4318/v1/metrics
- Logs: http://localhost:4318/v1/logs
- Traces: http://localhost:4318/v1/traces

## Troubleshooting

If you don't see data in the Telemy dashboard:

1. Verify that Telemy is running and accessible
2. Check the console output for any error messages
3. Ensure that the endpoints are correct (they can be modified in the code if needed)
4. Verify that your network allows connections to the specified endpoints 