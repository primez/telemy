# Telemy - All-in-One Telemetry Application

Telemy is an all-in-one telemetry application that replaces multiple components like OpenTelemetry Collector, Grafana Loki, Grafana Tempo, Grafana Mimir, and Grafana itself. It's designed to run as a Windows Service on edge servers, providing a lightweight and efficient solution for collecting, storing, and visualizing telemetry data.

## Features

- **Ingestion**: Receives telemetry data via the OpenTelemetry Protocol (OTLP) over HTTP and gRPC
- **Storage**: Stores logs, metrics, and traces using file-based storage engines
- **Dashboard**: Provides a built-in web dashboard for visualizing telemetry data
- **Alerting**: Supports rule-based alerting with email notifications
- **Windows Service**: Runs as a Windows Service for seamless integration with Windows servers

## Architecture

Telemy is built with a modular architecture, consisting of the following components:

- **Ingestion Module**: Receives OTLP data via HTTP and gRPC endpoints
- **Storage Module**: Stores telemetry data using TSDB (for metrics and traces) and BadgerDB (for logs)
- **Query Engine**: Executes queries against the storage engines
- **Dashboard Module**: Serves a web UI for visualizing telemetry data
- **Alerting Module**: Evaluates alert rules and sends notifications

## Getting Started

### Prerequisites

- Windows Server or Windows 10/11
- Go 1.21 or later

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/telemy.git
   cd telemy
   ```

2. Build the application:
   ```
   go build -o telemy.exe ./src/cmd
   ```

3. Configure the application by editing `config/config.json` (see Configuration section below)

4. Run the application directly (without installing as a service):
   ```
   telemy.exe
   ```
   This will create a `data` directory at the root of the application for storage files.

5. Or install and run as a Windows Service:
   ```
   telemy.exe -install
   telemy.exe -start
   ```

### Configuration

Telemy is configured using a JSON configuration file. By default, it looks for `config/config.json` in the config directory, but you can specify a different path using the `-config` flag.

Here's an example configuration:

```json
{
  "service": {
    "name": "TelemyService",
    "port": 8080,
    "protocols": ["http", "grpc"],
    "logLevel": "info"
  },
  "ingestion": {
    "otlp": {
      "httpEndpoint": "0.0.0.0:4318",
      "grpcEndpoint": "0.0.0.0:4317"
    }
  },
  "storage": {
    "metrics": {
      "engine": "tsdb",
      "dataPath": "./data/tsdb_metrics",
      "retentionPeriod": "30d",
      "indexConfig": {
        "blockSize": "2h",
        "compaction": true
      }
    },
    "logs": {
      "engine": "badger",
      "dataPath": "./data/badger_logs",
      "indexing": true,
      "maxFileSizeMB": 100
    },
    "traces": {
      "engine": "tsdb",
      "dataPath": "./data/tsdb_traces",
      "retentionPeriod": "7d"
    }
  },
  "dashboard": {
    "defaultView": "overview",
    "widgets": [
      {
        "type": "graph",
        "title": "CPU Usage",
        "dataSource": "metrics",
        "query": "__name__=cpu_usage",
        "refreshInterval": "30s"
      }
    ]
  },
  "alerts": {
    "email": {
      "enabled": true,
      "smtpServer": "smtp.example.com",
      "smtpPort": 587,
      "username": "alerts@example.com",
      "password": "your-password",
      "fromAddress": "alerts@example.com",
      "toAddresses": ["ops@example.com"],
      "templates": {
        "default": "Alert: {alertName} triggered at {timestamp}. Details: {details}"
      }
    },
    "rules": [
      {
        "name": "HighCPUUsage",
        "dataSource": "metrics",
        "query": "__name__=cpu_usage > 80",
        "duration": "5m",
        "severity": "critical"
      }
    ]
  }
}
```

### Usage

Once running, Telemy will:

1. Listen for OTLP data on the configured endpoints (default: HTTP on port 4318, gRPC on port 4317)
2. Store the received telemetry data in the configured storage engines
3. Serve a web dashboard on the configured port (default: 8080)
4. Evaluate alert rules and send notifications as configured

You can access the dashboard by navigating to `http://localhost:8080` in your web browser.

### Testing with the Test Client

Telemy includes a test client that can generate sample telemetry data for testing purposes. To use it:

1. Navigate to the test client directory:
   ```
   cd src/tools/test-client
   ```

2. Install dependencies:
   ```
   go mod tidy
   ```

3. Run the test client:
   ```
   go run main.go
   ```

The test client will continuously send metrics, logs, and traces to Telemy until you stop it with Ctrl+C. This is useful for testing the dashboard and alerting functionality without needing to set up real applications to send telemetry data.

See the [test client README](src/tools/test-client/README.md) for more details.

### Service Management

Telemy provides several command-line flags for managing the Windows Service:

- `-install`: Install as a Windows Service
- `-uninstall`: Uninstall the Windows Service
- `-start`: Start the Windows Service
- `-stop`: Stop the Windows Service

Example:
```
telemy.exe -install
telemy.exe -start
telemy.exe -stop
telemy.exe -uninstall
```

## Sending Telemetry Data

You can send telemetry data to Telemy using any OpenTelemetry SDK or client that supports the OTLP protocol. Here are some examples:

### Metrics

```python
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

# Configure the OTLP exporter
exporter = OTLPMetricExporter(endpoint="http://localhost:4318/v1/metrics")

# Create a metric reader
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)

# Create a meter provider
provider = MeterProvider(metric_readers=[reader])

# Set the global meter provider
metrics.set_meter_provider(provider)

# Create a meter
meter = metrics.get_meter("my-meter")

# Create a counter
counter = meter.create_counter("my-counter")

# Record a value
counter.add(1)
```

### Logs

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Configure the OTLP exporter
exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")

# Create a tracer provider
provider = TracerProvider()

# Add the exporter to the provider
provider.add_span_processor(BatchSpanProcessor(exporter))

# Set the global tracer provider
trace.set_tracer_provider(provider)

# Create a tracer
tracer = trace.get_tracer("my-tracer")

# Create a span
with tracer.start_as_current_span("my-span") as span:
    span.set_attribute("key", "value")
    span.add_event("event")
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [OpenTelemetry](https://opentelemetry.io/) for the OTLP protocol
- [BadgerDB](https://github.com/dgraph-io/badger) for the key-value storage engine
- [Gorilla Mux](https://github.com/gorilla/mux) for the HTTP router
- [Gorilla WebSocket](https://github.com/gorilla/websocket) for WebSocket support 