# Telemy Project Structure

This document explains the structure of the Telemy project.

## Directory Structure

The project follows a standard Go project layout with the following main directories:

- `cmd/`: Contains the main application entry point
  - `main.go`: The main executable for Telemy

- `tools/`: Contains supporting tools and test utilities
  - `simple-test-client/`: A test client for generating sample telemetry data
  - `tsdb_test/`: A testing utility for the TSDB (time series database) component

- Core functionality modules:
  - `alerting/`: Alert management and notification
  - `config/`: Configuration handling
  - `dashboard/`: Web dashboard for visualization
  - `ingestion/`: Data ingestion endpoints (OTLP, etc.)
  - `query/`: Query engine for retrieving data
  - `service/`: Core service functionality
  - `storage/`: Storage engines for metrics, logs, and traces

- Other directories:
  - `data/`: Contains databases and storage files (created at the application root level)
  - `config/`: Contains configuration files

## Build Process

Telemy uses a Makefile for build automation. Key commands include:

- `make build`: Build the main application
- `make run`: Run the application
- `make build-test-client`: Build the test client
- `make build-tsdb-test`: Build the TSDB test tool
- `make build-tests`: Build all test tools

For Windows service management:
- `make install`: Install as a Windows service
- `make uninstall`: Uninstall the service
- `make start`: Start the service
- `make stop`: Stop the service

## Configuration

Configuration is managed through `config/config.json` in the config directory. 