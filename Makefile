.PHONY: build run install uninstall start stop clean build-test-client build-tsdb-test

# Build the application
build:
	go build -o telemy.exe ./src/cmd

# Run the application
run:
	./telemy.exe

# Install as a Windows service
install: build
	./telemy.exe -install

# Uninstall the Windows service
uninstall:
	./telemy.exe -uninstall

# Start the Windows service
start:
	./telemy.exe -start

# Stop the Windows service
stop:
	./telemy.exe -stop

# Clean build artifacts
clean:
	rm -f telemy.exe
	rm -f test-client.exe
	rm -f tsdb-test.exe

# Build and run the application
dev: build
	./telemy.exe

# Build the test client
build-test-client:
	go build -o test-client.exe ./src/tools/simple-test-client

# Build the TSDB test tool
build-tsdb-test:
	go build -o tsdb-test.exe ./src/tools/tsdb_test

# Build all test tools
build-tests: build-test-client build-tsdb-test

# Default target
all: build 