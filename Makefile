.PHONY: build run install uninstall start stop clean

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

# Build and run the application
dev: build
	./telemy.exe

# Default target
all: build 