//go:build !windows
// +build !windows

package service

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"telemy/alerting"
	"telemy/config"
	"telemy/dashboard"
	"telemy/ingestion"
	"telemy/storage"
)

const serviceName = "TelemyService"

// IsWindowsService always returns false on non-Windows platforms
func IsWindowsService() bool {
	return false
}

// RunAsService starts the application on non-Windows platforms
func RunAsService(
	cfg *config.Config,
	storageManager *storage.Manager,
	ingestionManager *ingestion.Manager,
	dashboardManager *dashboard.Manager,
	alertingManager *alerting.Manager,
) error {
	log.Printf("Starting %s as a regular process on Unix-like platform...", cfg.Service.Name)

	// Start components
	if err := ingestionManager.Start(); err != nil {
		return fmt.Errorf("failed to start ingestion: %w", err)
	}

	if err := dashboardManager.Start(); err != nil {
		ingestionManager.Stop()
		return fmt.Errorf("failed to start dashboard: %w", err)
	}

	if err := alertingManager.Start(); err != nil {
		dashboardManager.Stop()
		ingestionManager.Stop()
		return fmt.Errorf("failed to start alerting: %w", err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down...", sig)

	// Shutdown components in reverse order
	alertingManager.Stop()
	dashboardManager.Stop()
	ingestionManager.Stop()

	log.Println("Shutdown complete.")
	return nil
}

// ServiceCommand is used to specify which service command to run
type ServiceCommand int

const (
	Install ServiceCommand = iota
	Uninstall
	Start
	Stop
)

// RunServiceCommand is a no-op on non-Windows platforms
func RunServiceCommand(cmd ServiceCommand) error {
	switch cmd {
	case Install:
		return fmt.Errorf("service installation not supported on non-Windows platforms")
	case Uninstall:
		return fmt.Errorf("service uninstallation not supported on non-Windows platforms")
	case Start:
		return fmt.Errorf("service start not supported on non-Windows platforms")
	case Stop:
		return fmt.Errorf("service stop not supported on non-Windows platforms")
	default:
		return fmt.Errorf("unknown service command: %d", cmd)
	}
}
