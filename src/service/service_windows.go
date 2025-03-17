//go:build windows
// +build windows

package service

import (
	"fmt"
	"log"
	"os"
	"sync"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"

	"telemy/alerting"
	"telemy/config"
	"telemy/dashboard"
	"telemy/ingestion"
	"telemy/storage"
)

const serviceName = "TelemyService"

// Service implements the Windows service interface
type Service struct {
	cfg              *config.Config
	storageManager   *storage.Manager
	ingestionManager *ingestion.Manager
	dashboardManager *dashboard.Manager
	alertingManager  *alerting.Manager
	stopChan         chan struct{}
	wg               sync.WaitGroup
	isRunning        bool
	elog             debug.Log
}

// IsWindowsService checks if the process is running as a Windows service
func IsWindowsService() bool {
	isService, err := svc.IsWindowsService()
	if err != nil {
		log.Printf("Failed to determine if running as service: %v", err)
		return false
	}
	return isService
}

// RunAsService starts the application as a Windows service
func RunAsService(
	cfg *config.Config,
	storageManager *storage.Manager,
	ingestionManager *ingestion.Manager,
	dashboardManager *dashboard.Manager,
	alertingManager *alerting.Manager,
) error {
	// Create the service instance
	s := &Service{
		cfg:              cfg,
		storageManager:   storageManager,
		ingestionManager: ingestionManager,
		dashboardManager: dashboardManager,
		alertingManager:  alertingManager,
		stopChan:         make(chan struct{}),
	}

	// Setup event log
	var err error
	s.elog, err = eventlog.Open(serviceName)
	if err != nil {
		s.elog = debug.New(serviceName)
	}

	// Run as a service
	return svc.Run(serviceName, s)
}

// Execute is called by the Windows service manager when the service is started
func (s *Service) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	// Set the service status to start pending
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}

	// Start the service components
	if err := s.start(); err != nil {
		s.elog.Error(1, fmt.Sprintf("Failed to start service: %v", err))
		changes <- svc.Status{State: svc.Stopped}
		return false, 1
	}

	// Set the service status to running
	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}

	// Wait for stop or shutdown signals
	for {
		select {
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				// Set the service status to stop pending
				changes <- svc.Status{State: svc.StopPending}

				// Stop the service
				s.stop()

				// Set the service status to stopped
				changes <- svc.Status{State: svc.Stopped}
				return false, 0
			default:
				s.elog.Error(1, fmt.Sprintf("Unexpected control request #%d", c))
			}
		}
	}
}

// start starts the service components
func (s *Service) start() error {
	s.elog.Info(1, "Starting Telemy service")

	// Start components
	if err := s.ingestionManager.Start(); err != nil {
		return fmt.Errorf("failed to start ingestion: %w", err)
	}

	if err := s.dashboardManager.Start(); err != nil {
		s.ingestionManager.Stop()
		return fmt.Errorf("failed to start dashboard: %w", err)
	}

	if err := s.alertingManager.Start(); err != nil {
		s.dashboardManager.Stop()
		s.ingestionManager.Stop()
		return fmt.Errorf("failed to start alerting: %w", err)
	}

	s.isRunning = true
	return nil
}

// stop gracefully stops the service components
func (s *Service) stop() {
	if !s.isRunning {
		return
	}

	s.elog.Info(1, "Stopping Telemy service")

	// Stop components in reverse order
	s.alertingManager.Stop()
	s.dashboardManager.Stop()
	s.ingestionManager.Stop()

	s.isRunning = false
}

// InstallService installs the Windows service
func InstallService(execPath string) error {
	// Open the service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Create the service
	s, err := m.CreateService(
		serviceName,
		execPath,
		mgr.Config{
			DisplayName: "Telemy Telemetry Service",
			Description: "All-in-one telemetry service for logs, metrics, and traces",
			StartType:   mgr.StartAutomatic,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer s.Close()

	// Setup event log
	err = eventlog.InstallAsEventCreate(serviceName, eventlog.Error|eventlog.Warning|eventlog.Info)
	if err != nil {
		s.Delete()
		return fmt.Errorf("failed to setup event log: %w", err)
	}

	log.Printf("Service %s installed successfully", serviceName)
	return nil
}

// UninstallService uninstalls the Windows service
func UninstallService() error {
	// Open the service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Open the service
	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("failed to open service: %w", err)
	}
	defer s.Close()

	// Delete the service
	err = s.Delete()
	if err != nil {
		return fmt.Errorf("failed to delete service: %w", err)
	}

	// Remove event log
	err = eventlog.Remove(serviceName)
	if err != nil {
		log.Printf("Failed to remove event log: %v", err)
	}

	log.Printf("Service %s uninstalled successfully", serviceName)
	return nil
}

// StartService starts the Windows service
func StartService() error {
	// Open the service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Open the service
	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("failed to open service: %w", err)
	}
	defer s.Close()

	// Start the service
	err = s.Start()
	if err != nil {
		return fmt.Errorf("failed to start service: %w", err)
	}

	log.Printf("Service %s started successfully", serviceName)
	return nil
}

// StopService stops the Windows service
func StopService() error {
	// Open the service manager
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	// Open the service
	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("failed to open service: %w", err)
	}
	defer s.Close()

	// Stop the service
	status, err := s.Control(svc.Stop)
	if err != nil {
		return fmt.Errorf("failed to send stop control: %w", err)
	}

	log.Printf("Service %s is stopping (current status: %v)", serviceName, status.State)
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

// RunServiceCommand runs the specified service command
func RunServiceCommand(cmd ServiceCommand) error {
	switch cmd {
	case Install:
		execPath, err := os.Executable()
		if err != nil {
			return fmt.Errorf("failed to get executable path: %w", err)
		}
		return InstallService(execPath)
	case Uninstall:
		return UninstallService()
	case Start:
		return StartService()
	case Stop:
		return StopService()
	default:
		return fmt.Errorf("unknown service command: %d", cmd)
	}
}
