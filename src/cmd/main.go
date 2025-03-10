package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"telemy/alerting"
	"telemy/config"
	"telemy/dashboard"
	"telemy/ingestion"
	"telemy/service"
	"telemy/storage"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config/config.json", "Path to configuration file")
	installService := flag.Bool("install", false, "Install as a Windows service")
	uninstallService := flag.Bool("uninstall", false, "Uninstall the Windows service")
	startService := flag.Bool("start", false, "Start the Windows service")
	stopService := flag.Bool("stop", false, "Stop the Windows service")
	flag.Parse()

	// Handle service management commands
	if *installService {
		if err := service.RunServiceCommand(service.Install); err != nil {
			log.Fatalf("Failed to install service: %v", err)
		}
		return
	}
	if *uninstallService {
		if err := service.RunServiceCommand(service.Uninstall); err != nil {
			log.Fatalf("Failed to uninstall service: %v", err)
		}
		return
	}
	if *startService {
		if err := service.RunServiceCommand(service.Start); err != nil {
			log.Fatalf("Failed to start service: %v", err)
		}
		return
	}
	if *stopService {
		if err := service.RunServiceCommand(service.Stop); err != nil {
			log.Fatalf("Failed to stop service: %v", err)
		}
		return
	}

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	setupLogger(cfg.Service.LogLevel)

	log.Printf("Starting %s...", cfg.Service.Name)

	// Initialize storage
	storageManager, err := storage.NewManager(cfg.Storage)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storageManager.Close()

	// Initialize ingestion
	ingestionManager, err := ingestion.NewManager(cfg.Ingestion, storageManager)
	if err != nil {
		log.Fatalf("Failed to initialize ingestion: %v", err)
	}
	defer ingestionManager.Close()

	// Initialize dashboard
	dashboardManager, err := dashboard.NewManager(cfg.Dashboard, storageManager)
	if err != nil {
		log.Fatalf("Failed to initialize dashboard: %v", err)
	}
	defer dashboardManager.Close()

	// Initialize alerting
	alertingManager, err := alerting.NewManager(cfg.Alerts, storageManager)
	if err != nil {
		log.Fatalf("Failed to initialize alerting: %v", err)
	}
	defer func() {
		if err := alertingManager.Stop(); err != nil {
			log.Printf("Error stopping alerting manager: %v", err)
		}
	}()

	// Start as Windows Service if installed as a service
	if service.IsWindowsService() {
		err = service.RunAsService(cfg, storageManager, ingestionManager, dashboardManager, alertingManager)
		if err != nil {
			log.Fatalf("Failed to run as service: %v", err)
		}
		return
	}

	// Start all components
	if err := ingestionManager.Start(); err != nil {
		log.Fatalf("Failed to start ingestion: %v", err)
	}

	if err := dashboardManager.Start(); err != nil {
		log.Fatalf("Failed to start dashboard: %v", err)
	}

	if err := alertingManager.Start(); err != nil {
		log.Fatalf("Failed to start alerting: %v", err)
	}

	// Setup signal handling for graceful shutdown
	setupSignalHandling(storageManager, ingestionManager, dashboardManager, alertingManager)

	fmt.Printf("%s is running. Press Ctrl+C to stop.\n", cfg.Service.Name)
	select {}
}

func setupLogger(logLevel string) {
	// Simple logger setup for now
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func setupSignalHandling(
	storageManager *storage.Manager,
	ingestionManager *ingestion.Manager,
	dashboardManager *dashboard.Manager,
	alertingManager *alerting.Manager,
) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down...", sig)

		// Shutdown components in reverse order
		alertingManager.Stop()
		dashboardManager.Stop()
		ingestionManager.Stop()
		storageManager.Close()

		log.Println("Shutdown complete. Exiting.")
		os.Exit(0)
	}()
}
