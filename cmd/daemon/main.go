package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/daemon"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "", "Path to configuration file")
	showVersion := flag.Bool("v", false, "Show version information")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	// Create logger
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// Create daemon service
	service, err := daemon.NewService(*configPath)
	if err != nil {
		sugar.Errorw("Failed to create daemon service", "error", err)
		os.Exit(1)
	}

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start daemon service
	go func() {
		if err := service.Start(); err != nil {
			sugar.Errorw("Failed to start daemon service", "error", err)
			os.Exit(1)
		}
	}()

	sugar.Info("Daemon service started. Press Ctrl+C to stop.")

	// Wait for signal
	<-sigCh
	sugar.Info("Received signal, stopping daemon service...")

	// Stop daemon service
	service.Stop()

	sugar.Info("Daemon service stopped.")
}
