package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/daemon"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/lineage"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "", "Path to configuration file")
	showVersion := flag.Bool("v", false, "Show version information")
	flag.Parse()

	if *showVersion {
		// Show build information
		buildInfo := appconfig.GetBuildInfo()
		fmt.Printf("Version: %s\n", appconfig.GetVersionString())
		fmt.Printf("Git Hash: %s\n", buildInfo["git_hash"])
		fmt.Printf("Git Branch: %s\n", buildInfo["git_branch"])
		fmt.Printf("Git Tag: %s\n", buildInfo["git_tag"])
		fmt.Printf("Git Remote: %s\n", buildInfo["git_remote"])
		fmt.Printf("Build Time: %s\n", buildInfo["build_time"])
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

	// Validate lineage configuration if enabled
	config := service.GetConfig()
	if lineage.IsEnabled(config) {
		sugar.Infow("Lineage configuration details",
			"enabled", config.Lineage.Enabled,
			"marquez_url", config.Lineage.MarquezURL,
			"namespace", config.Lineage.Namespace)

		if err := lineage.ValidateConfig(config); err != nil {
			sugar.Errorw("Lineage configuration validation failed", "error", err)
			sugar.Errorw("Set lineage.enabled=true and lineage.marquez_url in config file")
			os.Exit(1)
		}
	} else {
		sugar.Info("Lineage is disabled")
	}

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start daemon service
	go func() {
		<-sigCh
		sugar.Info("Received signal, stopping daemon service...")
		service.Stop()
	}()

	sugar.Info("Daemon service started. Press Ctrl+C to stop.")

	if err := service.Start(); err != nil && err != context.Canceled {
		sugar.Errorw("Failed to start daemon service", "error", err)
		os.Exit(1)
	}

	sugar.Info("Daemon service stopped.")
}
