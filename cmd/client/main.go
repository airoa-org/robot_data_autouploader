package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/airoa-org/robot_data_pipeline/autoloader/cmd/client/handlers"
	"github.com/airoa-org/robot_data_pipeline/autoloader/cmd/client/ui"
	appconfig "github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	// Parse flags
	dbPath := flag.String("db", "autoloader.db", "Path to SQLite database")
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

	// Set up logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// Open DB
	db, err := storage.NewDB(*dbPath, sugar)
	if err != nil {
		sugar.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Fetch all jobs
	allJobs, err := db.GetAllJobs()
	if err != nil {
		sugar.Fatalf("Failed to fetch jobs: %v", err)
	}

	// Create initial model
	m := ui.Model{
		DB:    db,
		State: ui.StateJobList,
		Jobs:  allJobs,
	}

	// Set up table columns and rows
	columns := m.CreateJobListColumns(80) // Default width
	rows := m.JobsToRows(allJobs)

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(12),
	)

	// Style the table
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(true)
	t.SetStyles(s)

	// Set the table and other fields
	m.Table = t
	m.MessageStyle = ui.InfoTextStyle
	m.RecreateJobHandler = handlers.RecreateJob

	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
		os.Exit(1)
	}
}
