package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"go.uber.org/zap"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	table table.Model
	db    *storage.DB
	width int
}

type tickMsg struct{}

func tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg{}
	})
}

func (m model) Init() tea.Cmd {
	return tick()
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		// Fixed columns: ID(10), Type(8), Status(10), Progress(8), plus 6 for table borders/padding
		fixed := 10 + 8 + 10 + 8 + 6
		avail := m.width - fixed
		if avail < 30 {
			avail = 30 // minimum reasonable width
		}
		createdW := 19
		srcDstW := int(math.Max(float64((avail-createdW)/2), 10))
		columns := []table.Column{
			{Title: "ID", Width: 10},
			{Title: "Created", Width: createdW},
			{Title: "Type", Width: 8},
			{Title: "Status", Width: 10},
			{Title: "Progress", Width: 8},
			{Title: "Source", Width: srcDstW},
			{Title: "Destination", Width: srcDstW},
		}
		m.table.SetColumns(columns)
		// Set table height to fill terminal (minus border and help line)
		// 2 for border, 1 for help line
		tableHeight := msg.Height - 3
		if tableHeight < 3 {
			tableHeight = 3
		}
		m.table.SetHeight(tableHeight)
		return m, nil
	case tickMsg:
		// Re-fetch jobs from DB
		jobs, err := m.db.GetAllJobs()
		if err == nil {
			rows := jobsToRows(jobs)
			m.table.SetRows(rows)
		}
		return m, tick()
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			return m, tea.Quit
		}
	}
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m model) View() string {
	return baseStyle.Render(m.table.View()) + "\nPress q to quit."
}

func jobsToRows(jobs []*jobs.Job) []table.Row {
	rows := make([]table.Row, 0, len(jobs))
	for _, job := range jobs {
		rows = append(rows, table.Row{
			job.ID,
			job.CreatedAt.Format(time.RFC3339),
			string(job.Type),
			string(job.Status),
			fmt.Sprintf("%.0f%%", job.Progress*100),
			job.Source,
			job.Destination,
		})
	}
	return rows
}

func main() {
	// Parse flags
	dbPath := flag.String("db", "autoloader.db", "Path to SQLite database")
	flag.Parse()

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
	jobs, err := db.GetAllJobs()
	if err != nil {
		sugar.Fatalf("Failed to fetch jobs: %v", err)
	}

	columns := []table.Column{
		{Title: "ID", Width: 10},
		{Title: "Created", Width: 19},
		{Title: "Type", Width: 8},
		{Title: "Status", Width: 10},
		{Title: "Progress", Width: 8},
		{Title: "Source", Width: 20},
		{Title: "Destination", Width: 20},
	}

	rows := jobsToRows(jobs)

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
	m := model{table: t, db: db}
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
		os.Exit(1)
	}
}
