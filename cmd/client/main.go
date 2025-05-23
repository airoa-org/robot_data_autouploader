package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobops"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"go.uber.org/zap"
)

// Define UI styles
var (
	baseStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240"))

	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("63")).
			Bold(true).
			Padding(0, 1)

	promptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205"))

	infoTextStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("197")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("76")).
			Bold(true)
)

// UI states
const (
	stateJobList      = "job_list"
	stateConfirmation = "confirmation"
	stateMessage      = "message"
	stateJobDetail    = "job_detail"
)

type model struct {
	table              table.Model
	viewport           viewport.Model
	db                 *storage.DB
	width              int
	height             int
	state              string
	selectedJob        *jobs.Job
	confirmationPrompt string
	message            string
	messageStyle       lipgloss.Style
	messageTimeout     *time.Time
	jobs               []*jobs.Job
	viewportReady      bool
}

type tickMsg struct{}

func tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg{}
	})
}

type messageTimeoutMsg struct{}

func messageTimeout() tea.Cmd {
	return tea.Tick(3*time.Second, func(t time.Time) tea.Msg {
		return messageTimeoutMsg{}
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
		m.height = msg.Height

		// Handle viewport sizing for job detail view
		if m.state == stateJobDetail {
			headerHeight := lipgloss.Height(m.detailHeaderView())
			footerHeight := lipgloss.Height(m.detailFooterView())
			verticalMarginHeight := headerHeight + footerHeight

			if !m.viewportReady {
				m.viewport = viewport.New(msg.Width, msg.Height-verticalMarginHeight)
				m.viewport.YPosition = headerHeight
				if m.selectedJob != nil {
					m.viewport.SetContent(m.formatJobDetails(m.selectedJob))
				}
				m.viewportReady = true
			} else {
				m.viewport.Width = msg.Width
				m.viewport.Height = msg.Height - verticalMarginHeight
			}
		}

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
		if m.state == stateJobList {
			// Re-fetch jobs from DB
			jobs, err := m.db.GetAllJobs()
			if err == nil {
				m.jobs = jobs
				rows := jobsToRows(jobs)
				m.table.SetRows(rows)
			}
			return m, tick()
		}
		return m, tick()

	case messageTimeoutMsg:
		if m.messageTimeout != nil && time.Now().After(*m.messageTimeout) {
			m.state = stateJobList
			m.message = ""
			m.messageTimeout = nil
			return m, tick()
		}
		return m, nil

	case tea.KeyMsg:
		switch m.state {
		case stateJobList:
			switch msg.String() {
			case "esc":
				if m.table.Focused() {
					m.table.Blur()
				} else {
					m.table.Focus()
				}
				return m, nil

			case "q", "ctrl+c":
				return m, tea.Quit

			case "enter":
				// Show job details
				if len(m.table.SelectedRow()) > 0 {
					jobID := m.table.SelectedRow()[0]
					// Load complete job data
					for _, job := range m.jobs {
						if job.ID == jobID {
							m.selectedJob = job
							break
						}
					}

					if m.selectedJob != nil {
						// Switch to job detail state
						m.state = stateJobDetail
						m.viewportReady = false
						// Trigger window size message to initialize viewport
						return m, func() tea.Msg {
							return tea.WindowSizeMsg{Width: m.width, Height: m.height}
						}
					}
				}
				return m, nil

			case "r":
				// Only allow recreation of upload jobs
				if m.table.SelectedRow()[2] == string(jobs.JobTypeUpload) {
					// Get the selected job ID
					jobID := m.table.SelectedRow()[0]
					// Load complete job data
					for _, job := range m.jobs {
						if job.ID == jobID {
							m.selectedJob = job
							break
						}
					}

					if m.selectedJob != nil {
						// Switch to confirmation state
						m.state = stateConfirmation
						m.confirmationPrompt = fmt.Sprintf(
							"Are you sure you want to recreate this upload job?\n\n"+
								"Job ID: %s\nSource: %s\nDestination: %s\n\n"+
								"Please verify that the source directory still contains the correct files.\n\n"+
								"Press y to confirm, n to cancel",
							m.selectedJob.ID, m.selectedJob.Source, m.selectedJob.Destination)
					}
				}
				return m, nil
			}

		case stateJobDetail:
			switch msg.String() {
			case "esc", "q":
				// Return to job list
				m.state = stateJobList
				m.selectedJob = nil
				m.viewportReady = false
				return m, nil

			case "ctrl+c":
				return m, tea.Quit
			}

		case stateConfirmation:
			switch msg.String() {
			case "y", "Y":
				// User confirmed, recreate the job
				newJob, err := recreateJob(m.db, m.selectedJob.ID)
				if err != nil {
					// Show error message
					m.state = stateMessage
					m.message = fmt.Sprintf("Failed to recreate job: %v", err)
					m.messageStyle = errorStyle
					timeout := time.Now().Add(5 * time.Second)
					m.messageTimeout = &timeout
					return m, messageTimeout()
				}

				// Show success message
				m.state = stateMessage
				m.message = fmt.Sprintf("Successfully recreated job. New job ID: %s", newJob.ID)
				m.messageStyle = successStyle
				timeout := time.Now().Add(3 * time.Second)
				m.messageTimeout = &timeout
				return m, messageTimeout()

			case "n", "N", "esc":
				// Cancel and return to job list
				m.state = stateJobList
				m.selectedJob = nil
				return m, nil

			case "q", "ctrl+c":
				return m, tea.Quit
			}

		case stateMessage:
			switch msg.String() {
			case "esc", "enter", " ":
				// Return to job list
				m.state = stateJobList
				m.message = ""
				m.messageTimeout = nil
				return m, nil

			case "q", "ctrl+c":
				return m, tea.Quit
			}
		}
	}

	// Handle table updates
	if m.state == stateJobList {
		m.table, cmd = m.table.Update(msg)
	}

	// Handle viewport updates
	if m.state == stateJobDetail && m.viewportReady {
		m.viewport, cmd = m.viewport.Update(msg)
	}

	return m, cmd
}

func (m model) View() string {
	switch m.state {
	case stateConfirmation:
		// Show the confirmation screen
		return baseStyle.Render(
			titleStyle.Render("RECREATE JOB") + "\n\n" +
				promptStyle.Render(m.confirmationPrompt),
		)

	case stateMessage:
		// Show a temporary message
		return baseStyle.Render(
			m.messageStyle.Render(m.message) + "\n\n" +
				infoTextStyle.Render("Press any key to return"),
		)

	case stateJobDetail:
		// Show job details in viewport
		if !m.viewportReady {
			return "\n  Loading job details..."
		}
		return fmt.Sprintf("%s\n%s\n%s", m.detailHeaderView(), m.viewport.View(), m.detailFooterView())

	default: // stateJobList
		helpText := "Press q to quit, enter to view details"
		if len(m.table.SelectedRow()) > 0 && m.table.SelectedRow()[2] == string(jobs.JobTypeUpload) {
			helpText += ", r to recreate selected upload job"
		}
		return baseStyle.Render(m.table.View()) + "\n" + helpText
	}
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

// formatJobDetails formats all job information for display in the viewport
func (m model) formatJobDetails(job *jobs.Job) string {
	var details strings.Builder

	details.WriteString(fmt.Sprintf("Job ID: %s\n", job.ID))
	details.WriteString(fmt.Sprintf("Created: %s\n", job.CreatedAt.Format(time.RFC3339)))
	details.WriteString(fmt.Sprintf("Updated: %s\n", job.UpdatedAt.Format(time.RFC3339)))
	details.WriteString(fmt.Sprintf("Type: %s\n", job.Type))
	details.WriteString(fmt.Sprintf("Status: %s\n", job.Status))
	details.WriteString(fmt.Sprintf("Progress: %.1f%%\n", job.Progress*100))
	details.WriteString(fmt.Sprintf("Source: %s\n", job.Source))
	details.WriteString(fmt.Sprintf("Destination: %s\n", job.Destination))
	details.WriteString(fmt.Sprintf("File Count: %d\n", job.FileCount))
	details.WriteString(fmt.Sprintf("Total Size: %d bytes\n", job.TotalSize))
	details.WriteString(fmt.Sprintf("Processed Size: %d bytes\n", job.ProcessedSize))

	if job.ErrorMsg.Valid && job.ErrorMsg.String != "" {
		details.WriteString(fmt.Sprintf("\nError: %s\n", job.ErrorMsg.String))
	}

	if len(job.Metadata) > 0 {
		details.WriteString("\nMetadata:\n")
		for key, value := range job.Metadata {
			details.WriteString(fmt.Sprintf("  %s: %s\n", key, value))
		}
	}

	// Add some spacing at the end
	details.WriteString("\n\n")

	return details.String()
}

// detailHeaderView creates the header for the job detail viewport
func (m model) detailHeaderView() string {
	title := titleStyle.Render("Job Details")
	line := strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(title)))
	return lipgloss.JoinHorizontal(lipgloss.Center, title, line)
}

// detailFooterView creates the footer for the job detail viewport
func (m model) detailFooterView() string {
	info := infoTextStyle.Render(fmt.Sprintf("%3.f%% • Press esc to return", m.viewport.ScrollPercent()*100))
	line := strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(info)))
	return lipgloss.JoinHorizontal(lipgloss.Center, line, info)
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// recreateJob recreates a job from the original job ID and ensures it will be picked up for processing
// This handles creating a properly formatted job that the daemon will recognize and process
func recreateJob(db *storage.DB, originalJobID string) (*jobs.Job, error) {
	// Use the shared job recreation logic
	newJob, err := jobops.RecreateUploadJob(db, originalJobID)
	if err != nil {
		return nil, err
	}

	// Add client-specific metadata
	newJob.AddMetadata("manually_recreated_at", time.Now().Format(time.RFC3339))
	newJob.AddMetadata("recreated_by", "client")

	// Save the updated job back to the database
	if err := db.SaveJob(newJob); err != nil {
		return nil, fmt.Errorf("failed to update recreated job: %w", err)
	}

	// The daemon has a periodic job checker that will pick up this job
	// from the database and add it to its queue automatically

	return newJob, nil
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
	allJobs, err := db.GetAllJobs()
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

	rows := jobsToRows(allJobs)

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

	// Initialize the model with all fields
	m := model{
		table:        t,
		db:           db,
		state:        stateJobList,
		jobs:         allJobs,
		messageStyle: infoTextStyle,
	}

	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
		os.Exit(1)
	}
}
