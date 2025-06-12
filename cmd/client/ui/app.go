package ui

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/jobs"
	"github.com/airoa-org/robot_data_pipeline/autoloader/internal/storage"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
)

type Model struct {
	Table              table.Model
	Viewport           viewport.Model
	DB                 *storage.DB
	Width              int
	Height             int
	State              string
	SelectedJob        *jobs.Job
	ConfirmationPrompt string
	Message            string
	MessageStyle       lipgloss.Style
	MessageTimeout     *time.Time
	Jobs               []*jobs.Job
	ViewportReady      bool
	RecreateJobHandler func(*storage.DB, string) (*jobs.Job, error)
}

type TickMsg struct{}

func Tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return TickMsg{}
	})
}

type MessageTimeoutMsg struct{}

func MessageTimeout() tea.Cmd {
	return tea.Tick(3*time.Second, func(t time.Time) tea.Msg {
		return MessageTimeoutMsg{}
	})
}

type ConfirmRecreateMsg struct{}

func (m Model) Init() tea.Cmd {
	return Tick()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.Width = msg.Width
		m.Height = msg.Height

		// Handle viewport sizing for job detail view
		if m.State == StateJobDetail {
			headerHeight := lipgloss.Height(m.DetailHeaderView())
			footerHeight := lipgloss.Height(m.DetailFooterView())
			verticalMarginHeight := headerHeight + footerHeight

			if !m.ViewportReady {
				m.Viewport = viewport.New(msg.Width, msg.Height-verticalMarginHeight)
				m.Viewport.YPosition = headerHeight
				if m.SelectedJob != nil {
					m.Viewport.SetContent(m.FormatJobDetailsWrapped(m.SelectedJob, msg.Width))
				}
				m.ViewportReady = true
			} else {
				m.Viewport.Width = msg.Width
				m.Viewport.Height = msg.Height - verticalMarginHeight
				// Update content with new width for proper wrapping
				if m.SelectedJob != nil {
					m.Viewport.SetContent(m.FormatJobDetailsWrapped(m.SelectedJob, msg.Width))
				}
			}
		}

		// Update table columns
		columns := m.CreateJobListColumns(m.Width)
		m.Table.SetColumns(columns)
		// Set table height to fill terminal (minus border and help line)
		tableHeight := msg.Height - 3
		if tableHeight < 3 {
			tableHeight = 3
		}
		m.Table.SetHeight(tableHeight)
		return m, nil

	case TickMsg:
		if m.State == StateJobList {
			// Re-fetch jobs from DB
			jobs, err := m.DB.GetAllJobs()
			if err == nil {
				m.Jobs = jobs
				rows := m.JobsToRows(jobs)
				m.Table.SetRows(rows)
			}
			return m, Tick()
		} else if m.State == StateJobDetail && m.SelectedJob != nil {
			// Re-fetch the selected job to update its data
			jobs, err := m.DB.GetAllJobs()
			if err == nil {
				for _, job := range jobs {
					if job.ID == m.SelectedJob.ID {
						m.SelectedJob = job
						// Update viewport content with fresh data
						if m.ViewportReady {
							m.Viewport.SetContent(m.FormatJobDetailsWrapped(job, m.Width))
						}
						break
					}
				}
			}
			return m, Tick()
		}
		return m, Tick()

	case MessageTimeoutMsg:
		if m.MessageTimeout != nil && time.Now().After(*m.MessageTimeout) {
			m.State = StateJobList
			m.Message = ""
			m.MessageTimeout = nil
			return m, Tick()
		}
		return m, nil

	case ConfirmRecreateMsg:
		// Handle job recreation using the handlers package
		newJob, err := m.RecreateJob(m.SelectedJob.ID)
		if err != nil {
			// Show error message
			m.State = StateMessage
			m.Message = fmt.Sprintf("Failed to recreate job: %v", err)
			m.MessageStyle = ErrorStyle
			timeout := time.Now().Add(5 * time.Second)
			m.MessageTimeout = &timeout
			return m, MessageTimeout()
		}

		// Show success message
		m.State = StateMessage
		m.Message = fmt.Sprintf("Successfully recreated job. New job ID: %s", newJob.ID)
		m.MessageStyle = SuccessStyle
		timeout := time.Now().Add(3 * time.Second)
		m.MessageTimeout = &timeout
		return m, MessageTimeout()

	case tea.KeyMsg:
		switch m.State {
		case StateJobList:
			// First let our key handler process the message
			newModel, newCmd := m.HandleJobListKeys(msg)
			if newCmd != nil {
				// If our handler returned a command, use it
				return newModel, newCmd
			}
			// Otherwise, let the table handle the key (for arrow keys, etc.)
			m = newModel.(Model)
			m.Table, cmd = m.Table.Update(msg)
			return m, cmd
		case StateJobDetail:
			// First let our key handler process the message
			newModel, newCmd := m.HandleJobDetailKeys(msg)
			if newCmd != nil {
				return newModel, newCmd
			}
			// Otherwise, let the viewport handle the key (for scrolling, etc.)
			m = newModel.(Model)
			if m.ViewportReady {
				m.Viewport, cmd = m.Viewport.Update(msg)
			}
			return m, cmd
		case StateConfirmation:
			return m.HandleConfirmationKeys(msg)
		case StateMessage:
			return m.HandleMessageKeys(msg)
		}
	}

	return m, cmd
}

func (m Model) View() string {
	switch m.State {
	case StateConfirmation:
		return BaseStyle.Render(
			TitleStyle.Render("RECREATE JOB") + "\n\n" +
				PromptStyle.Render(m.ConfirmationPrompt),
		)

	case StateMessage:
		return BaseStyle.Render(
			m.MessageStyle.Render(m.Message) + "\n\n" +
				InfoTextStyle.Render("Press any key to return"),
		)

	case StateJobDetail:
		if !m.ViewportReady {
			return "\n  Loading job details..."
		}
		return fmt.Sprintf("%s\n%s\n%s", m.DetailHeaderView(), m.Viewport.View(), m.DetailFooterView())

	default: // StateJobList
		helpText := "Press q to quit, enter to view details"
		if len(m.Table.SelectedRow()) > 0 && m.Table.SelectedRow()[2] == string(jobs.JobTypeUpload) {
			helpText += ", r to recreate selected upload job"
		}
		return BaseStyle.Render(m.Table.View()) + "\n" + helpText
	}
}

// Helper methods for the model
func (m Model) HandleJobListKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		if m.Table.Focused() {
			m.Table.Blur()
		} else {
			m.Table.Focus()
		}
		return m, tea.Sequence() // Return empty command to indicate we handled it

	case "q", "ctrl+c":
		return m, tea.Quit

	case "enter":
		// Show job details
		if len(m.Table.SelectedRow()) > 0 {
			jobID := m.Table.SelectedRow()[0]
			// Load complete job data
			for _, job := range m.Jobs {
				if job.ID == jobID {
					m.SelectedJob = job
					break
				}
			}

			if m.SelectedJob != nil {
				// Switch to job detail state
				m.State = StateJobDetail
				m.ViewportReady = false
				// Trigger window size message to initialize viewport
				return m, func() tea.Msg {
					return tea.WindowSizeMsg{Width: m.Width, Height: m.Height}
				}
			}
		}
		return m, tea.Sequence() // Return empty command to indicate we handled it

	case "r":
		// Only allow recreation of upload jobs
		if len(m.Table.SelectedRow()) > 0 && m.Table.SelectedRow()[2] == string(jobs.JobTypeUpload) {
			// Get the selected job ID
			jobID := m.Table.SelectedRow()[0]
			// Load complete job data
			for _, job := range m.Jobs {
				if job.ID == jobID {
					m.SelectedJob = job
					break
				}
			}

			if m.SelectedJob != nil {
				// Switch to confirmation state
				m.State = StateConfirmation
				m.ConfirmationPrompt = fmt.Sprintf(
					"Are you sure you want to recreate this upload job?\n\n"+
						"Job ID: %s\nSource: %s\nDestination: %s\n\n"+
						"Please verify that the source directory still contains the correct files.\n\n"+
						"Press y to confirm, n to cancel",
					m.SelectedJob.ID, m.SelectedJob.Source, m.SelectedJob.Destination)
			}
		}
		return m, tea.Sequence() // Return empty command to indicate we handled it
	}

	// Return nil command for unhandled keys so table can process them (arrow keys, etc.)
	return m, nil
}

func (m Model) HandleJobDetailKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "q":
		// Return to job list
		m.State = StateJobList
		m.SelectedJob = nil
		m.ViewportReady = false
		return m, tea.Sequence() // Return empty command to indicate we handled it

	case "ctrl+c":
		return m, tea.Quit
	}

	// Return nil command for unhandled keys so viewport can process them (scrolling, etc.)
	return m, nil
}

func (m Model) HandleConfirmationKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "y", "Y":
		// User confirmed, trigger recreation
		return m, func() tea.Msg {
			return ConfirmRecreateMsg{}
		}

	case "n", "N", "esc":
		// Cancel and return to job list
		m.State = StateJobList
		m.SelectedJob = nil
		return m, nil

	case "q", "ctrl+c":
		return m, tea.Quit
	}

	return m, nil
}

func (m Model) HandleMessageKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "enter", " ":
		// Return to job list
		m.State = StateJobList
		m.Message = ""
		m.MessageTimeout = nil
		return m, nil

	case "q", "ctrl+c":
		return m, tea.Quit
	}

	return m, nil
}

// DetailHeaderView creates the header for the job detail viewport
func (m Model) DetailHeaderView() string {
	title := TitleStyle.Render("Job Details")
	line := strings.Repeat("─", max(0, m.Viewport.Width-lipgloss.Width(title)))
	return lipgloss.JoinHorizontal(lipgloss.Center, title, line)
}

// DetailFooterView creates the footer for the job detail viewport
func (m Model) DetailFooterView() string {
	info := InfoTextStyle.Render(fmt.Sprintf("%3.f%% • Press esc to return", m.Viewport.ScrollPercent()*100))
	line := strings.Repeat("─", max(0, m.Viewport.Width-lipgloss.Width(info)))
	return lipgloss.JoinHorizontal(lipgloss.Center, line, info)
}

// FormatJobDetailsWrapped formats all job information for display in the viewport with line wrapping
func (m Model) FormatJobDetailsWrapped(job *jobs.Job, width int) string {
	var details strings.Builder

	// Calculate available width for content (accounting for padding and borders)
	contentWidth := width - 4 // Leave some margin
	if contentWidth < 20 {
		contentWidth = 20 // Minimum width
	}

	details.WriteString(m.wrapText(fmt.Sprintf("Job ID: %s", job.ID), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Created: %s (%s)", humanize.Time(job.CreatedAt), job.CreatedAt.Format(time.RFC3339)), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Updated: %s (%s)", humanize.Time(job.UpdatedAt), job.UpdatedAt.Format(time.RFC3339)), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Type: %s", job.Type), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Status: %s", job.Status), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Progress: %.1f%%", job.Progress*100), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Source: %s", job.Source), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Destination: %s", job.Destination), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("File Count: %d", job.FileCount), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Total Size: %s (%d bytes)", humanize.Bytes(uint64(job.TotalSize)), job.TotalSize), contentWidth) + "\n")
	details.WriteString(m.wrapText(fmt.Sprintf("Processed Size: %s (%d bytes)", humanize.Bytes(uint64(job.ProcessedSize)), job.ProcessedSize), contentWidth) + "\n")

	if job.ErrorMsg.Valid && job.ErrorMsg.String != "" {
		details.WriteString("\n")
		details.WriteString(m.wrapText(fmt.Sprintf("Error: %s", job.ErrorMsg.String), contentWidth) + "\n")
	}

	if len(job.Metadata) > 0 {
		details.WriteString("\nMetadata:\n")
		// Sort metadata keys alphabetically
		keys := make([]string, 0, len(job.Metadata))
		for key := range job.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			details.WriteString(m.wrapText(fmt.Sprintf("  %s: %s", key, job.Metadata[key]), contentWidth) + "\n")
		}
	}

	// Add some spacing at the end
	details.WriteString("\n\n")

	return details.String()
}

// wrapText wraps text to fit within the specified width
func (m Model) wrapText(text string, width int) string {
	if len(text) <= width {
		return text
	}

	var result strings.Builder
	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	currentLine := words[0]

	for _, word := range words[1:] {
		if len(currentLine)+1+len(word) <= width {
			currentLine += " " + word
		} else {
			result.WriteString(currentLine + "\n")
			currentLine = word
		}
	}

	result.WriteString(currentLine)
	return result.String()
}

func (m Model) JobsToRows(jobs []*jobs.Job) []table.Row {
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

func (m Model) CreateJobListColumns(width int) []table.Column {
	// Fixed columns: ID(10), Type(8), Status(10), Progress(8), plus 6 for table borders/padding
	fixed := 10 + 8 + 10 + 8 + 6
	avail := width - fixed
	if avail < 30 {
		avail = 30 // minimum reasonable width
	}
	createdW := 19
	srcDstW := int(math.Max(float64((avail-createdW)/2), 10))
	
	return []table.Column{
		{Title: "ID", Width: 10},
		{Title: "Created", Width: createdW},
		{Title: "Type", Width: 8},
		{Title: "Status", Width: 10},
		{Title: "Progress", Width: 8},
		{Title: "Source", Width: srcDstW},
		{Title: "Destination", Width: srcDstW},
	}
}

func (m Model) RecreateJob(originalJobID string) (*jobs.Job, error) {
	// Use external handler function
	return m.RecreateJobHandler(m.DB, originalJobID)
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}