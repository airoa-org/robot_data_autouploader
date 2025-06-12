package ui

import "github.com/charmbracelet/lipgloss"

var (
	BaseStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color("240"))

	TitleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("63")).
			Bold(true).
			Padding(0, 1)

	PromptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205"))

	InfoTextStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	ErrorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("197")).
			Bold(true)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("76")).
			Bold(true)
)