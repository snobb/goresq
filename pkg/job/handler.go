package job

import "encoding/json"

// Plugin defines a plugin interface
type Plugin interface {
	// BeforePerform is a function to run before handling a job.
	BeforePerform(queue, class string, args []json.RawMessage) error

	// AfterPerform is a function to run after handling a job
	AfterPerform(queue, class string, args []json.RawMessage, err error) error
}

// Handler represents a job Handler.
type Handler interface {
	// Plugins returns a list of registered plugins with the handler.
	Plugins() []Plugin

	// Perform is a function that handles the job
	Perform(queue, class string, args []json.RawMessage) error
}

// PerformFunc represents a function that performs the job
type PerformFunc func(queue, class string, args []json.RawMessage) error
