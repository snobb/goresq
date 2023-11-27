package job

import (
	"context"
	"encoding/json"
)

// Result is a result from the job hansler.
type Result interface{}

// Plugin defines a plugin interface
type Plugin interface {
	// BeforePerform is a function to run before handling a job.
	BeforePerform(ctx context.Context, queue, class string, args []json.RawMessage) error

	// AfterPerform is a function to run after handling a job
	AfterPerform(ctx context.Context, queue, class string, args []json.RawMessage, result Result, err error) error
}

// Handler represents a job Handler.
type Handler interface {
	// Plugins returns a list of registered plugins with the handler.
	Plugins() []Plugin

	// Perform is a function that handles the job
	Perform(ctx context.Context, queue, class string, args []json.RawMessage) (Result, error)
}

// PerformFunc represents a function that performs the job
type PerformFunc func(ctx context.Context, queue, class string, args []json.RawMessage) (Result, error)

// Plugins returns a list of registered plugins with the handler.
func (p PerformFunc) Plugins() []Plugin {
	return []Plugin{}
}

// Perform is a function that handles the job
func (p PerformFunc) Perform(ctx context.Context, queue string, class string, args []json.RawMessage) (Result, error) {
	return p(ctx, queue, class, args)
}
