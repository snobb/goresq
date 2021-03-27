package job

import "encoding/json"

// Plugin defines methods to run before or after operation.
type Plugin interface {
	Before(queue, class string, args []json.RawMessage) error
	After(queue, class string, args []json.RawMessage, err error) error
}

// Handler represents a message handler.
type Handler struct {
	Plugins []Plugin
	Perform HandlerFunc
}

// Handler is the handler function for specific job class
type HandlerFunc func(queue, class string, args []json.RawMessage) error
