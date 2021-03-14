package job

import "encoding/json"

type PerformPlugin interface {
	BeforePerform(queue, class string, args []json.RawMessage) error
	AfterPerform(queue, class string, args []json.RawMessage, err error) error
}

type Handler struct {
	Plugins []PerformPlugin
	Perform HandlerFunc
}

// Handler is the handler function for specific job class
type HandlerFunc func(queue, class string, args []json.RawMessage) error
