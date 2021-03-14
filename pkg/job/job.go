package job

import "encoding/json"

// Payload represents a job payload
type Payload struct {
	Class string            `json:"class"`
	Args  []json.RawMessage `json:"args"`
}

// Job represents a resque job
type Job struct {
	Queue   string
	Payload Payload
}
