package queue

import "context"

// Plugin defines a plugin interface
type Plugin interface {
	// BeforeEnqueue is a function to run before handling a job.
	BeforeEnqueue(ctx context.Context, queue, class string, args []interface{}) error

	// AfterEnqueue is a function to run after handling a job
	AfterEnqueue(ctx context.Context, queue, class string, args []interface{}) error
}
