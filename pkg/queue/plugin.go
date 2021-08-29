package queue

// Plugin defines a plugin interface
type Plugin interface {
	// BeforeEnqueue is a function to run before handling a job.
	BeforeEnqueue(queue, class string, args []interface{}) error

	// AfterEnqueue is a function to run after handling a job
	AfterEnqueue(queue, class string, args []interface{}) error
}
