package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/snobb/goresq/pkg/db"
)

// Queue is the job enqueuer.
type Queue struct {
	Namespace string
	pool      db.Pooler
}

// New creates a new instance of Queue
func New(pool db.Pooler) *Queue {
	return &Queue{
		Namespace: "resque",
		pool:      pool,
	}
}

// Enqueue enqueues a job into the queue.
func (q *Queue) Enqueue(ctx context.Context, class, queue string, data []interface{}) error {
	conn, err := q.pool.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	payload := struct {
		Class string        `json:"class"`
		Args  []interface{} `json:"args"`
	}{class, data}

	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	if err = conn.Send("RPUSH", fmt.Sprintf("%s:queue:%s", q.Namespace, queue), buf); err != nil {
		return err
	}

	if err = conn.Send("SADD", fmt.Sprintf("%s:queues", q.Namespace), queue); err != nil {
		return err
	}

	return nil
}
