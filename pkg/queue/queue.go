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
	plugins   []Plugin
}

// New creates a new instance of Queue
func New(pool db.Pooler) *Queue {
	return &Queue{
		Namespace: "resque",
		pool:      pool,
	}
}

// RegisterPlugins adds a plugin to the queue instance.
func (q *Queue) RegisterPlugins(plugin ...Plugin) {
	q.plugins = append(q.plugins, plugin...)
}

// Enqueue enqueues a job into the queue.
func (q *Queue) Enqueue(ctx context.Context, queue, class string, data []interface{}) error {
	conn, err := q.pool.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, plugin := range q.plugins {
		if err := plugin.BeforeEnqueue(ctx, queue, class, data); err != nil {
			return err
		}
	}

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

	for _, plugin := range q.plugins {
		if err := plugin.AfterEnqueue(ctx, queue, class, data); err != nil {
			return err
		}
	}

	return nil
}
