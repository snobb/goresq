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
	db        db.Accessor
	plugins   []Plugin
}

// New creates a new instance of Queue
func New(db db.Accessor) *Queue {
	return &Queue{
		Namespace: "resque",
		db:        db,
	}
}

// RegisterPlugins adds a plugin to the queue instance.
func (q *Queue) RegisterPlugins(plugin ...Plugin) {
	q.plugins = append(q.plugins, plugin...)
}

// Enqueue enqueues a job into the queue.
func (q *Queue) Enqueue(ctx context.Context, queue, class string, data []interface{}) error {
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

	if _, err = q.db.RPush(ctx, fmt.Sprintf("%s:queue:%s", q.Namespace, queue), buf); err != nil {
		return err
	}

	if _, err = q.db.SAdd(ctx, fmt.Sprintf("%s:queues", q.Namespace), queue); err != nil {
		return err
	}

	for _, plugin := range q.plugins {
		if err := plugin.AfterEnqueue(ctx, queue, class, data); err != nil {
			return err
		}
	}

	return nil
}
