package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
)

// Worker represents a queue worker.
type Worker struct {
	Track
	runAt    time.Time
	pool     db.Pooler
	handlers map[string]job.Handler
}

// NewWorker creates a new worker.
func NewWorker(id int, namespace string, queues []string, handlers map[string]job.Handler, pool db.Pooler) *Worker {
	return &Worker{
		Track:    newTrack(fmt.Sprintf("worker%d", id), namespace, queues),
		runAt:    time.Now(),
		pool:     pool,
		handlers: handlers,
	}
}

// Work is a method that starts job worker and processes jobs.
func (w *Worker) Work(ctx context.Context, jobs <-chan *job.Job, wg *sync.WaitGroup, errors chan<- error) error {
	if err := w.track(); err != nil {
		errors <- err
		return err
	}

	wg.Add(1)

	go func() {
		defer func() {
			wg.Done()
			w.untrack()
		}()

		for jb := range jobs {
			if jb == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return

			default:
				conn, err := w.pool.Conn()
				if err != nil {
					errors <- err
				}
				defer conn.Close()

				err = w.run(conn, jb)
				if err != nil {
					w.fail(conn, jb, err)
				} else {
					w.success(conn, jb)
				}
			}
		}
	}()

	return nil
}

func (w *Worker) run(conn db.Conn, jb *job.Job) (err error) {
	var result job.Result
	handler, ok := w.handlers[jb.Payload.Class]
	if !ok {
		return fmt.Errorf("Could not find a handler for job class %s", jb.Payload.Class)
	}

	for _, plugin := range handler.Plugins() {
		if err = plugin.BeforePerform(jb.Queue, jb.Payload.Class, jb.Payload.Args); err != nil {
			return
		}
	}

	defer func() {
		for _, plugin := range handler.Plugins() {
			if err = plugin.AfterPerform(jb.Queue, jb.Payload.Class, jb.Payload.Args, result, err); err != nil {
				return
			}
		}
	}()

	result, err = handler.Perform(jb.Queue, jb.Payload.Class, jb.Payload.Args)
	if err != nil {
		return
	}

	return
}

func (w *Worker) untrack() error {
	conn, err := w.pool.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	return w.Track.untrack(conn)
}

func (w *Worker) track() error {
	conn, err := w.pool.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	return w.Track.track(conn)
}

func (w *Worker) success(conn db.Conn, job *job.Job) error {
	return w.Track.success(conn)
}

func (w *Worker) fail(conn db.Conn, job *job.Job, err error) error {
	resqueError := struct {
		FailedAt  time.Time
		Payload   json.RawMessage
		Exception string
		Error     string
		Worker    string
		Queue     string
	}{
		time.Now(),
		job.Payload.Args[0],
		"Error",
		err.Error(),
		w.String(),
		job.Queue,
	}

	buf, err := json.Marshal(resqueError)
	if err != nil {
		return fmt.Errorf("Marshal failed during %w for job %v", err, job)
	}

	conn.Send("RPUSH", fmt.Sprintf("%s:failed", w.Namespace), buf)
	return w.Track.fail(conn)
}
