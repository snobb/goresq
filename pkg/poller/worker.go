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
	db       db.Accessor
	handlers map[string]job.Handler
}

const connCoolDown = 1 * time.Second

// NewWorker creates a new worker.
func NewWorker(id int, namespace string, queues []string, handlers map[string]job.Handler, acc db.Accessor) *Worker {
	return &Worker{
		Track:    newTrack(fmt.Sprintf("worker%d", id), namespace, queues),
		runAt:    time.Now(),
		db:       acc,
		handlers: handlers,
	}
}

// Work is a method that starts job worker and processes jobs.
func (w *Worker) Work(ctx context.Context, jobs <-chan *job.Job, wg *sync.WaitGroup, errors chan<- error) error {
	if err := w.track(ctx); err != nil {
		errors <- err
		return err
	}

	wg.Add(1)

	go func() {
		defer func() {
			if err := w.untrack(ctx); err != nil {
				errors <- err
			}

			wg.Done()
		}()

		for jb := range jobs {
			if jb == nil {
				continue
			}

			if err := w.handleJob(ctx, jb); err != nil {
				errors <- err
			}
		}
	}()

	return nil
}

func (w *Worker) handleJob(ctx context.Context, jb *job.Job) (err error) {
	if err = w.run(ctx, jb); err != nil {
		if err = w.fail(ctx, jb, err); err != nil {
			return err
		}
	} else {
		if err = w.success(ctx, jb); err != nil {
			return err
		}
	}

	return err
}

func (w *Worker) run(ctx context.Context, jb *job.Job) (err error) {
	var result job.Result
	handler, ok := w.handlers[jb.Payload.Class]
	if !ok {
		return fmt.Errorf("could not find a handler for job class %s", jb.Payload.Class)
	}

	for _, plugin := range handler.Plugins() {
		if err = plugin.BeforePerform(ctx, jb.Queue, jb.Payload.Class, jb.Payload.Args); err != nil {
			return
		}
	}

	defer func() {
		for _, plugin := range handler.Plugins() {
			if err = plugin.AfterPerform(ctx, jb.Queue, jb.Payload.Class, jb.Payload.Args, result, err); err != nil {
				return
			}
		}
	}()

	result, err = handler.Perform(ctx, jb.Queue, jb.Payload.Class, jb.Payload.Args)
	return
}

func (w *Worker) untrack(ctx context.Context) error {
	return w.Track.untrack(ctx, w.db)
}

func (w *Worker) track(ctx context.Context) error {
	return w.Track.track(ctx, w.db)
}

func (w *Worker) success(ctx context.Context, _ *job.Job) error {
	return w.Track.success(ctx, w.db)
}

func (w *Worker) fail(ctx context.Context, job *job.Job, err error) error {
	resqueError := struct {
		FailedAt  time.Time
		Payload   json.RawMessage
		Exception string
		Error     string
		Worker    string
		Queue     string
	}{
		FailedAt:  time.Now(),
		Payload:   job.Payload.Args[0],
		Exception: "Error",
		Error:     err.Error(),
		Worker:    w.String(),
		Queue:     job.Queue,
	}

	buf, err := json.Marshal(resqueError)
	if err != nil {
		return fmt.Errorf("marshal failed during %w for job %v", err, job)
	}

	if _, err := w.db.RPush(ctx, fmt.Sprintf("%s:failed", w.Namespace), buf); err != nil {
		return err
	}

	return w.Track.fail(ctx, w.db)
}
