package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
)

// Poller represents a queue poller
type Poller struct {
	Namespace string
	interval  time.Duration
	concur    int
	db        db.Accessor
}

// New creates a new Poller
func New(db db.Accessor, interval time.Duration, concur int) *Poller {
	return &Poller{
		Namespace: "resque",
		interval:  interval,
		concur:    concur,
		db:        db,
	}
}

// Start polling the queue. The poller is aware of context cancel and timeout and will quite on
// these events.
func (p *Poller) Start(ctx context.Context, queues []string, handlers map[string]job.Handler, errors chan<- error) error {
	var wg sync.WaitGroup

	jobs := p.poll(ctx, queues, &wg, errors)

	for i := 0; i < p.concur; i++ {
		w := NewWorker(i, p.Namespace, queues, handlers, p.db)

		if err := w.Work(ctx, jobs, &wg, errors); err != nil {
			select {
			case <-ctx.Done():
				break
			default:
				i--
			}
		}
	}

	wg.Wait()

	return nil
}

func (p *Poller) poll(ctx context.Context, queues []string, wg *sync.WaitGroup, errors chan<- error) <-chan *job.Job {
	ticker := time.NewTicker(p.interval)
	jobs := make(chan *job.Job)

	wg.Add(1)

	go func() {
		defer func() {
			close(jobs)
			ticker.Stop()
			wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				if err := p.pollTick(ctx, queues, jobs); err != nil {
					errors <- err
				}
			}
		}
	}()

	return jobs
}

func (p *Poller) pollTick(ctx context.Context, queues []string, jobs chan<- *job.Job) error {
	job, err := p.getJob(ctx, queues)
	if err != nil {
		return err
	}

	jobs <- job

	return nil
}

func (p *Poller) getJob(ctx context.Context, queues []string) (*job.Job, error) {
	for _, queue := range queues {
		res, err := p.db.LPop(ctx, fmt.Sprintf("%s:queue:%s", p.Namespace, queue))
		if err != nil {
			return nil, err
		}

		if res == nil {
			continue // nothing in the queue
		}

		job := &job.Job{Queue: queue}

		decoder := json.NewDecoder(bytes.NewReader(res.([]byte)))

		if err := decoder.Decode(&job.Payload); err != nil {
			return nil, err
		}

		return job, nil
	}

	return nil, nil
}
