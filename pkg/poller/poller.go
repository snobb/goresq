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
	pool      db.Pooler
}

// New creates a new Poller
func New(pool db.Pooler, interval time.Duration, concur int) *Poller {
	return &Poller{
		Namespace: "resque",
		interval:  interval,
		concur:    concur,
		pool:      pool,
	}
}

// Start polling the queue. The poller is aware of context cancel and timeout and will quite on
// these events.
func (p *Poller) Start(ctx context.Context, queues []string, handlers map[string]job.Handler, errors chan<- error) error {
	var wg sync.WaitGroup

	jobs := p.poll(ctx, queues, &wg, errors)

	for i := 0; i < p.concur; i++ {
		w := NewWorker(i, p.Namespace, queues, handlers, p.pool)

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
	tick := time.Tick(p.interval)
	jobs := make(chan *job.Job)

	wg.Add(1)

	go func() {
		defer func() {
			close(jobs)
			wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case <-tick:
				if err := p.pollTick(queues, jobs); err != nil {
					errors <- err
				}
			}
		}
	}()

	return jobs
}

func (p *Poller) pollTick(queues []string, jobs chan<- *job.Job) error {
	conn, err := p.pool.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	job, err := p.getJob(conn, queues)
	if err != nil {
		return err
	}

	jobs <- job

	return nil
}

func (p *Poller) getJob(conn db.Conn, queues []string) (*job.Job, error) {
	for _, queue := range queues {
		res, err := conn.Do("LPOP", fmt.Sprintf("%s:queue:%s", p.Namespace, queue))
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
