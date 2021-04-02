package poller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/signal"
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

// StartQuit polling the queue and quit on signal from the quit channel.
func (p *Poller) StartQuit(queues []string, handlers map[string]job.Handler, quit signal.QuitChannel) error {
	jobs, err := p.poll(queues, quit)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < p.concur; i++ {
		w := NewWorker(i, p.Namespace, queues, handlers, p.pool)
		if err := w.Work(jobs, &wg); err != nil {
			return err
		}
	}

	wg.Wait()

	return nil
}

// Start polling the queue and quit on OS signals.
func (p *Poller) Start(queues []string, handlers map[string]job.Handler) error {
	return p.StartQuit(queues, handlers, signal.Quit())
}

func (p *Poller) poll(queues []string, quit signal.QuitChannel) (<-chan *job.Job, error) {
	tick := time.Tick(p.interval)
	jobs := make(chan *job.Job)

	go func() {
		defer close(jobs)

		for {
			select {
			case <-quit:
				return

			default:
				conn, err := p.pool.Conn()
				if err != nil {
					return
				}
				defer conn.Close()

				job, err := p.getJob(conn, queues)
				if err != nil {
					return
				}

				jobs <- job

				select {
				case <-quit:
					return
				case <-tick:
				}
			}
		}
	}()

	return jobs, nil
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
