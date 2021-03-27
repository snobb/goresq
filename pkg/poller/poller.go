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

type Poller struct {
	Namespace string
	interval  time.Duration
	concur    int
	pool      db.Pooler
}

func New(pool db.Pooler, interval time.Duration, concur int) *Poller {
	return &Poller{
		Namespace: "resque",
		interval:  interval,
		concur:    concur,
		pool:      pool,
	}
}

func (p *Poller) Start(queues []string, handlers map[string]*job.Handler) error {
	jobs, err := p.poll(queues, signal.Quit())
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

func (p *Poller) poll(queues []string, quit signal.QuitChannel) (<-chan *job.Job, error) {
	ticker := time.NewTicker(p.interval)
	jobs := make(chan *job.Job)

	go func() {
		defer close(jobs)

		for {
			select {
			case <-quit:
				ticker.Stop()
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

				timeout := time.After(p.interval)
				select {
				case <-quit:
					return
				case <-timeout:
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
