package listener

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

var handlers = make(map[string]*job.Handler)

type Listener struct {
	Namespace string
	interval  time.Duration
	pool      db.Pooler
}

func New(pool db.Pooler, interval time.Duration) *Listener {
	handlers = make(map[string]*job.Handler)

	return &Listener{
		Namespace: "resque:",
		interval:  interval,
		pool:      pool,
	}
}

func Register(class string, handler *job.Handler) {
	handlers[class] = handler
}

func (l *Listener) Pool() db.Pooler {
	return l.pool
}

func (l *Listener) Start(queues []string, concur int) error {
	jobCh, err := l.poll(queues, signal.Quit())
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	for i := 0; i < concur; i++ {
		w := NewWorker(i, l.Namespace, queues, l.pool)
		if err := w.Work(jobCh, &wg); err != nil {
			return err
		}
	}

	wg.Wait()

	return nil
}

func (l *Listener) poll(queues []string, quit signal.QuitChannel) (<-chan *job.Job, error) {
	ticker := time.NewTicker(l.interval)
	jch := make(chan *job.Job)

	go func() {
		defer close(jch)

		for {
			select {
			case <-quit:
				ticker.Stop()
				return

			default:
				conn, err := l.pool.Conn()
				if err != nil {
					return
				}
				defer conn.Close()

				job, err := l.getJob(conn, queues)
				if err != nil {
					return
				}

				jch <- job

				timeout := time.After(l.interval)
				select {
				case <-quit:
					return
				case <-timeout:
				}
			}
		}
	}()

	return jch, nil
}

func (l *Listener) getJob(conn db.Conn, queues []string) (*job.Job, error) {
	for _, queue := range queues {
		res, err := conn.Do("LPOP", fmt.Sprintf("%squeue:%s", l.Namespace, queue))
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
