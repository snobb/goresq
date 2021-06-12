package poller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/db/mock"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/poller"

	"github.com/snobb/goresq/test/assert"
	"github.com/snobb/goresq/test/helpers"
)

func TestWorker_Work(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Errorf("could get the hostname: %s", err.Error())
	}
	workerID := fmt.Sprintf("%s:%d-worker1", hostname, os.Getpid())

	tests := []struct {
		name         string
		job          job.Job
		perform      job.PerformFunc
		wantCommands []string
		wantRedisOut interface{}
		wantErr      bool
		wantDbErr    bool
	}{
		{
			name: "test it works",
			job: job.Job{
				Queue: "queue1",
				Payload: job.Payload{
					Class: "test",
					Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
				},
			},
			perform: func(queue, class string, args []json.RawMessage) (job.Result, error) {
				assert.Eq(t, "queue1", queue)
				assert.Eq(t, "test", class)
				return "foobar", nil
			},
			wantCommands: []string{
				"SADD resque:workers",
				fmt.Sprintf("SET resque:stat:processed:%s:queue1,queue2", workerID),
				fmt.Sprintf("SET resque:stat:failed:%s:queue1,queue2", workerID),
				fmt.Sprintf("SET resque:worker:%s:queue1,queue2:started", workerID),
				"Conn::Close",
				"INCR resque:stat:processed",
				fmt.Sprintf("INCR resque:stat:processed:%s:queue1,queue2", workerID),
				"Conn::Flush",
				"Conn::Close",
				"SREM resque:workers",
				fmt.Sprintf("DEL resque:stat:processed:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:stat:failed:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:worker:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:worker:%s:queue1,queue2:started", workerID),
				"Conn::Flush",
				"Conn::Close",
			},
			wantErr:   false,
			wantDbErr: false,
		},
		{
			name: "fails if handler returns an error",
			job: job.Job{
				Queue: "queue1",
				Payload: job.Payload{
					Class: "test",
					Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
				},
			},
			perform: func(queue, class string, args []json.RawMessage) (job.Result, error) {
				assert.Eq(t, "queue1", queue)
				assert.Eq(t, "test", class)
				return nil, fmt.Errorf("spanner")
			},
			wantCommands: []string{
				"SADD resque:workers",
				fmt.Sprintf("SET resque:stat:processed:%s:queue1,queue2", workerID),
				fmt.Sprintf("SET resque:stat:failed:%s:queue1,queue2", workerID),
				fmt.Sprintf("SET resque:worker:%s:queue1,queue2:started", workerID),
				"Conn::Close",
				"RPUSH resque:failed",
				"INCR resque:stat:failed",
				fmt.Sprintf("INCR resque:stat:failed:%s:queue1,queue2", workerID),
				"Conn::Flush",
				"Conn::Close",
				"SREM resque:workers",
				fmt.Sprintf("DEL resque:stat:processed:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:stat:failed:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:worker:%s:queue1,queue2", workerID),
				fmt.Sprintf("DEL resque:worker:%s:queue1,queue2:started", workerID),
				"Conn::Flush",
				"Conn::Close",
			},
			wantErr:   false,
			wantDbErr: false,
		},
		{
			name: "fails if failed to get redis connection",
			job: job.Job{
				Queue: "queue1",
				Payload: job.Payload{
					Class: "test",
					Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
				},
			},
			wantErr:   true,
			wantDbErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			jobs := make(chan *job.Job, 1)
			defer close(jobs)

			var redisCmds []string
			mockedConn := &mock.ConnMock{
				CloseFunc: func() error {
					redisCmds = append(redisCmds, "Conn::Close")
					return nil
				},
				DoFunc: func(commandName string, args ...interface{}) (interface{}, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("%s %s", commandName, args[0]))
					return tt.wantRedisOut, nil
				},
				ErrFunc: func() error {
					panic("mock out the Err method")
				},
				FlushFunc: func() error {
					redisCmds = append(redisCmds, "Conn::Flush")
					return nil
				},
				ReceiveFunc: func() (interface{}, error) {
					panic("mock out the Receive method")
				},
				SendFunc: func(commandName string, args ...interface{}) error {
					redisCmds = append(redisCmds, fmt.Sprintf("%s %s", commandName, args[0]))
					return nil
				},
			}

			mockedPool := &mock.PoolerMock{
				ConnFunc: func() (db.Conn, error) {
					if tt.wantDbErr {
						return nil, fmt.Errorf("db spanner")
					}
					return mockedConn, nil
				},
			}

			handlers := map[string]job.Handler{"test": tt.perform}

			w := poller.NewWorker(1, "resque", []string{"queue1", "queue2"}, handlers, mockedPool)
			jobs <- &tt.job

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			go func() {
				if err := w.Work(ctx, jobs, &wg, nil); (err != nil) != tt.wantErr {
					t.Errorf("Worker.Work() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()

			for i, cmd := range redisCmds {
				assert.Eq(t, tt.wantCommands[i], cmd)
			}
		})
	}
}
