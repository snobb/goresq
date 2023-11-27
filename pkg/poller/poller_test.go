package poller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/db/mock"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/poller"
	"github.com/snobb/goresq/test/helpers"
)

func TestPoller_Start(t *testing.T) {
	tests := []struct {
		name        string
		interval    time.Duration
		concur      int
		perform     job.PerformFunc
		job         interface{}
		wantDbErr   bool
		wantChanErr bool
		wantErr     bool
	}{
		{
			name:     "should poll and get a job",
			interval: 20 * time.Millisecond,
			perform: func(ctx context.Context, queue, class string, args []json.RawMessage) (job.Result, error) {
				return "ok", nil
			},
			concur: 2,
			job: job.Payload{
				Class: "foo",
				Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
			},
		},
		{
			name:     "should poll and get a job but fail in performer",
			interval: 20 * time.Millisecond,
			perform: func(ctx context.Context, queue, class string, args []json.RawMessage) (job.Result, error) {
				return nil, fmt.Errorf("spanner")
			},
			concur: 1,
			job: job.Payload{
				Class: "foo",
				Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
			},
			wantChanErr: true,
		},
		{
			name:     "should poll and get a job but fail in redis calls",
			interval: 20 * time.Millisecond,
			perform: func(ctx context.Context, queue, class string, args []json.RawMessage) (job.Result, error) {
				return nil, fmt.Errorf("spanner")
			},
			concur: 1,
			job: job.Payload{
				Class: "foo",
				Args:  []json.RawMessage{json.RawMessage(helpers.Marshal(map[string]string{"foo": "bar"}))},
			},
			wantDbErr:   true,
			wantChanErr: true,
		},
		{
			name:     "should poll and get a job but fail to decode job payload",
			interval: 20 * time.Millisecond,
			perform: func(ctx context.Context, queue, class string, args []json.RawMessage) (job.Result, error) {
				t.Errorf("must not happen")
				return nil, nil
			},
			concur:      1,
			job:         "spanner",
			wantChanErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var redisCmds []string
			mockedConn := &mock.ConnMock{
				CloseFunc: func() error {
					redisCmds = append(redisCmds, "Conn::Close")
					return nil
				},
				DoFunc: func(commandName string, args ...interface{}) (interface{}, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("%s %s", commandName, args[0]))
					return helpers.Marshal(tt.job), nil
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

			p := poller.New(mockedPool, tt.interval, tt.concur)

			queues := []string{"queue1", "queue1"}
			errors := make(chan error)
			defer close(errors)

			handlers := map[string]job.Handler{"foo": tt.perform}

			ctx, cancel := context.WithTimeout(context.Background(), tt.interval+(5*time.Millisecond))
			defer cancel()

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case e := <-errors:
						if e != nil && !tt.wantChanErr {
							t.Errorf("Poller.Start() unexpected channel error: %#v", e)
						}
					}
				}
			}()

			if err := p.Start(ctx, queues, handlers, errors); (err != nil) != tt.wantErr {
				t.Errorf("Poller.Start() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
