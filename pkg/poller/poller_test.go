package poller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

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
		wantCmd     []string
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
			mockedAccessor := &mock.AccessorMock{
				DecrFunc: func(ctx context.Context, key string) (int64, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("decr %s", key))
					return 0, nil
				},
				DelFunc: func(ctx context.Context, keys ...string) (int64, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("del %s", keys[0]))
					return 0, nil
				},
				DoFunc: func(ctx context.Context, args ...any) (any, error) {
					panic("mock out the Do method")
				},
				GetFunc: func(ctx context.Context, key string) (string, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("get %s", key))
					return "", nil
				},
				IncrFunc: func(ctx context.Context, key string) (int64, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("incr %s", key))
					return 0, nil
				},
				LPopFunc: func(ctx context.Context, key string) (any, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("lpop %s", key))
					return nil, nil
				},
				LPushFunc: func(ctx context.Context, key string, value any) (any, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("lpush %s %v", key, value))
					return nil, nil
				},
				RPopFunc: func(ctx context.Context, key string) (any, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("rpop %s", key))
					return nil, nil
				},
				RPushFunc: func(ctx context.Context, key string, value any) (any, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("rpush %s %v", key, value))
					return nil, nil
				},
				SAddFunc: func(ctx context.Context, key string, members ...any) (int64, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("sadd %s %v", key, members[0]))
					return 0, nil
				},
				SRemFunc: func(ctx context.Context, key string, members ...any) (int64, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("srem %s %v", key, members[0]))
					return 0, nil
				},
				SetFunc: func(ctx context.Context, key string, value any, exp time.Duration) (string, error) {
					redisCmds = append(redisCmds, fmt.Sprintf("set %s %v %d", key, value, exp))
					return "", nil
				},
			}

			p := poller.New(mockedAccessor, tt.interval, tt.concur)

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

			for _, cmd := range redisCmds {
				fmt.Println(cmd)
			}
		})
	}
}
