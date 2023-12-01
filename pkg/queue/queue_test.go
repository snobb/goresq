package queue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/snobb/goresq/pkg/db/mock"
	"github.com/snobb/goresq/pkg/queue"
	"github.com/snobb/goresq/test/assert"
)

type plugin struct {
	beforeCount, afterCount int
	beforeFunc              func(ctx context.Context, queue, class string, args []interface{}) error
	afterFunc               func(ctx context.Context, queue, class string, args []interface{}) error
}

// BeforeEnqueue is a function to run before handling a job.
func (p *plugin) BeforeEnqueue(ctx context.Context, queue string, class string, args []interface{}) error {
	p.beforeCount++
	return p.beforeFunc(ctx, queue, class, args)
}

// AfterEnqueue is a function to run after handling a job
func (p *plugin) AfterEnqueue(ctx context.Context, queue string, class string, args []interface{}) error {
	p.afterCount++
	return p.afterFunc(ctx, queue, class, args)
}

func TestQueue_Enqueue(t *testing.T) {
	class := "foobar"
	queueName := "queue1"

	tests := []struct {
		name            string
		data            []interface{}
		plugins         []*plugin
		wantRedisCmds   []string
		wantAfterCalls  int
		wantBeforeCalls int
		wantDbErr       bool
		wantSendErr     int
		wantErr         bool
	}{
		{
			name: "should enqueue a job successfully (no plugins)",
			data: []interface{}{"taskdata"},
			wantRedisCmds: []string{
				fmt.Sprintf(`RPUSH resque:queue:queue1 {"class":"%s","args":["taskdata"]}`, class),
				"SADD resque:queues queue1",
			},
			wantAfterCalls:  1,
			wantBeforeCalls: 1,
		},
		{
			name: "should enqueue a job successfully and run plugins",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
				},
			},
			wantRedisCmds: []string{
				fmt.Sprintf(`RPUSH resque:queue:queue1 {"class":"%s","args":["taskdata"]}`, class),
				"SADD resque:queues queue1",
			},
			wantAfterCalls:  1,
			wantBeforeCalls: 1,
		},
		{
			name: "should enqueue a job but fail to update redis",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
				},
			},
			wantDbErr:       true,
			wantAfterCalls:  0,
			wantBeforeCalls: 0,
			wantErr:         true,
		},
		{
			name: "should enqueue a job but fail to send first redis call",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(ctx context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(ctx context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
				},
			},
			wantAfterCalls:  0,
			wantBeforeCalls: 1,
			wantSendErr:     1,
			wantErr:         true,
		},
		{
			name: "should enqueue a job but fail to send second redis call",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
				},
			},
			wantAfterCalls:  0,
			wantBeforeCalls: 1,
			wantSendErr:     2,
			wantErr:         true,
		},
		{
			name: "should enqueue a job but fail to run before plugin",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return fmt.Errorf("spanner")
					},
					afterFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
				},
			},
			wantAfterCalls:  0,
			wantBeforeCalls: 1,
			wantErr:         true,
		},
		{
			name: "should enqueue a job but fail to run after plugin",
			data: []interface{}{"taskdata"},
			plugins: []*plugin{
				{
					beforeFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(_ context.Context, q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return fmt.Errorf("spanner")
					},
				},
			},
			wantAfterCalls:  1,
			wantBeforeCalls: 1,
			wantErr:         true,
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

			q := queue.New(mockedAccessor)
			var plugins []queue.Plugin
			for _, p := range tt.plugins {
				plugins = append(plugins, p)
			}

			q.RegisterPlugins(plugins...)

			ctx := context.Background()
			if err := q.Enqueue(ctx, queueName, class, tt.data); (err != nil) != tt.wantErr {
				t.Errorf("Queue.Enqueue() error = %v", err)
			}

			for _, p := range tt.plugins {
				assert.Eq(t, tt.wantBeforeCalls, p.beforeCount)
				assert.Eq(t, tt.wantAfterCalls, p.afterCount)
			}

			for i, cmd := range tt.wantRedisCmds {
				assert.Eq(t, cmd, redisCmds[i])
			}
		})
	}
}
