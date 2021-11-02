package queue_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/db/mock"
	"github.com/snobb/goresq/pkg/queue"
	"github.com/snobb/goresq/test/assert"
)

type plugin struct {
	beforeCount, afterCount int
	beforeFunc              func(queue, class string, args []interface{}) error
	afterFunc               func(queue, class string, args []interface{}) error
}

// BeforeEnqueue is a function to run before handling a job.
func (p *plugin) BeforeEnqueue(queue string, class string, args []interface{}) error {
	p.beforeCount++
	return p.beforeFunc(queue, class, args)
}

// AfterEnqueue is a function to run after handling a job
func (p *plugin) AfterEnqueue(queue string, class string, args []interface{}) error {
	p.afterCount++
	return p.afterFunc(queue, class, args)
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(q, c string, as []interface{}) error {
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(q, c string, as []interface{}) error {
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(q, c string, as []interface{}) error {
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(q, c string, as []interface{}) error {
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return fmt.Errorf("spanner")
					},
					afterFunc: func(q, c string, as []interface{}) error {
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
					beforeFunc: func(q, c string, as []interface{}) error {
						assert.Eq(t, class, c)
						assert.Eq(t, queueName, q)
						assert.Eq(t, "taskdata", as[0].(string))
						return nil
					},
					afterFunc: func(q, c string, as []interface{}) error {
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

			mockedConn := &mock.ConnMock{
				CloseFunc: func() error {
					redisCmds = append(redisCmds, "Conn::Close")
					return nil
				},
				DoFunc: func(commandName string, args ...interface{}) (interface{}, error) {
					panic("mock out the Do method")
				},
				ErrFunc: func() error {
					panic("mock out the Err method")
				},
				FlushFunc: func() error {
					panic("mock out the Flush method")
				},
				ReceiveFunc: func() (interface{}, error) {
					panic("mock out the Receive method")
				},
				SendFunc: func(commandName string, args ...interface{}) error {
					if tt.wantSendErr == 1 {
						return fmt.Errorf("spanner")
					}
					tt.wantSendErr--
					redisCmds = append(redisCmds, fmt.Sprintf("%s %s %s", commandName, args[0], args[1]))
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

			q := queue.New(mockedPool)
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
