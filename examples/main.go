package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/poller"
	"github.com/snobb/goresq/pkg/queue"
)

type sumHandler struct {
	plugins []job.Plugin
}

func (s *sumHandler) Plugins() []job.Plugin {
	return s.plugins
}

func (s *sumHandler) Perform(ctx context.Context, queue string, class string, args []json.RawMessage) (job.Result, error) {
	var params struct {
		TaskData []int `json:"task_data"`
	}

	if err := json.Unmarshal(args[0], &params); err != nil {
		return nil, err
	}

	fmt.Printf("%s : %s : %#v\n", queue, class, params.TaskData)
	sum := sumArray(params.TaskData...)
	fmt.Printf("sum: %d\n", sum)
	return sum, nil
}

type delayPlugin struct{}

// BeforePerform is a function to run before handling a job.
func (d *delayPlugin) BeforePerform(ctx context.Context, queue string, class string, args []json.RawMessage) error {
	return nil
}

// AfterPerform is a function to run after handling a job
func (d *delayPlugin) AfterPerform(ctx context.Context, queue string, class string, args []json.RawMessage, result job.Result, err error) error {
	fmt.Printf("result = %v  Delaying...\n", result)
	time.Sleep(10 * time.Second)
	return nil
}

func sumArray(nums ...int) int {
	var result int

	for _, n := range nums {
		result += n
	}

	return result
}

type queueLogger struct{}

// BeforeEnqueue is a function to run before handling a job.
func (q *queueLogger) BeforeEnqueue(ctx context.Context, queue string, class string, args []interface{}) error {
	log.Printf(":: enqueueing message: queue:%s, class:%s, args:%#v", queue, class, args)
	return nil
}

// AfterEnqueue is a function to run after handling a job
func (q *queueLogger) AfterEnqueue(ctx context.Context, queue string, class string, args []interface{}) error {
	log.Printf(":: enqueued message: queue:%s, class:%s, args:%#v", queue, class, args)
	return nil
}

func main() {
	redis := db.NewPool(&db.Config{
		URI: "localhost:6379",
		DB:  4,
	})

	handlers := map[string]job.Handler{
		"sum": &sumHandler{
			plugins: []job.Plugin{&delayPlugin{}},
		},
	}

	p := poller.New(redis, time.Millisecond*100, 3)
	errs := make(chan error)
	go func() {
		for err := range errs {
			log.Printf("error: %s", err.Error())
		}
	}()

	go func() {
		q := queue.New(redis)
		q.RegisterPlugins(&queueLogger{})

		payload := map[string]interface{}{
			"class":     "sum",
			"task_data": []int{10, 20, 30},
		}

		q.Enqueue(context.Background(), "queue2.test", "sum", []interface{}{payload})
		q.Enqueue(context.Background(), "queue1.test", "sum", []interface{}{payload})
	}()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, os.Interrupt)
		<-sigs
		cancel()
	}()

	if err := p.Start(ctx, []string{"queue1.test", "queue2.test"}, handlers, errs); err != nil {
		panic(err)
	}
}
