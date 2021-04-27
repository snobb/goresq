package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/poller"
)

type sumHandler struct {
	plugins []job.Plugin
}

func (s *sumHandler) Plugins() []job.Plugin {
	return s.plugins
}

func (s *sumHandler) Perform(queue string, class string, args []json.RawMessage) (job.Result, error) {
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
func (d *delayPlugin) BeforePerform(queue string, class string, args []json.RawMessage) error {
	return nil
}

// AfterPerform is a function to run after handling a job
func (d *delayPlugin) AfterPerform(queue string, class string, args []json.RawMessage, result job.Result, err error) error {
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

	p := poller.New(redis, time.Second*2, 1)
	if err := p.Start(context.Background(), []string{"queue1.test", "queue2.test"}, handlers); err != nil {
		panic(err)
	}
}
