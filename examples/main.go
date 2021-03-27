package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/snobb/goresq/pkg/config"
	"github.com/snobb/goresq/pkg/db"
	"github.com/snobb/goresq/pkg/job"
	"github.com/snobb/goresq/pkg/poller"
)

type DelayPlugin struct{}

func (d *DelayPlugin) Before(queue, class string, args []json.RawMessage) error {
	return nil // no-op
}

func (d *DelayPlugin) After(queue, class string, args []json.RawMessage, err error) error {
	fmt.Println("Delaying...")
	time.Sleep(1 * time.Second)
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
	redis := db.NewPool(&config.Redis{
		URI: "localhost:6379",
		DB:  4,
	})

	handlers := map[string]*job.Handler{
		"test": {
			Plugins: []job.Plugin{&DelayPlugin{}},
			Perform: func(queue, class string, args []json.RawMessage) error {
				var params struct {
					TaskData []int `json:"task_data"`
				}

				if err := json.Unmarshal(args[0], &params); err != nil {
					return err
				}

				fmt.Printf("%s : %s : %#v\n", queue, class, params.TaskData)
				fmt.Printf("sum: %d\n", sumArray(params.TaskData...))
				return nil
			},
		},
	}

	p := poller.New(redis, time.Second*2, 1)
	p.Start([]string{"queue1.test", "queue2.test"}, handlers)
}
