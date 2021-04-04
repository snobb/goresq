package poller

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/snobb/goresq/pkg/db"
)

// Track represents a redis connection tracker.
type Track struct {
	Hostname  string
	Pid       int
	ID        string
	Namespace string
	Queues    []string
}

func newTrack(id, Namespace string, queues []string) Track {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	return Track{
		Hostname:  hostname,
		Pid:       os.Getpid(),
		ID:        id,
		Namespace: Namespace,
		Queues:    queues,
	}
}

func (t *Track) String() string {
	return fmt.Sprintf("%s:%d-%s:%s", t.Hostname, t.Pid, t.ID, strings.Join(t.Queues, ","))
}

func (t *Track) track(conn db.Conn) error {
	conn.Send("SADD", fmt.Sprintf("%s:workers", t.Namespace), t)
	conn.Send("SET", fmt.Sprintf("%s:stat:processed:%v", t.Namespace, t), "0")
	conn.Send("SET", fmt.Sprintf("%s:stat:failed:%v", t.Namespace, t), "0")
	conn.Send("SET", fmt.Sprintf("%s:worker:%s:started", t.Namespace, t), int64(time.Now().Unix()))

	return nil
}

func (t *Track) untrack(conn db.Conn) error {
	conn.Send("SREM", fmt.Sprintf("%s:workers", t.Namespace), t)
	conn.Send("DEL", fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t))
	conn.Send("DEL", fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t))
	conn.Send("DEL", fmt.Sprintf("%s:worker:%s", t.Namespace, t))
	conn.Send("DEL", fmt.Sprintf("%s:worker:%s:started", t.Namespace, t))
	conn.Flush()

	return nil
}

func (t *Track) success(conn db.Conn) error {
	conn.Send("INCR", fmt.Sprintf("%s:stat:processed", t.Namespace))
	conn.Send("INCR", fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t))
	conn.Flush()

	return nil
}

func (t *Track) fail(conn db.Conn) error {
	conn.Send("INCR", fmt.Sprintf("%s:stat:failed", t.Namespace))
	conn.Send("INCR", fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t))
	conn.Flush()

	return nil
}
