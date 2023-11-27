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

func newTrack(id, ns string, queues []string) Track {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	return Track{
		Hostname:  hostname,
		Pid:       os.Getpid(),
		ID:        id,
		Namespace: ns,
		Queues:    queues,
	}
}

func (t *Track) String() string {
	return fmt.Sprintf("%s:%d-%s:%s", t.Hostname, t.Pid, t.ID, strings.Join(t.Queues, ","))
}

func (t *Track) track(conn db.Conn) error {
	if err := conn.Send("SADD", fmt.Sprintf("%s:workers", t.Namespace), t); err != nil {
		return err
	}

	if err := conn.Send("SET", fmt.Sprintf("%s:stat:processed:%v", t.Namespace, t), "0"); err != nil {
		return err
	}

	if err := conn.Send("SET", fmt.Sprintf("%s:stat:failed:%v", t.Namespace, t), "0"); err != nil {
		return err
	}

	if err := conn.Send("SET", fmt.Sprintf("%s:worker:%s:started", t.Namespace, t), time.Now().Unix()); err != nil {
		return err
	}

	_ = conn.Flush()

	return nil
}

func (t *Track) untrack(conn db.Conn) error {
	if err := conn.Send("SREM", fmt.Sprintf("%s:workers", t.Namespace), t); err != nil {
		return err
	}

	if err := conn.Send("DEL", fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t)); err != nil {
		return err
	}

	if err := conn.Send("DEL", fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t)); err != nil {
		return err
	}

	if err := conn.Send("DEL", fmt.Sprintf("%s:worker:%s", t.Namespace, t)); err != nil {
		return err
	}

	if err := conn.Send("DEL", fmt.Sprintf("%s:worker:%s:started", t.Namespace, t)); err != nil {
		return err
	}

	_ = conn.Flush()

	return nil
}

func (t *Track) success(conn db.Conn) error {
	if err := conn.Send("INCR", fmt.Sprintf("%s:stat:processed", t.Namespace)); err != nil {
		return err
	}

	if err := conn.Send("INCR", fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t)); err != nil {
		return err
	}

	_ = conn.Flush()

	return nil
}

func (t *Track) fail(conn db.Conn) error {
	if err := conn.Send("INCR", fmt.Sprintf("%s:stat:failed", t.Namespace)); err != nil {
		return err
	}

	if err := conn.Send("INCR", fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t)); err != nil {
		return err
	}

	_ = conn.Flush()

	return nil
}
