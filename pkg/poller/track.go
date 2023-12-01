package poller

import (
	"context"
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

func (t *Track) track(ctx context.Context, dbh db.Accessor) error {
	if _, err := dbh.SAdd(ctx, fmt.Sprintf("%s:workers", t.Namespace), t); err != nil {
		return err
	}

	if _, err := dbh.Set(ctx, fmt.Sprintf("%s:stat:processed:%v", t.Namespace, t), "0", 0); err != nil {
		return err
	}

	if _, err := dbh.Set(ctx, fmt.Sprintf("%s:stat:failed:%v", t.Namespace, t), "0", 0); err != nil {
		return err
	}

	if _, err := dbh.Set(ctx, fmt.Sprintf("%s:worker:%s:started", t.Namespace, t), time.Now().Unix(), 0); err != nil {
		return err
	}

	return nil
}

func (t *Track) untrack(ctx context.Context, dbh db.Accessor) error {
	if _, err := dbh.SRem(ctx, fmt.Sprintf("%s:workers", t.Namespace), t); err != nil {
		return err
	}

	if _, err := dbh.Del(ctx, fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t)); err != nil {
		return err
	}

	if _, err := dbh.Del(ctx, fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t)); err != nil {
		return err
	}

	if _, err := dbh.Del(ctx, fmt.Sprintf("%s:worker:%s", t.Namespace, t)); err != nil {
		return err
	}

	if _, err := dbh.Del(ctx, fmt.Sprintf("%s:worker:%s:started", t.Namespace, t)); err != nil {
		return err
	}

	return nil
}

func (t *Track) success(ctx context.Context, dbh db.Accessor) error {
	if _, err := dbh.Incr(ctx, fmt.Sprintf("%s:stat:processed", t.Namespace)); err != nil {
		return err
	}

	if _, err := dbh.Incr(ctx, fmt.Sprintf("%s:stat:processed:%s", t.Namespace, t)); err != nil {
		return err
	}

	return nil
}

func (t *Track) fail(ctx context.Context, dbh db.Accessor) error {
	if _, err := dbh.Incr(ctx, fmt.Sprintf("%s:stat:failed", t.Namespace)); err != nil {
		return err
	}

	if _, err := dbh.Incr(ctx, fmt.Sprintf("%s:stat:failed:%s", t.Namespace, t)); err != nil {
		return err
	}

	return nil
}
