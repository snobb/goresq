package db

//go:generate moq -pkg mock -out mock/pooler.go . Pooler
//go:generate moq -pkg mock -out mock/conn.go . Conn

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/snobb/goresq/pkg/config"
)

type Pool struct {
	pool *redis.Pool
}

type Conn redis.Conn

type Pooler interface {
	Conn() (Conn, error)
	Close() error
}

func NewPool(config *config.Redis) *Pool {
	config.Defaults()

	dialOptions := []redis.DialOption{
		redis.DialConnectTimeout(time.Duration(config.ConnectTimeout) * time.Second),
	}

	if config.DB != 0 {
		dialOptions = append(dialOptions, redis.DialDatabase(config.DB))
	}

	return &Pool{
		pool: &redis.Pool{
			MaxIdle:     config.MaxIdle,
			MaxActive:   config.MaxActive,
			IdleTimeout: time.Duration(config.IdleTimeout) * time.Second,
			Dial: func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", config.URI, dialOptions...)
				if err != nil {
					return nil, fmt.Errorf("Unable to connect to %s: %w", config.URI, err)
				}

				return conn, nil
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}

func (r *Pool) Conn() (Conn, error) {
	conn := r.pool.Get()
	_, err := conn.Do("PING")
	return conn, err
}

func (r *Pool) Close() error {
	return r.pool.Close()
}
