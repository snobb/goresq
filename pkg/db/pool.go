package db

//go:generate moq -pkg mock -out mock/pooler.go . Pooler
//go:generate moq -pkg mock -out mock/conn.go . Conn

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Config is a configuration for the redis pool
type Config struct {
	URI            string `json:"uri"`
	DB             int    `json:"db"`
	MaxIdle        int    `json:"max_idle"`
	MaxActive      int    `json:"max_active"`
	IdleTimeout    int    `json:"idle_timeout"`
	ConnectTimeout int    `json:"connection_timeout"`
}

// Defaults sets the redis connection defaults.
func (r *Config) Defaults() {
	if r.MaxIdle == 0 {
		r.MaxIdle = 500
	}

	if r.MaxActive == 0 {
		r.MaxActive = 500
	}

	if r.IdleTimeout == 0 {
		r.IdleTimeout = 5
	}

	if r.ConnectTimeout == 0 {
		r.ConnectTimeout = 0
	}
}

// Pool represents a redis pool
type Pool struct {
	pool *redis.Pool
}

// Conn represents a redis connection
type Conn redis.Conn

// Pooler is a redis pool interface
type Pooler interface {
	Conn() (Conn, error)
	Close() error
}

// NewPool creates new redis pool with the given configuration.
func NewPool(config *Config) *Pool {
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

// Conn returns a new redis connection. Caller must close the connection.
// Before returning the connection is tested and if it's not alive - an error is returned.
func (r *Pool) Conn() (Conn, error) {
	conn := r.pool.Get()
	_, err := conn.Do("PING")
	return conn, err
}

// Close closes the redis pool
func (r *Pool) Close() error {
	return r.pool.Close()
}
