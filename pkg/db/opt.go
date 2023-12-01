package db

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Option func(opts *redis.Options) *redis.Options

// Defaults sets the defaults
func Defaults() Option {
	return func(opts *redis.Options) *redis.Options {
		return &redis.Options{
			Addr: "localhost:6379",
			DB:   0,
		}
	}
}

// URL parses the url and sets the redis options
func URL(url string) Option {
	return func(orig *redis.Options) *redis.Options {
		opts, err := redis.ParseURL(url)
		if err != nil {
			panic(err)
		}
		*orig = *opts

		return orig
	}
}

// Addr sets hosts [and port] in the options
func Addr(addr string) Option {
	return func(opts *redis.Options) *redis.Options {
		opts.Addr = addr
		return opts
	}
}

// DB sets the db number
func DB(db int) Option {
	return func(opts *redis.Options) *redis.Options {
		opts.DB = db
		return opts
	}
}

func ReadTimeout(timeout time.Duration) Option {
	return func(opts *redis.Options) *redis.Options {
		opts.ReadTimeout = timeout
		return opts
	}
}

func DialTimeout(timeout time.Duration) Option {
	return func(opts *redis.Options) *redis.Options {
		opts.DialTimeout = timeout
		return opts
	}
}

func MaxRetries(retries int) Option {
	return func(opts *redis.Options) *redis.Options {
		opts.MaxRetries = retries
		return opts
	}
}

func WithRedisOptions(options *redis.Options) Option {
	return func(opts *redis.Options) *redis.Options {
		return options
	}
}
