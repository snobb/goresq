package db

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:generate moq -pkg mock -out mock/db.go . Accessor

type Accessor interface {
	Do(ctx context.Context, args ...any) (any, error)

	LPush(ctx context.Context, key string, value any) (any, error)
	RPush(ctx context.Context, key string, value any) (any, error)

	LPop(ctx context.Context, key string) (any, error)
	RPop(ctx context.Context, key string) (any, error)

	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value any, exp time.Duration) (string, error)
	Del(ctx context.Context, keys ...string) (int64, error)

	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)

	SAdd(ctx context.Context, key string, members ...any) (int64, error)
	SRem(ctx context.Context, key string, members ...any) (int64, error)
}

// Store server instance
type Store struct {
	client *redis.Client
}

// New returns a new redis client
func New(options ...Option) *Store {
	opts := &redis.Options{}
	for _, opFn := range options {
		opts = opFn(opts)
	}

	opts.OnConnect = func(ctx context.Context, cn *redis.Conn) error {
		_, err := cn.Ping(ctx).Result()
		return err
	}

	return &Store{
		client: redis.NewClient(opts),
	}
}

func (kv *Store) Client() *redis.Client {
	return kv.client
}

// Get is a thin abstraction on top of redis-go
func (kv *Store) Get(ctx context.Context, key string) (string, error) {
	return kv.client.Get(ctx, key).Result()
}

// Do is a thin abstraction on top of redis-go
func (kv *Store) Do(ctx context.Context, args ...any) (any, error) {
	return kv.client.Do(ctx, args...).Result()
}

// LPush is a thin abstraction on top of redis-go
func (kv *Store) LPush(ctx context.Context, key string, value any) (any, error) {
	return kv.client.LPush(ctx, key, value).Result()
}

// RPush is a thin abstraction on top of redis-go
func (kv *Store) RPush(ctx context.Context, key string, value any) (any, error) {
	return kv.client.RPush(ctx, key, value).Result()
}

// LPop is a thin abstraction on top of redis-go
func (kv *Store) LPop(ctx context.Context, key string) (any, error) {
	return kv.client.LPop(ctx, key).Result()
}

// RPop is a thin abstraction on top of redis-go
func (kv *Store) RPop(ctx context.Context, key string) (any, error) {
	return kv.client.RPop(ctx, key).Result()
}

// Set is a thin abstraction on top of redis-go
func (kv *Store) Set(ctx context.Context, key string, value any, exp time.Duration) (string, error) {
	return kv.client.Set(ctx, key, value, exp).Result()
}

// Del is a thin abstraction on top of redis-go
func (kv *Store) Del(ctx context.Context, keys ...string) (int64, error) {
	return kv.client.Del(ctx, keys...).Result()
}

// Incr is a thin abstraction on top of redis-go
func (kv *Store) Incr(ctx context.Context, key string) (int64, error) {
	return kv.client.Incr(ctx, key).Result()
}

// Decr is a thin abstraction on top of redis-go
func (kv *Store) Decr(ctx context.Context, key string) (int64, error) {
	return kv.client.Decr(ctx, key).Result()
}

// SAdd is a thin abstraction on top of redis-go
func (kv *Store) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return kv.client.SAdd(ctx, key, members...).Result()
}

// SRem is a thin abstraction on top of redis-go
func (kv *Store) SRem(ctx context.Context, key string, members ...any) (int64, error) {
	return kv.client.SRem(ctx, key, members...).Result()
}
