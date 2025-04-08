package redis

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

var ErrTimeout = errors.New("try lock time out")
var ErrFailure = errors.New("get lock failure")

const delLua = `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) end return 0`

// Locker 基于redis实现的分布式锁
type Locker struct {
	client       *redis.Client
	unlockScript *redis.Script
	key          string
	token        string
	deadline     time.Time
}

func NewLocker(key string, rdb *redis.Client) *Locker {
	return &Locker{
		client:       rdb,
		key:          key,
		token:        uuid.New().String(),
		unlockScript: redis.NewScript(delLua),
	}
}

// Lock 非阻塞锁
func (l *Locker) Lock(exp time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), exp)
	defer cancel()
	ok, err := l.client.SetNX(ctx, l.key, l.token, exp).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrFailure
	}
	l.deadline = time.Now().Add(exp)
	return nil
}

// TryLock 自旋锁
func (l *Locker) TryLock(wait time.Duration) (err error) {
	ttl := wait.Milliseconds()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var ok bool
	for {
		// 超时时间与锁时间相同
		if time.Since(start).Milliseconds() < ttl {
			ok, err = l.client.SetNX(ctx, l.key, l.token, wait).Result()
			if err != nil {
				time.Sleep(time.Millisecond * 50)
				continue
			}
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			}
			l.deadline = time.Now().Add(wait)
			return nil
		}
		if err != nil {
			return err
		}
		return ErrTimeout
	}
}

func (l *Locker) UnLock() {
	if l.deadline.IsZero() {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), l.deadline)
	defer cancel()
	err := l.unlockScript.Run(ctx, l.client, []string{l.key}, l.token).Err()
	if err != nil {
		log.Printf("redis unlock error[%s]:%s\n,", l.key, err)
	}
}
