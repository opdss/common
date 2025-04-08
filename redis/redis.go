package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Host            string        `help:"redis主机" default:"127.0.0.1"`
	Port            int           `help:"redis端口" default:"6379"`
	Password        string        `help:"redis密码" default:""`
	Db              int           `help:"redis数据库" default:"0"`
	MaxIdleConn     int           `help:"连接池中空闲连接的最大数量" default:"0"`
	MaxActiveConns  int           `help:"最大的活动连接数量" default:"0"`
	ConnMaxLifetime time.Duration `help:"连接可复用的最大时间" default:"0"`
	ConnMaxIdleTime time.Duration `help:"连接可以空闲的最长时间" default:"0"`
	DialTimeout     time.Duration `help:"" default:"0"`
	ReadTimeout     time.Duration `help:"" default:"0"`
	WriteTimeout    time.Duration `help:"" default:"0"`
}

func NewRedis(conf Config) (*redis.Client, error) {
	opts := redis.Options{
		Addr:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Password: conf.Password,
		DB:       conf.Db,
	}
	if conf.MaxActiveConns > 0 {
		opts.MaxActiveConns = conf.MaxActiveConns
	}
	if conf.MaxIdleConn > 0 {
		opts.MaxIdleConns = conf.MaxIdleConn
	}
	if conf.ConnMaxLifetime > 0 {
		opts.ConnMaxLifetime = conf.ConnMaxLifetime
	}
	if conf.ConnMaxIdleTime > 0 {
		opts.ConnMaxIdleTime = conf.ConnMaxIdleTime
	}

	if conf.DialTimeout > 0 {
		opts.DialTimeout = conf.DialTimeout
	}
	if conf.ReadTimeout > 0 {
		opts.ReadTimeout = conf.ReadTimeout
	}
	if conf.WriteTimeout > 0 {
		opts.WriteTimeout = conf.WriteTimeout
	}

	client := redis.NewClient(&opts)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("init redis connection error:%s", err)
	}
	return client, nil
}
