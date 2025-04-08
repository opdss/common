package locker

import "time"

type Locker interface {
	//Lock 非阻塞锁
	Lock(time.Duration) error
	//TryLock 自旋锁
	TryLock(time.Duration) error
	// Unlock 解锁
	Unlock() error
}
