package db

import (
	"errors"
	"go.uber.org/zap"
	"time"

	"github.com/zeebo/errs"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const Mysql = "mysql"

const Postgresql = "postgres"

const Sqlite3 = "sqlite3"

var ErrDB = errs.Class("DB")

type Config struct {
	Driver string `help:"数据库驱动" default:"sqlite3"`
	//Dsn string `help:"数据库连接"  default:"ljg:abcd123456@tcp(192.168.43.90:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"`
	//Dsn string `help:"数据库连接"  default:"host=myhost port=myport user=gorm dbname=gorm password=mypassword"`
	//Dsn string `help:"数据库连接"  default:"/tmp/gorm.db"`
	Dsn             string        `help:"数据库连接"  default:"$ROOT/sqlite.db"`
	LogLevel        string        `help:"数据库日志打印级别,默认为空,可选[error|warn|info]" releaseDefault:"warn" default:"info"`
	MaxIdleConn     int           `help:"连接池中空闲连接的最大数量" default:"10"`
	MaxOpenConn     int           `help:"打开数据库连接的最大数量" default:"100"`
	ConnMaxLifetime time.Duration `help:"连接可复用的最大时间" default:"1h"`
	ConnMaxIdleTime time.Duration `help:"连接可以空闲的最长时间" default:"0"`
}

func (conf *Config) Dialector() (dial gorm.Dialector, err error) {
	switch conf.Driver {
	case Mysql:
		dial = mysql.New(mysql.Config{
			DSN:                       conf.Dsn,
			DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
			DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
			DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
			SkipInitializeWithVersion: false, // 根据当前 MySQL 版本自动配置
		})
	case Postgresql:
		dial = postgres.New(postgres.Config{
			DSN: conf.Dsn,
		})
	case Sqlite3:
		dial = sqlite.Open(conf.Dsn)
	default:
		return nil, errors.New("database url error")
	}
	return
}

//	func NewDB(cfg *Config, zapLog *zap.Logger) (*gorm.DB, error) {
//		dail, err := cfg.Dialector()
//		if err != nil {
//			return nil, ErrDB.Wrap(err)
//		}
//		db, err := gorm.Open(dail, &gorm.Config{
//			DisableForeignKeyConstraintWhenMigrating: true,
//			SkipDefaultTransaction:                   true,
//			Logger:                                   getLogInterface(zapLog, cfg.LogLevel),
//		})
//		return db, err
//	}
func NewDB(zapLog *zap.Logger, cfg Config) (*gorm.DB, error) {
	dail, err := cfg.Dialector()
	if err != nil {
		return nil, ErrDB.Wrap(err)
	}
	db, err := gorm.Open(dail, &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		SkipDefaultTransaction:                   true,
		Logger:                                   getLogInterface(zapLog, cfg.LogLevel),
	})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	if cfg.MaxIdleConn > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConn)
	}
	if cfg.MaxOpenConn > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConn)
	}
	if cfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
	return db, nil
}
