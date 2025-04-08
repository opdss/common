package db

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MsConfig struct {
	Master  Config        `help:"主库"`
	Slave   Config        `help:"从库"`
	Plugins []gorm.Plugin `help:"插件" internal:"true"`
}

type MsDb struct {
	master *gorm.DB
	slave  *gorm.DB
}

func NewMsDB(logger *zap.Logger, conf MsConfig) (_ *MsDb, err error) {
	ms := &MsDb{}
	if ms.master, err = NewDB(logger, conf.Master); err != nil {
		return nil, err
	}

	if ms.slave, err = NewDB(logger, conf.Slave); err != nil {
		return nil, err
	}
	if conf.Plugins != nil && len(conf.Plugins) > 0 {
		for _, p := range conf.Plugins {
			if err = ms.master.Use(p); err != nil {
				return nil, err
			}
			if err = ms.slave.Use(p); err != nil {
				return nil, err
			}
		}
	}
	return ms, nil
}

func (mdb *MsDb) Master() *gorm.DB {
	return mdb.master
}

func (mdb *MsDb) Slave() *gorm.DB {
	return mdb.slave
}
