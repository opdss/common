package export

import (
	"gorm.io/gorm"
)

type SqlDataProvider[T any] struct {
	gormDpT *GormDataProviderT[T]
}

func NewSqlDataProvider[T any](db *gorm.DB, selectSql string, opts ...GormDataProviderTOption[T]) *SqlDataProvider[T] {
	return &SqlDataProvider[T]{
		gormDpT: NewGormDataProviderT[T](db.Raw(selectSql), opts...),
	}
}

func (dp *SqlDataProvider[T]) Next() bool {
	return dp.gormDpT.Next()
}

func (dp *SqlDataProvider[T]) Value() any {
	return dp.gormDpT.Value()
}
