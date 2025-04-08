package export

import (
	"reflect"
)

var nilValue reflect.Value

type Option func(opt *options)

// WithMaxRows 最大数据行数，超过会报异常
func WithMaxRows(n int) Option {
	return func(opt *options) {
		if n > 0 && n < MaxRows {
			opt.maxRows = n
		}
	}
}

// WithSingleFileMaxRows 单个文件导出最大数量，超出会自动切分
func WithSingleFileMaxRows(n int) Option {
	return func(opt *options) {
		if n >= 0 && n < SingleFileMaxRows {
			opt.singleFileMaxRows = n
		}
	}
}

// WithFilename 设置导出文件名,不用加后缀，会自动加
func WithFilename(filename string) Option {
	return func(opt *options) {
		opt.filename = filename
	}
}

// WithRowStart 设置数据从第几行开始写入，导出excel生效
func WithRowStart(n int) Option {
	return func(opt *options) {
		if n >= 0 {
			opt.rowStart = n
		}
	}
}

// WithColStart 设置数据从第几列开始写入，导出excel生效
func WithColStart(n int) Option {
	return func(opt *options) {
		if n >= 0 {
			opt.colStart = n
		}
	}
}

// WithForceZip 是否强制zip压缩，即导出只有一个文件时也压缩成zip
func WithForceZip() Option {
	return func(opt *options) {
		opt.forceZip = true
	}
}

// WithForceSingleFile 是否强制单文件导出，为ture时即使数量超单文件大小也不会切片
func WithForceSingleFile() Option {
	return func(opt *options) {
		opt.forceSingleFile = true
	}
}

type options struct {
	maxRows           int    //导出最大数量，避免数据提供商出错无限数据
	singleFileMaxRows int    //单个文件导出最大数量，超出会自动切分
	filename          string //文件名，不要加后缀，会自动加
	rowStart          int    //从第几行开始写数据，仅导出 excel支持
	colStart          int    //从第几列开始写数据，仅导出 excel支持
	forceZip          bool   //是否强制zip压缩，即导出只有一个文件时也压缩成zip
	forceSingleFile   bool   //是否强制单文件导出，为ture时即使数量超单文件大小也不会切片
}

func newOptions(opts ...Option) *options {
	o := &options{
		maxRows:           MaxRows,
		singleFileMaxRows: SingleFileMaxRows,
		filename:          "",
		rowStart:          0,
		colStart:          0,
		forceZip:          false,
		forceSingleFile:   false,
	}
	for i := range opts {
		opts[i](o)
	}
	return o
}
