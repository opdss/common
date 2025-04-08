package export

import (
	"context"
	"errors"
	"github.com/opdss/common/contracts/excel"
	"io"
)

var ErrMaximumLimit = errors.New("export quantity exceeds maximum limit")

const TagName = "export"         // export 导出字段的tag
const SingleFileMaxRows = 100000 //单个文件最大数据量
const MaxRows = 1000000          //最大导出数据,防止dataProvider出错无限数据导出

// ExcelSuffix CsvSuffix 导出文件后缀
const ExcelSuffix = "xlsx"
const CsvSuffix = "csv"
const ZipSuffix = "zip"

// ToExcelStream 导出excel的快捷方法
func ToExcelStream(ctx context.Context, h Headers, dp DataProvider, w io.Writer, opt ...Option) (int64, error) {
	return NewExcel(h, dp, opt...).ExportTo(ctx, w)
}

// ToExcelFile 导出excel的快捷方法
func ToExcelFile(ctx context.Context, h Headers, dp DataProvider, opt ...Option) (string, error) {
	return NewExcel(h, dp, opt...).Export(ctx)
}

// ToExcelStorage 导出excel到oss的快捷方法
func ToExcelStorage(ctx context.Context, h Headers, dp DataProvider, fs excel.FileStorage, opt ...Option) (string, error) {
	return NewExcel(h, dp, opt...).ExportToStorage(ctx, fs)
}

// ToCsvStream 导出csv的快捷方法
func ToCsvStream(ctx context.Context, h Headers, dp DataProvider, w io.Writer, opt ...Option) (int64, error) {
	return NewCsv(h, dp, opt...).ExportTo(ctx, w)
}

// ToCsvFile 导出csv的快捷方法
func ToCsvFile(ctx context.Context, h Headers, dp DataProvider, opt ...Option) (string, error) {
	return NewCsv(h, dp, opt...).Export(ctx)
}

// ToCsvStorage 导出csv到oss的快捷方法
func ToCsvStorage(ctx context.Context, h Headers, dp DataProvider, fs excel.FileStorage, opt ...Option) (string, error) {
	return NewCsv(h, dp, opt...).ExportToStorage(ctx, fs)
}
