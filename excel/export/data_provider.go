package export

import (
	"github.com/opdss/common/contracts/iterator"
	"github.com/xuri/excelize/v2"
)

// DataProvider 数据提供者
type DataProvider iterator.Iterator[any]

// CellRender 单元格数据渲染
// @rowData 整个行数据
// @val 获取到的单元格元数据
// @row 当前行数
// @col 当前列数
type CellRender func(rowData any, val any, row int, col int) any

// Header 表头
type Header struct {
	Field      string          //字段名
	Title      string          //列名
	CellRender CellRender      //单元格数据处理
	ColStyle   *excelize.Style //列样式,导出excel时支持
	ColWidth   float64         //列宽度,导出excel时支持
}

// Headers 表头
type Headers []Header
