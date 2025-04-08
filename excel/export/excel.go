package export

import (
	"archive/zip"
	"context"
	"github.com/opdss/common/contracts/excel"
	"github.com/xuri/excelize/v2"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"sync"
)

// DefaultSheetName 默认操作表
const DefaultSheetName = "Sheet1"

var _ excel.Exporter = (*Excel)(nil)

type Excel struct {
	options *options
	columns *columns
	dp      DataProvider
	total   int
}

func NewExcel(h Headers, dp DataProvider, opts ...Option) *Excel {
	e := &Excel{
		dp:      dp,
		columns: newColumns(h),
		options: newOptions(opts...),
	}
	return e
}

// Export 导出到本地文件，返回本地文件路径
func (e *Excel) Export(ctx context.Context) (string, error) {
	ef, err := e.export(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if err = ef.Close(); err != nil {
			log.Println("excel Export() close err:", err)
		}
	}()
	return ef.Save()
}

// ExportTo 导出到io.Writer
func (e *Excel) ExportTo(ctx context.Context, w io.Writer) (n int64, err error) {
	ef, err := e.export(ctx)
	if err != nil {
		return
	}
	defer func() {
		if err = ef.Close(); err != nil {
			log.Println("excel Export() close err:", err)
		}
	}()
	return ef.WriteTo(w)
}

// ExportToStorage 导出到文件存储，返回下载地址
func (e *Excel) ExportToStorage(ctx context.Context, fs excel.FileStorage) (string, error) {
	ef, err := e.export(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if err = ef.Close(); err != nil {
			log.Println("excel Export() close err:", err)
		}
	}()
	fk := filepath.Base(ef.Filepath())
	fr, fw := io.Pipe()
	wg := sync.WaitGroup{}
	wg.Add(2)
	var _err error
	go func() {
		defer wg.Done()
		if _, _err = ef.WriteTo(fw); _err != nil {
			log.Println("io pipe write error", _err.Error())
		}
		_ = fw.Close()
	}()
	go func() {
		defer wg.Done()
		if _err = fs.PutStream(ctx, fk, fr); _err != nil {
			log.Println("io pipe read error", _err.Error())
		}
		_ = fr.Close()
	}()
	wg.Wait()
	if _err != nil {
		return "", _err
	}
	return fs.Url(fk), nil
}

// 执行导出
func (e *Excel) export(ctx context.Context) (ef exportFile, err error) {
	//强制打包zip
	if e.options.forceZip {
		return e.exportZip(ctx, nil)
	}
	//先导出第一个文件
	firstFile := excelize.NewFile()
	hasMore, err := e.exportToExcelize(ctx, firstFile)
	if err != nil {
		_ = firstFile.Close()
		return nil, err
	}
	if !hasMore {
		return newExportExcel(getFilename(e.options.filename, 0, ExcelSuffix), firstFile), nil
	}
	defer func() {
		_ = firstFile.Close()
	}()
	//导出zip
	return e.exportZip(ctx, firstFile)
}

func (e *Excel) exportZip(ctx context.Context, firstFile *excelize.File) (exportFile, error) {
	var idx int
	ef, err := newExportTmpFile(getFilename(e.options.filename, idx, ZipSuffix))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = ef.Close()
		}
	}()
	zw := zip.NewWriter(ef)
	defer func() {
		_ = zw.Close()
	}()
	var w io.Writer
	//把外面传进来的加进去
	if firstFile != nil {
		w, err = newZipWriter(zw, getFilename(e.options.filename, 0, ExcelSuffix))
		if err != nil {
			return nil, err
		}
		if err = firstFile.Write(w); err != nil {
			return nil, err
		}
		idx++
	}
	//读取数据
	hasMore := true
	for {
		if !hasMore {
			break
		}
		w, err = newZipWriter(zw, getFilename(e.options.filename, idx, ExcelSuffix))
		if err != nil {
			return nil, err
		}
		idx++
		fw := excelize.NewFile()
		hasMore, err = e.exportToExcelize(ctx, fw)
		if err != nil {
			_ = fw.Close()
			return nil, err
		}
		//写入zip
		if err = fw.Write(w); err != nil {
			_ = fw.Close()
			return nil, err
		}
		_ = fw.Close()
	}
	return ef, nil
}

func (e *Excel) exportToExcelize(ctx context.Context, fp *excelize.File) (hasMore bool, err error) {
	row := e.options.rowStart + 1
	col := e.options.colStart + 1

	//设置列相关属性
	if err = e.setColStyle(col, fp); err != nil {
		return false, err
	}

	fw, err := fp.NewStreamWriter(DefaultSheetName)
	if err != nil {
		return
	}
	//设置导出表头
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return
	}
	if err = fw.SetRow(cell, e.columns.getTitleToAny()); err != nil {
		return
	}
	//开始写入数据
	for {
		row++
		if !e.dp.Next() {
			break
		}
		_v := e.dp.Value()
		values := e.processRow(reflect.ValueOf(_v), _v, row)
		cell, err = excelize.CoordinatesToCellName(col, row)
		if err != nil {
			log.Println(err)
			break
		}
		err = fw.SetRow(cell, values)
		if err != nil {
			log.Println(err)
			break
		}
		if !e.options.forceSingleFile && row-e.options.rowStart-1 >= e.options.singleFileMaxRows {
			hasMore = true
			break
		}
		//检查是否超过最大导出限制
		e.total++
		if e.total > e.options.maxRows {
			err = ErrMaximumLimit
			break
		}
		//收到取消导出信号
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		_ = fw.Flush()
		return
	}
	err = fw.Flush()
	return
}

// setColStyle 设置列相关属性
func (e *Excel) setColStyle(colStart int, fp *excelize.File) error {
	//设置列宽度
	for i, h := range e.columns.headers {
		var colName string
		colName, err := excelize.ColumnNumberToName(colStart + i)
		if err != nil {
			return err
		}
		//设置宽度
		if h.ColWidth > 0 {
			if err = fp.SetColWidth(DefaultSheetName, colName, colName, h.ColWidth); err != nil {
				return err
			}
		}
		//设置列样式
		if h.ColStyle != nil {
			styleId, err := fp.NewStyle(h.ColStyle)
			if err != nil {
				return err
			}
			if err = fp.SetColStyle(DefaultSheetName, colName, styleId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Excel) TestProcessRow(rowData reflect.Value, row int) []any {
	return e.processRow(rowData, rowData.Interface(), row)
}

func (e *Excel) processRow(rowData reflect.Value, rawData any, row int) []any {
	if !rowData.IsValid() {
		return make([]any, e.columns.nums)
	}
	switch rowData.Type().Kind() {
	case reflect.Ptr:
		return e.processRow(rowData.Elem(), rawData, row)
	case reflect.Map:
		return e.processRowFromMap(rowData, rawData, row)
	case reflect.Struct:
		return e.processRowFromStruct(rowData, rawData, row)
	case reflect.Slice:
		return e.processRowFromSlice(rowData, rawData, row)
	default:
		return make([]any, e.columns.nums)
	}
}

func (e *Excel) processRowFromMap(rowData reflect.Value, rawData any, row int) []any {
	_rowData := make([]any, e.columns.nums)
	for i := range e.columns.fields {
		field := e.columns.fields[i]
		val := rowData.MapIndex(reflect.ValueOf(field))
		_rowData[i] = e.processCell(field, val, rawData, row, e.options.colStart+i+1)
	}
	return _rowData
}

func (e *Excel) processRowFromStruct(rowData reflect.Value, rawData any, row int) []any {
	_rowData := make([]any, e.columns.nums)
	typ := rowData.Type()
	gets := 0
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		key := field.Name
		//读取tag
		if k, ok := typ.Field(i).Tag.Lookup(TagName); ok {
			key = k
		}
		if idx, ok := e.columns.keyIndex[key]; ok {
			gets++
			e.columns.nilKeyIndex[key] = row
			_rowData[idx] = e.processCell(key, rowData.Field(i), rawData, row, e.options.colStart+idx+1)
		}
		if gets == e.columns.nums {
			break
		}
	}
	//没取到的字段得补充下
	if gets != e.columns.nums {
		var idx int
		for k, v := range e.columns.nilKeyIndex {
			if v != row {
				idx = e.columns.keyIndex[k]
				_rowData[idx] = e.processCell(k, nilValue, rawData, row, e.options.colStart+idx+1)
			}
		}
	}
	return _rowData
}

// processRowFromSlice 处理数据数据
func (e *Excel) processRowFromSlice(rowData reflect.Value, rawData any, row int) []any {
	_rowData := make([]any, e.columns.nums)
	l := rowData.Len()
	for i := 0; i < e.columns.nums; i++ {
		if i < l {
			_rowData[i] = e.processCell(e.columns.fields[i], rowData.Index(i), rawData, row, e.options.colStart+i+1)
		} else {
			_rowData[i] = e.processCell(e.columns.fields[i], nilValue, rawData, row, e.options.colStart+i+1)
		}
	}
	return _rowData
}

func (e *Excel) processCell(field string, val reflect.Value, rawData any, row, col int) any {
	var v any
	if val.IsValid() {
		v = val.Interface()
	}
	if e.columns.columnRenders[field] != nil {
		return e.columns.columnRenders[field](rawData, v, row, col)
	}
	return v
}
