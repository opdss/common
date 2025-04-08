package export

type columns struct {
	headers       Headers               //导出表配置
	fields        []string              //导出字段名
	titles        []string              //导出列名
	nums          int                   //列数量
	keyIndex      map[string]int        //列字段索引映射
	nilKeyIndex   map[string]int        //用来做struct补充数据
	columnRenders map[string]CellRender //列字段渲染函数映射
}

func newColumns(headers Headers) *columns {
	size := len(headers)
	if size == 0 {
		panic("header is empty")
	}
	c := &columns{
		headers:       headers,
		fields:        make([]string, size),
		titles:        make([]string, size),
		nums:          size,
		keyIndex:      make(map[string]int),
		nilKeyIndex:   make(map[string]int),
		columnRenders: make(map[string]CellRender),
	}
	for i := 0; i < size; i++ {
		c.fields[i] = headers[i].Field
		c.titles[i] = headers[i].Title
		c.keyIndex[headers[i].Field] = i
		c.nilKeyIndex[headers[i].Field] = 0
		c.columnRenders[headers[i].Field] = headers[i].CellRender
	}
	return c
}

func (c *columns) getTitleToAny() []any {
	res := make([]any, len(c.titles))
	for i := range c.titles {
		res[i] = c.titles[i]
	}
	return res
}
