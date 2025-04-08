package excel

import (
	"context"
	"io"
)

type FileStorage interface {
	PutStream(ctx context.Context, filename string, rs io.Reader) error
	Url(fileKey string) string
}

// Exporter 导出接口
type Exporter interface {
	// Export 导出到本地文件，返回本地文件路径
	Export(ctx context.Context) (string, error)
	// ExportTo 导出到io.Writer
	ExportTo(ctx context.Context, at io.Writer) (int64, error)
	// ExportToStorage 导出到文件存储，返回下载地址
	ExportToStorage(ctx context.Context, fileStorage FileStorage) (string, error)
}
