package export

import (
	"archive/zip"
	"fmt"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/rand"
	"io"
	"os"
	"path"
	filepath2 "path/filepath"
	"time"
)

type exportFile interface {
	Filepath() string //本地文件路径
	WriteTo(w io.Writer) (n int64, err error)
	Close() error
	Save() (string, error)
}

var _ exportFile = (*exportTmpFile)(nil)

// exportTmpFile 临时文件
type exportTmpFile struct {
	filepath string
	*os.File
}

func newExportTmpFile(filepath string) (*exportTmpFile, error) {
	ef := &exportTmpFile{filepath: filepath}
	var err error
	ef.File, err = os.Create(filepath)
	if err != nil {
		return nil, err
	}
	return ef, nil
}

func (e *exportTmpFile) Filepath() string {
	return e.filepath
}

func (e *exportTmpFile) WriteTo(w io.Writer) (n int64, err error) {
	_, err = e.File.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return e.File.WriteTo(w)
}

func (e *exportTmpFile) Close() error {
	return e.File.Close()
}

func (e *exportTmpFile) Save() (string, error) {
	return e.filepath, nil
}

var _ exportFile = (*exportExcel)(nil)

// exportExcel excelize文件
type exportExcel struct {
	filepath string
	fp       *excelize.File
}

func newExportExcel(filepath string, fp *excelize.File) *exportExcel {
	return &exportExcel{
		filepath: filepath,
		fp:       fp,
	}
}

func (e *exportExcel) Filepath() string {
	return e.filepath
}

func (e *exportExcel) WriteTo(w io.Writer) (n int64, err error) {
	return e.fp.WriteTo(w)
}

func (e *exportExcel) Close() error {
	return e.fp.Close()
}

func (e *exportExcel) Save() (string, error) {
	return e.filepath, e.fp.SaveAs(e.filepath)
}

// getFilename 生成导出文件名
func getFilename(filename string, idx int, suf string) string {
	tmp := os.TempDir()
	if filename == "" {
		return path.Join(tmp,
			fmt.Sprintf("export_%s_%d_%d.%s",
				time.Now().Format("20060102_150405"),
				randInt(1000, 9999),
				idx,
				suf))
	}
	if filename[0] == os.PathSeparator {
		return fmt.Sprintf("%s_%d.%s", filename, idx, suf)
	}
	return fmt.Sprintf("%s_%d.%s", path.Join(tmp, filename), idx, suf)
}

func randInt(min, max int) int {
	return rand.Intn(max-min) + min
}

func newZipWriter(zw *zip.Writer, filepath string) (io.Writer, error) {
	return zw.CreateHeader(&zip.FileHeader{
		Name:     filepath2.Base(filepath),
		Method:   zip.Deflate,
		Modified: time.Now(),
	})
}
