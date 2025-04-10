package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/opdss/common/contracts/storage"
)

type LocalConfig struct {
	Endpoint string `help:"访问地址" default:"http://localhost" json:"endpoint"`
	Root     string `help:"根目录" default:"$ROOT" json:"root"`
}

var _ storage.FileSystem = (*Local)(nil)

type Local struct {
	root     string
	endpoint string
}

func NewLocal(config LocalConfig) (*Local, error) {
	return &Local{
		root:     config.Root,
		endpoint: strings.TrimSuffix(config.Endpoint, "/"),
	}, nil
}

func (r *Local) ListObjects(ctx context.Context, opt *storage.ListObjectOpts) (*storage.ListObjectRes, error) {
	res := &storage.ListObjectRes{
		List: make([]storage.ListObject, 0),
	}
	start := false
	if opt.NextToken == "" {
		start = true
	}
	pre := r.fullPath(opt.Directory)
	num := getPageSize(opt.MaxKeys)
	err := filepath.Walk(pre, func(path string, info fs.FileInfo, err error) error {
		fileKey := strings.TrimPrefix(path, pre)
		if fileKey == "" {
			return nil
		}
		if !start {
			if opt.NextToken == fileKey {
				start = true
			}
			return nil
		}
		num -= 1
		if num == -1 {
			res.HasMore = true
			return io.EOF
		}
		if info.IsDir() {
			res.List = append(res.List, storage.ListObject{
				Name:  filepath.Base(path),
				IsDir: true,
			})
		} else {
			res.List = append(res.List, storage.ListObject{
				Name:         filepath.Base(path),
				IsDir:        false,
				Size:         info.Size(),
				Path:         fileKey,
				Url:          r.Url(fileKey),
				LastModified: info.ModTime(),
			})
		}
		res.NextToken = fileKey
		return err
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return res, nil
}

func (r *Local) AllDirectories(path string) ([]string, error) {
	var directories []string
	err := filepath.Walk(r.fullPath(path), func(fullPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			realPath := strings.ReplaceAll(fullPath, r.fullPath(path), "")
			realPath = strings.TrimPrefix(realPath, string(filepath.Separator))
			if realPath != "" {
				directories = append(directories, realPath+string(filepath.Separator))
			}
		}

		return nil
	})

	return directories, err
}

func (r *Local) AllFiles(path string) ([]string, error) {
	var files []string
	err := filepath.Walk(r.fullPath(path), func(fullPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, strings.ReplaceAll(fullPath, r.fullPath(path)+string(filepath.Separator), ""))
		}

		return nil
	})

	return files, err
}

func (r *Local) Copy(ctx context.Context, originFile, targetFile string) error {
	rs, err := r.GetStream(ctx, originFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = rs.Close()
	}()
	return r.PutStream(ctx, targetFile, rs)
}

func (r *Local) Delete(ctx context.Context, files ...string) error {
	for _, file := range files {
		fileInfo, err := os.Stat(r.fullPath(file))
		if err != nil {
			return err
		}

		if fileInfo.IsDir() {
			return errors.New("can't delete directory, please use DeleteDirectory")
		}
	}

	for _, file := range files {
		if err := os.Remove(r.fullPath(file)); err != nil {
			return err
		}
	}

	return nil
}

func (r *Local) DeleteDirectory(ctx context.Context, directory string) error {
	return os.RemoveAll(r.fullPath(directory))
}

func (r *Local) Directories(ctx context.Context, path string) ([]string, error) {
	var directories []string
	fileInfo, _ := os.ReadDir(r.fullPath(path))
	for _, f := range fileInfo {
		if f.IsDir() {
			directories = append(directories, f.Name()+string(filepath.Separator))
		}
	}

	return directories, nil
}

func (r *Local) Exists(ctx context.Context, file string) bool {
	_, err := os.Stat(r.fullPath(file))
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func (r *Local) Files(ctx context.Context, path string) ([]string, error) {
	var files []string
	fileInfo, err := os.ReadDir(r.fullPath(path))
	if err != nil {
		return nil, err
	}
	for _, f := range fileInfo {
		if !f.IsDir() {
			files = append(files, f.Name())
		}
	}

	return files, nil
}

func (r *Local) Get(ctx context.Context, file string) ([]byte, error) {
	rs, err := r.GetStream(ctx, file)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rs.Close()
	}()
	return io.ReadAll(rs)
}

func (r *Local) GetStream(ctx context.Context, file string) (io.ReadCloser, error) {
	return os.Open(r.fullPath(file))
}

func (r *Local) LastModified(ctx context.Context, file string) (time.Time, error) {
	return LastModified(r.fullPath(file))
}

func (r *Local) MakeDirectory(ctx context.Context, directory string) error {
	return os.MkdirAll(filepath.Dir(r.fullPath(directory)+string(filepath.Separator)), os.ModePerm)
}

func (r *Local) MimeType(ctx context.Context, file string) (string, error) {
	return MimeType(r.fullPath(file))
}

func (r *Local) Missing(ctx context.Context, file string) bool {
	return !r.Exists(ctx, file)
}

func (r *Local) Move(ctx context.Context, oldFile, newFile string) error {
	newFile = r.fullPath(newFile)
	if err := os.MkdirAll(filepath.Dir(newFile), os.ModePerm); err != nil {
		return err
	}
	if err := os.Rename(r.fullPath(oldFile), newFile); err != nil {
		return err
	}
	return nil
}

func (r *Local) Path(file string) string {
	return r.fullPath(file)
}

func (r *Local) Put(ctx context.Context, file string, content []byte) error {
	return r.PutStream(ctx, file, bytes.NewReader(content))
}

func (r *Local) PutStream(ctx context.Context, file string, rs io.Reader) error {
	file = r.fullPath(file)
	if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
		return err
	}
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	if _, err = io.Copy(f, rs); err != nil {
		return err
	}
	return nil
}

func (r *Local) Size(ctx context.Context, file string) (int64, error) {
	return Size(r.fullPath(file))
}

func (r *Local) Url(file string) string {
	return r.endpoint + "/" + strings.TrimPrefix(filepath.ToSlash(file), "/")
}

func (r *Local) fullPath(path string) string {
	realPath := filepath.Clean(path)
	if realPath == "." {
		return r.root
	}
	return filepath.Join(r.root, realPath)
}
