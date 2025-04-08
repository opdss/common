package storage

import (
	"context"
	"io"
	"time"
)

const DefaultFileNum = 50
const MaxFileNum = 1000

type ListObject struct {
	Name         string    `json:"name"`   //文件名
	IsDir        bool      `json:"is_dir"` //是否是目录
	Url          string    `json:"url"`    //文件地址
	Size         int64     `json:"size"`   //文件大小
	Path         string    `json:"path"`   //文件路径
	MIME         string    `json:"mime"`
	LastModified time.Time `json:"last_modified"`
}

type ListObjectRes struct {
	List      []ListObject `json:"list"`
	NextToken string       `json:"next_token"`
	HasMore   bool         `json:"has_more"`
}

type ListObjectOpts struct {
	Directory string `json:"directory"`  //文件目录
	NextToken string `json:"next_token"` //下一页开始位置
	MaxKeys   int32  `json:"max_keys"`   //分页大小
	Prefix    string `json:"prefix"`     //文件前缀
}

type Credentials struct {
	AccessKeySecret string `json:"AccessKeySecret"`
	Expiration      string `json:"Expiration"`
	AccessKeyId     string `json:"AccessKeyId"`
	SecurityToken   string `json:"SecurityToken"`
}

type FileSystem interface {
	ListObjects(ctx context.Context, opt *ListObjectOpts) (*ListObjectRes, error)

	Copy(ctx context.Context, oldFile, newFile string) error
	// Delete deletes the given file(s).
	Delete(ctx context.Context, file ...string) error
	// DeleteDirectory deletes the given directory(recursive).
	DeleteDirectory(ctx context.Context, directory string) error
	// Directories get all the directories within a given directory.
	Directories(ctx context.Context, path string) ([]string, error)
	// Exists determines if a file exists.
	Exists(ctx context.Context, file string) bool
	// Files gets all the files from the given directory.
	Files(ctx context.Context, path string) ([]string, error)
	// Get gets the contents of a file.
	Get(ctx context.Context, file string) ([]byte, error)
	GetStream(ctx context.Context, file string) (io.ReadCloser, error)
	// LastModified gets the file's last modified time.
	LastModified(ctx context.Context, file string) (time.Time, error)
	// MakeDirectory creates a directory.
	MakeDirectory(ctx context.Context, directory string) error
	// MimeType gets the file's mime type.
	MimeType(ctx context.Context, file string) (string, error)
	// Missing determines if a file is missing.
	Missing(ctx context.Context, file string) bool
	// Move a file to a new location.
	Move(ctx context.Context, oldFile, newFile string) error
	// Path gets the full path for the file.
	Path(file string) string
	// Put writes the contents of a file.
	Put(ctx context.Context, file string, content []byte) error
	PutStream(ctx context.Context, file string, rs io.Reader) error
	// Size gets the file size of a given file.
	Size(ctx context.Context, file string) (int64, error)
	// Url get the URL for the file at the given path.
	Url(file string) string
}
