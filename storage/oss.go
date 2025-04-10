package storage

import (
	"bytes"
	"context"
	"errors"
	"github.com/opdss/common/contracts/storage"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type OssConfig struct {
	AccessKeyId     string `help:"accessKeyId" default:""  json:"access_key_id""`
	AccessKeySecret string `help:"accessKeySecret" default:""  json:"access_key_secret"`

	RoleArn  string `help:"roleArn" default:"" json:"role_arn"`
	RegionId string `help:"regionId" default:"" json:"region_id"`

	Bucket   string `help:"存储桶" default:"" json:"bucket"`
	Url      string `help:"加速访问地址" default:"" json:"url"`
	Endpoint string `help:"api入口" default:"" json:"endpoint"`
}

var _ storage.FileSystem = (*Oss)(nil)

/*
 * Oss OSS
 * Document: https://help.aliyun.com/document_detail/32144.html
 */
type Oss struct {
	config         OssConfig
	bucketInstance *oss.Bucket
}

func NewOss(config OssConfig) (*Oss, error) {
	if config.AccessKeyId == "" || config.AccessKeySecret == "" || config.Bucket == "" || config.Endpoint == "" || config.Url == "" {
		return nil, errors.New("please set configuration")
	}

	client, err := oss.New(config.Endpoint, config.AccessKeyId, config.AccessKeySecret)
	if err != nil {
		return nil, err
	}

	bucketInstance, err := client.Bucket(config.Bucket)
	if err != nil {
		return nil, err
	}

	if config.Url == "" {
		config.Url = config.Endpoint
	}
	config.Url = strings.TrimSuffix(config.Url, "/")
	return &Oss{
		config:         config,
		bucketInstance: bucketInstance,
	}, nil
}

func (r *Oss) ListObjects(ctx context.Context, opt *storage.ListObjectOpts) (*storage.ListObjectRes, error) {
	res := storage.ListObjectRes{
		List: make([]storage.ListObject, 0),
	}
	vPath := validPath(opt.Directory)
	listObjsResponse, err := r.bucketInstance.ListObjectsV2(oss.Prefix(vPath), oss.MaxKeys(int(getPageSize(opt.MaxKeys))), oss.ContinuationToken(opt.NextToken), oss.Delimiter("/"))
	if err != nil {
		return nil, err
	}
	if listObjsResponse.NextContinuationToken != "" {
		res.NextToken = listObjsResponse.NextContinuationToken
	}
	res.HasMore = listObjsResponse.IsTruncated
	for _, object := range listObjsResponse.Objects {
		if object.Type == "Symlink" {
			res.List = append(res.List, storage.ListObject{
				Name:  strings.Trim(strings.ReplaceAll(object.Key, vPath, ""), "/"),
				IsDir: true,
			})
		} else {
			file := strings.ReplaceAll(object.Key, vPath, "")
			if file == "" {
				continue
			}
			res.List = append(res.List, storage.ListObject{
				Name:         file,
				IsDir:        false,
				Size:         object.Size,
				Path:         object.Key,
				Url:          r.Url(object.Key),
				LastModified: object.LastModified,
			})
		}
	}
	return &res, nil
}

func (r *Oss) Copy(ctx context.Context, originFile, targetFile string) error {
	if _, err := r.bucketInstance.CopyObject(originFile, targetFile); err != nil {
		return err
	}
	return nil
}

func (r *Oss) Delete(ctx context.Context, files ...string) error {
	_, err := r.bucketInstance.DeleteObjects(files)
	if err != nil {
		return err
	}
	return nil
}

func (r *Oss) DeleteDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}
	marker := oss.Marker("")
	prefix := oss.Prefix(directory)
	for {
		lor, err := r.bucketInstance.ListObjects(marker, prefix)
		if err != nil {
			return err
		}
		if len(lor.Objects) == 0 {
			return nil
		}

		var objects []string
		for _, object := range lor.Objects {
			objects = append(objects, object.Key)
		}

		if _, err := r.bucketInstance.DeleteObjects(objects, oss.DeleteObjectsQuiet(true)); err != nil {
			return err
		}

		prefix = oss.Prefix(lor.Prefix)
		marker = oss.Marker(lor.NextMarker)
		if !lor.IsTruncated {
			break
		}
	}

	return nil
}

func (r *Oss) Directories(ctx context.Context, path string) ([]string, error) {
	var directories []string
	vPath := validPath(path)
	lsRes, err := r.bucketInstance.ListObjectsV2(oss.MaxKeys(storage.MaxFileNum), oss.Prefix(vPath), oss.Delimiter("/"))
	if err != nil {
		return nil, err
	}
	for _, directory := range lsRes.CommonPrefixes {
		directories = append(directories, strings.ReplaceAll(directory, vPath, ""))
	}
	return directories, nil
}

func (r *Oss) Exists(ctx context.Context, file string) bool {
	exist, err := r.bucketInstance.IsObjectExist(file)
	if err != nil {
		return false
	}
	return exist
}

func (r *Oss) Files(ctx context.Context, path string) ([]string, error) {
	var files []string
	vPath := validPath(path)
	lsRes, err := r.bucketInstance.ListObjectsV2(oss.MaxKeys(storage.MaxFileNum), oss.Prefix(vPath), oss.Delimiter("/"))
	if err != nil {
		return nil, err
	}
	for _, object := range lsRes.Objects {
		files = append(files, strings.ReplaceAll(object.Key, vPath, ""))
	}
	return files, nil
}

func (r *Oss) Get(ctx context.Context, file string) ([]byte, error) {
	rs, err := r.GetStream(ctx, file)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rs.Close()
	}()
	return io.ReadAll(rs)
}

func (r *Oss) GetStream(ctx context.Context, file string) (io.ReadCloser, error) {
	return r.bucketInstance.GetObject(file)
}

func (r *Oss) LastModified(ctx context.Context, file string) (time.Time, error) {
	headers, err := r.bucketInstance.GetObjectDetailedMeta(file)
	if err != nil {
		return time.Time{}, err
	}
	return http.ParseTime(headers.Get("Last-Modified"))
}

func (r *Oss) MakeDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	return r.bucketInstance.PutObject(directory, bytes.NewReader([]byte("")))
}

func (r *Oss) MimeType(ctx context.Context, file string) (string, error) {
	headers, err := r.bucketInstance.GetObjectDetailedMeta(file)
	if err != nil {
		return "", err
	}
	return headers.Get("Content-Type"), nil
}

func (r *Oss) Missing(ctx context.Context, file string) bool {
	return !r.Exists(ctx, file)
}

func (r *Oss) Move(ctx context.Context, oldFile, newFile string) error {
	if err := r.Copy(ctx, oldFile, newFile); err != nil {
		return err
	}
	return r.Delete(ctx, oldFile)
}

func (r *Oss) Path(file string) string {
	return file
}

func (r *Oss) Put(ctx context.Context, file string, content []byte) error {
	return r.bucketInstance.PutObject(file, bytes.NewReader(content))
}

func (r *Oss) PutStream(ctx context.Context, file string, rs io.Reader) error {
	return r.bucketInstance.PutObject(file, rs)
}

func (r *Oss) Size(ctx context.Context, file string) (int64, error) {
	props, err := r.bucketInstance.GetObjectDetailedMeta(file)
	if err != nil {
		return 0, err
	}
	lens := props["Content-Length"]
	if len(lens) == 0 {
		return 0, nil
	}
	contentLengthInt, err := strconv.ParseInt(lens[0], 10, 64)
	if err != nil {
		return 0, err
	}
	return contentLengthInt, nil
}

func (r *Oss) Url(file string) string {
	return r.config.Url + "/" + file
}

func validPath(path string) string {
	realPath := strings.TrimPrefix(path, "./")
	realPath = strings.TrimPrefix(realPath, "/")
	realPath = strings.TrimPrefix(realPath, ".")
	if realPath != "" && !strings.HasSuffix(realPath, "/") {
		realPath += "/"
	}
	return realPath
}
