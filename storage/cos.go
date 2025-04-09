package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/opdss/common/contracts/storage"
	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/zeebo/errs"
)

/*
* Cos COS
* Document: https://cloud.tencent.com/document/product/436/31215
 */
var ErrCos = errs.Class("storage.cos")

type CosConfig struct {
	AccessKeyId     string `help:"accessKeyId" default:""  json:"access_key_id""`
	AccessKeySecret string `help:"accessKeySecret" default:""  json:"access_key_secret"`
	Bucket          string `help:"存储桶" default:"" json:"bucket"`
	Url             string `help:"访问地址" default:"" json:"url"`
	Endpoint        string `help:"api入口" default:"" json:"endpoint"`
}

var _ storage.FileSystem = (*Cos)(nil)

type Cos struct {
	config   CosConfig
	instance *cos.Client
}

func NewCos(config CosConfig) (*Cos, error) {
	if config.AccessKeyId == "" || config.AccessKeySecret == "" || config.Endpoint == "" {
		return nil, ErrCos.New("please set configuration")
	}

	u, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, ErrCos.Wrap(err)
	}

	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  config.AccessKeyId,
			SecretKey: config.AccessKeySecret,
		},
	})

	if config.Url == "" {
		config.Url = config.Endpoint
	}
	config.Url = strings.TrimSuffix(config.Url, "/")

	return &Cos{
		config:   config,
		instance: client,
	}, nil
}

func (r *Cos) ListObjects(ctx context.Context, opt *ListObjectOpts) (*ListObjectRes, error) {
	vPath := validPath(opt.Directory)
	v, _, err := r.instance.Bucket.Get(ctx, &cos.BucketGetOptions{
		Prefix:  vPath,
		Marker:  opt.NextToken,
		MaxKeys: int(getPageSize(opt.MaxKeys)),
	})
	res := &ListObjectRes{
		List: make([]ListObject, 0),
	}
	if err != nil {
		return res, err
	}
	for _, item := range v.Contents {
		res.HasMore = v.IsTruncated
		res.NextToken = v.NextMarker
		if strings.HasSuffix(item.Key, "/") {
			res.List = append(res.List, ListObject{
				Name:  strings.Trim(strings.ReplaceAll(item.Key, vPath, ""), "/"),
				IsDir: true,
			})
		} else {
			file := strings.ReplaceAll(item.Key, vPath, "")
			if file == "" {
				continue
			}
			t := time.Time{}
			if _t, _err := time.Parse(time.RFC3339, item.LastModified); _err == nil {
				t = _t
			}
			res.List = append(res.List, ListObject{
				Name:         file,
				IsDir:        false,
				Size:         item.Size,
				Path:         item.Key,
				Url:          r.Url(item.Key),
				LastModified: t,
			})
		}
	}
	return res, nil
}

func (r *Cos) Copy(ctx context.Context, originFile, targetFile string) error {
	originFile = strings.ReplaceAll(strings.ReplaceAll(strings.TrimSuffix(r.config.Endpoint, "/")+"/"+strings.TrimPrefix(originFile, "/"), "https://", ""), "http://", "")
	if _, _, err := r.instance.Object.Copy(ctx, targetFile, originFile, nil); err != nil {
		return err
	}

	return nil
}

func (r *Cos) Delete(ctx context.Context, files ...string) error {
	var obs []cos.Object
	for _, v := range files {
		obs = append(obs, cos.Object{Key: v})
	}
	opt := &cos.ObjectDeleteMultiOptions{
		Objects: obs,
		Quiet:   true,
	}
	if _, _, err := r.instance.Object.DeleteMulti(ctx, opt); err != nil {
		return err
	}
	return nil
}

func (r *Cos) DeleteDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}
	var marker string
	opt := &cos.BucketGetOptions{
		Prefix:  directory,
		MaxKeys: 1000,
	}
	isTruncated := true
	for isTruncated {
		opt.Marker = marker
		res, _, err := r.instance.Bucket.Get(ctx, opt)
		if err != nil {
			return err
		}
		if len(res.Contents) == 0 {
			return nil
		}

		for _, content := range res.Contents {
			_, err = r.instance.Object.Delete(ctx, content.Key)
			if err != nil {
				return err
			}
		}
		isTruncated = res.IsTruncated
		marker = res.NextMarker
	}

	return nil
}

func (r *Cos) Directories(ctx context.Context, path string) ([]string, error) {
	var directories []string
	var marker string
	vPath := validPath(path)
	opt := &cos.BucketGetOptions{
		Prefix:    vPath,
		Delimiter: "/",
		MaxKeys:   1000,
	}
	isTruncated := true
	for isTruncated {
		opt.Marker = marker
		v, _, err := r.instance.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}
		for _, commonPrefix := range v.CommonPrefixes {
			directories = append(directories, strings.ReplaceAll(commonPrefix, vPath, ""))
		}
		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}

	return directories, nil
}

func (r *Cos) Exists(ctx context.Context, file string) bool {
	ok, err := r.instance.Object.IsExist(ctx, file)
	if err != nil {
		return false
	}
	return ok
}

func (r *Cos) Files(ctx context.Context, path string) ([]string, error) {
	var files []string
	var marker string
	vPath := validPath(path)
	opt := &cos.BucketGetOptions{
		Prefix:    vPath,
		Delimiter: "/",
		MaxKeys:   1000,
	}
	isTruncated := true
	for isTruncated {
		opt.Marker = marker
		v, _, err := r.instance.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}
		for _, content := range v.Contents {
			files = append(files, strings.ReplaceAll(content.Key, vPath, ""))
		}
		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}

	return files, nil
}

func (r *Cos) Get(ctx context.Context, file string) ([]byte, error) {
	rs, err := r.GetStream(ctx, file)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rs.Close()
	}()
	return io.ReadAll(rs)
}

func (r *Cos) GetStream(ctx context.Context, file string) (io.ReadCloser, error) {
	opt := &cos.ObjectGetOptions{
		ResponseContentType: "text/html",
	}
	resp, err := r.instance.Object.Get(ctx, file, opt)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (r *Cos) LastModified(ctx context.Context, file string) (time.Time, error) {
	resp, err := r.instance.Object.Head(ctx, file, nil)
	if err != nil {
		return time.Time{}, err
	}
	return http.ParseTime(resp.Header.Get("Last-Modified"))
}

func (r *Cos) MakeDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}
	if _, err := r.instance.Object.Put(ctx, directory, strings.NewReader(""), nil); err != nil {
		return err
	}

	return nil
}

func (r *Cos) MimeType(ctx context.Context, file string) (string, error) {
	resp, err := r.instance.Object.Head(ctx, file, nil)
	if err != nil {
		return "", err
	}
	return resp.Header.Get("Content-Type"), nil
}

func (r *Cos) Missing(ctx context.Context, file string) bool {
	return !r.Exists(ctx, file)
}

func (r *Cos) Move(ctx context.Context, oldFile, newFile string) error {
	if err := r.Copy(ctx, oldFile, newFile); err != nil {
		return err
	}
	return r.Delete(ctx, oldFile)
}

func (r *Cos) Path(file string) string {
	return file
}

func (r *Cos) Put(ctx context.Context, file string, content []byte) error {
	return r.PutStream(ctx, file, bytes.NewReader(content))
}

func (r *Cos) PutStream(ctx context.Context, file string, rs io.Reader) error {
	_, err := r.instance.Object.Put(ctx, file, rs, nil)
	return err
}

//
//func (r *Cos) PutFileAs(filePath string, source filesystem.File, name string) (string, error) {
//	fullPath, err := fullPathOfFile(filePath, source, name)
//	if err != nil {
//		return "", err
//	}
//
//	if _, _, err := r.instance.Object.Upload(
//		r.ctx, fullPath, source.File(), nil,
//	); err != nil {
//		return "", err
//	}
//
//	return fullPath, nil
//}

func (r *Cos) Size(ctx context.Context, file string) (int64, error) {
	resp, err := r.instance.Object.Head(ctx, file, nil)
	if err != nil {
		return 0, err
	}
	contentLength := resp.Header.Get("Content-Length")
	contentLengthInt, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return 0, err
	}
	return contentLengthInt, nil
}

func (r *Cos) Url(file string) string {
	objectUrl := r.instance.Object.GetObjectURL(file)
	return objectUrl.String()
}
