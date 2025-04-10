package storage

import (
	"bytes"
	"context"
	"errors"
	"github.com/opdss/common/contracts/storage"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gabriel-vasile/mimetype"
)

/*
* S3 OSS
* Document: https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/gov2/s3
* More: https://aws.github.io/aws-sdk-go-v2/docs/sdk-utilities/s3/#putobjectinput-body-field-ioreadseeker-vs-ioreader
 */

type S3Config struct {
	AccessKeyId     string `help:"accessKeyId" default:""`
	AccessKeySecret string `help:"accessKeySecret" default:""`
	RoleArn         string `help:"roleArn" default:"" json:"role_arn"`
	Bucket          string `help:"存储桶" default:""`
	Region          string `help:"地区" default:""`
	Url             string `help:"访问地址" default:""`
	Endpoint        string `help:"api入口" default:""`
}

var _ storage.FileSystem = (*S3)(nil)

type S3 struct {
	config   S3Config
	instance *s3.Client
}

func NewS3(config S3Config) (*S3, error) {
	if config.AccessKeyId == "" || config.AccessKeySecret == "" || config.Endpoint == "" {
		return nil, errors.New("please set configuration")
	}

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: config.Endpoint,
		}, nil
	})

	cfg, _ := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithEndpointResolverWithOptions(r2Resolver),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.AccessKeyId, config.AccessKeySecret, "")),
		awsConfig.WithRegion(config.Region),
	)

	if config.Url == "" {
		config.Url = config.Endpoint
	}
	config.Url = strings.TrimSuffix(config.Url, "/")

	client := s3.NewFromConfig(cfg)
	return &S3{
		config: config,
		//instance: client,
		instance: client,
	}, nil
}

func (r *S3) ListObjects(ctx context.Context, opt *storage.ListObjectOpts) (*storage.ListObjectRes, error) {
	res := storage.ListObjectRes{
		List: make([]storage.ListObject, 0),
	}
	vPath := validPath(opt.Directory)
	var continuationToken *string
	if opt.NextToken != "" {
		continuationToken = aws.String(opt.NextToken)
	}
	listObjsResponse, err := r.instance.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(r.config.Bucket),
		Delimiter:         aws.String("/"),
		Prefix:            aws.String(vPath + opt.Prefix),
		MaxKeys:           aws.Int32(getPageSize(opt.MaxKeys)),
		ContinuationToken: continuationToken,
	})
	if err != nil {
		return nil, err
	}
	if listObjsResponse.NextContinuationToken != nil {
		res.NextToken = *listObjsResponse.NextContinuationToken
	}
	res.HasMore = *listObjsResponse.IsTruncated
	for _, object := range listObjsResponse.CommonPrefixes {
		name := strings.Trim(strings.ReplaceAll(*object.Prefix, vPath, ""), "/")
		if name == "" {
			continue
		}
		res.List = append(res.List, storage.ListObject{
			Name:  name,
			IsDir: true,
		})
	}
	for _, object := range listObjsResponse.Contents {
		file := strings.ReplaceAll(*object.Key, vPath, "")
		if file == "" {
			continue
		}
		res.List = append(res.List, storage.ListObject{
			Name:         file,
			IsDir:        false,
			Size:         *object.Size,
			Path:         *object.Key,
			Url:          r.Url(*object.Key),
			LastModified: *object.LastModified,
		})
	}
	return &res, nil
}

func (r *S3) Copy(ctx context.Context, originFile, targetFile string) error {
	_, err := r.instance.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(r.config.Bucket),
		CopySource: aws.String(r.config.Bucket + "/" + originFile),
		Key:        aws.String(targetFile),
	})
	return err
}

func (r *S3) Delete(ctx context.Context, files ...string) error {
	var objectIdentifiers []types.ObjectIdentifier
	for _, file := range files {
		objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{
			Key: aws.String(file),
		})
	}

	_, err := r.instance.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(r.config.Bucket),
		Delete: &types.Delete{
			Objects: objectIdentifiers,
			Quiet:   aws.Bool(true),
		},
	})
	return err
}

func (r *S3) DeleteDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	listObjectsV2Response, err := r.instance.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.config.Bucket),
		Prefix: aws.String(directory),
	})
	if err != nil {
		return err
	}
	if len(listObjectsV2Response.Contents) == 0 {
		return nil
	}

	for {
		for _, item := range listObjectsV2Response.Contents {
			_, err = r.instance.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(r.config.Bucket),
				Key:    item.Key,
			})
			if err != nil {
				return err
			}
		}

		if *listObjectsV2Response.IsTruncated {
			listObjectsV2Response, err = r.instance.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(r.config.Bucket),
				ContinuationToken: listObjectsV2Response.ContinuationToken,
			})
			if err != nil {
				return err
			}
		} else {
			break
		}
	}

	return nil
}

func (r *S3) Directories(ctx context.Context, path string) ([]string, error) {
	var directories []string
	validPath := validPath(path)
	listObjsResponse, err := r.instance.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(r.config.Bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(validPath),
	})
	if err != nil {
		return nil, err
	}
	for _, commonPrefix := range listObjsResponse.CommonPrefixes {
		directories = append(directories, strings.ReplaceAll(*commonPrefix.Prefix, validPath, ""))
	}

	return directories, nil
}

func (r *S3) Exists(ctx context.Context, file string) bool {
	_, err := r.instance.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
	})

	return err == nil
}

func (r *S3) Files(ctx context.Context, path string) ([]string, error) {
	var files []string
	validPath := validPath(path)
	listObjsResponse, err := r.instance.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(r.config.Bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(validPath),
	})
	if err != nil {
		return nil, err
	}
	for _, object := range listObjsResponse.Contents {
		file := strings.ReplaceAll(*object.Key, validPath, "")
		if file == "" {
			continue
		}

		files = append(files, file)
	}

	return files, nil
}

func (r *S3) Get(ctx context.Context, file string) ([]byte, error) {
	rs, err := r.GetStream(ctx, file)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rs.Close()
	}()
	return io.ReadAll(rs)
}

func (r *S3) GetStream(ctx context.Context, file string) (io.ReadCloser, error) {
	resp, err := r.instance.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (r *S3) LastModified(ctx context.Context, file string) (time.Time, error) {
	resp, err := r.instance.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
	})
	if err != nil {
		return time.Time{}, err
	}
	return aws.ToTime(resp.LastModified), nil
}

func (r *S3) MakeDirectory(ctx context.Context, directory string) error {
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}
	if directory == "/" || directory == "./" {
		return nil
	}
	return r.Put(ctx, directory, []byte{})
}

func (r *S3) MimeType(ctx context.Context, file string) (string, error) {
	resp, err := r.instance.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(resp.ContentType), nil
}

func (r *S3) Missing(ctx context.Context, file string) bool {
	return !r.Exists(ctx, file)
}

func (r *S3) Move(ctx context.Context, oldFile, newFile string) error {
	if err := r.Copy(ctx, oldFile, newFile); err != nil {
		return err
	}
	return r.Delete(ctx, oldFile)
}

func (r *S3) Path(file string) string {
	return strings.TrimPrefix(file, "/")
}

func (r *S3) Put(ctx context.Context, file string, content []byte) error {
	file = r.Path(file)
	if ext := filepath.Ext(file); ext != "" {
		if err := r.MakeDirectory(ctx, filepath.Dir(file)); err != nil {
			return err
		}
	}
	mtype := mimetype.Detect(content)
	_, err := r.instance.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(r.config.Bucket),
		Key:           aws.String(file),
		Body:          bytes.NewReader(content),
		ContentLength: aws.Int64(int64(len(content))),
		ContentType:   aws.String(mtype.String()),
	})
	return err
}

func (r *S3) PutStream(ctx context.Context, file string, rs io.Reader) error {
	file = r.Path(file)
	ext := filepath.Ext(file)
	if ext != "" {
		if err := r.MakeDirectory(ctx, filepath.Dir(file)); err != nil {
			return err
		}
	}
	// var content []byte
	content, _ := io.ReadAll(rs)
	mtype := mimetype.Detect(content)
	contentType := aws.String(mtype.String())
	if strings.ToLower(strings.Trim(ext, ".")) == "apk" {
		contentType = aws.String("application/vnd.android.package-archive")
	}
	_, err := r.instance.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
		Body:   bytes.NewReader(content),

		ContentLength: aws.Int64(int64(len(content))),
		ContentType:   contentType,
	})
	return err
}

func (r *S3) Size(ctx context.Context, file string) (int64, error) {
	resp, err := r.instance.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(file),
	})
	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func (r *S3) Url(file string) string {
	return r.config.Url + "/" + strings.TrimPrefix(file, "/")
}
