package s3concat

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

const (
	MaxObjects         = 1000
	MultipartThreshold = 5 * 1024 * 1024 // 5MB
)

var logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

type S3Object struct {
	Bucket string
	Key    string
	Size   int64
}

func (o *S3Object) String() string {
	return fmt.Sprintf("s3://%s/%s", o.Bucket, o.Key)
}

func ParseS3URL(str string) (*S3Object, error) {
	u, err := url.Parse(str)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid s3 url: %s", str)
	}
	return &S3Object{
		Bucket: u.Host,
		Key:    strings.TrimLeft(u.Path, "/"),
	}, nil
}

type Concatenator struct {
	svc        *s3.S3
	uploadID   string
	partNumber int64
	parts      []*s3.CompletedPart
}

func init() {
	level := os.Getenv("S3CONCAT_LOG_LEVEL")
	if level == "" {
		level = "warn"
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "warn", "error"},
		MinLevel: logutils.LogLevel(level),
		Writer:   os.Stderr,
	}
	logger.SetOutput(filter)
}

func Concat(sess *session.Session, srcs []string, dst string) error {
	return ConcatWithContext(context.Background(), sess, srcs, dst)
}

func ConcatWithContext(ctx context.Context, sess *session.Session, srcs []string, dst string) error {
	svc := s3.New(sess)
	dstObj, err := ParseS3URL(dst)
	if err != nil {
		return err
	}
	if strings.HasSuffix(dstObj.Key, "/") {
		return errors.New("destination object must not be a directory")
	}

	list := make([]*S3Object, 0)
	var totalSize int64
	for _, src := range srcs {
		srcObject, err := ParseS3URL(src)
		if err != nil {
			return err
		}
		ls, size, err := listObjects(ctx, svc, srcObject)
		if err != nil {
			return err
		}
		if len(ls) == 0 {
			return fmt.Errorf("object not found at %s", srcObject)
		}
		logger.Printf("[debug] %d objects %d bytes at %s", len(ls), size, srcObject)
		list = append(list, ls...)
		totalSize += size
	}
	if n := len(list); n > MaxObjects {
		return fmt.Errorf("too many objects %d > %d", n, MaxObjects)
	}

	c := &Concatenator{
		svc:        s3.New(sess),
		partNumber: 0,
	}
	if totalSize >= MultipartThreshold {
		return c.Multipart(ctx, list, dstObj)
	} else {
		return c.Upload(ctx, list, dstObj)
	}
}

func listObjects(ctx context.Context, svc *s3.S3, o *S3Object) ([]*S3Object, int64, error) {
	logger.Printf("[debug] listObjects %s", o)
	res, err := svc.ListObjectsWithContext(
		ctx,
		&s3.ListObjectsInput{
			Bucket:  aws.String(o.Bucket),
			Prefix:  aws.String(o.Key),
			MaxKeys: aws.Int64(MaxObjects + 1),
		},
	)
	if err != nil {
		return nil, 0, err
	}
	if n := len(res.Contents); n > MaxObjects {
		return nil, 0, fmt.Errorf("too many objects %d > %d", n, MaxObjects)
	}

	var totalSize int64
	objs := make([]*S3Object, 0, len(res.Contents))
	for _, c := range res.Contents {
		so := &S3Object{
			Bucket: o.Bucket,
			Key:    *c.Key,
			Size:   *c.Size,
		}
		logger.Printf("[debug] found object %s %d bytes", so, so.Size)
		objs = append(objs, so)
		totalSize += *c.Size
	}
	return objs, totalSize, nil
}

func (c *Concatenator) Upload(ctx context.Context, src []*S3Object, dest *S3Object) error {
	var size int64
	for _, obj := range src {
		size += obj.Size
	}
	content := make([]byte, 0, size)
	for _, obj := range src {
		part, err := c.getObjectContent(ctx, obj, 0, obj.Size-1)
		if err != nil {
			return err
		}
		content = append(content, part...)
	}
	logger.Printf("[info] putObject: %s size: %d", dest, len(content))
	_, err := c.svc.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(dest.Bucket),
			Key:    aws.String(dest.Key),
			Body:   bytes.NewReader(content),
		},
	)
	return err
}

func (c *Concatenator) updateParts(etag string) error {
	c.parts = append(c.parts, &s3.CompletedPart{
		ETag:       aws.String(etag),
		PartNumber: aws.Int64(c.partNumber),
	})
	logger.Printf("[debug] num: %d, ETag: %s", c.partNumber, etag)
	return nil
}

func (c *Concatenator) Multipart(ctx context.Context, src []*S3Object, dest *S3Object) error {
	logger.Printf("[debug] starting multipart upload to %s", dest)
	mpu, err := c.svc.CreateMultipartUploadWithContext(
		ctx,
		&s3.CreateMultipartUploadInput{
			Bucket: aws.String(dest.Bucket),
			Key:    aws.String(dest.Key),
		},
	)
	if err != nil {
		return err
	}
	c.uploadID = *mpu.UploadId
	logger.Printf("[info] begin multipart upload: %s id: %s", dest, c.uploadID)

	var currentPart []byte
	var uploadsErr error
UPLOADS:
	for i, obj := range src {
		logger.Printf("[debug] currentPart %d bytes", len(currentPart))
		if len(currentPart) == 0 && (obj.Size >= MultipartThreshold || i == len(src)-1) {
			// single object > 5MB or last part
			if err := c.uploadPartCopy(ctx, obj, dest, 0, obj.Size-1); err != nil {
				uploadsErr = err
				break UPLOADS
			}
			continue
		}

		restSize := int64(MultipartThreshold - len(currentPart))
		if obj.Size <= restSize || obj.Size-restSize < MultipartThreshold {
			// whole of object
			logger.Printf("[debug] whole obj.Size=%d, restSize=%d", obj.Size, restSize)
			content, err := c.getObjectContent(ctx, obj, 0, obj.Size-1)
			if err != nil {
				uploadsErr = err
				break UPLOADS
			}
			currentPart = append(currentPart, content...)
			if len(currentPart) >= MultipartThreshold {
				if err := c.uploadPart(ctx, currentPart, dest); err != nil {
					uploadsErr = err
					break UPLOADS
				}
				currentPart = currentPart[0:0] // reset
			}
			continue
		}

		// overflow, object head
		logger.Printf("[debug] overflow get head obj.Size=%d, restSize=%d", obj.Size, restSize)
		content, err := c.getObjectContent(ctx, obj, 0, restSize-1)
		if err != nil {
			uploadsErr = err
			break UPLOADS
		}
		currentPart = append(currentPart, content...)
		if len(currentPart) != MultipartThreshold {
			panic("unexpected currentPart size")
		}
		if err := c.uploadPart(ctx, currentPart, dest); err != nil {
			uploadsErr = err
			break UPLOADS
		}
		currentPart = currentPart[0:0] // reset

		// object tail (must be larger than MultipartThreshold)
		logger.Println("[debug] overflow tail")
		if err := c.uploadPartCopy(ctx, obj, dest, restSize, obj.Size-1); err != nil {
			uploadsErr = err
			break UPLOADS
		}
	}

	// Last part is allowed to be smaller than MultipartThreshold
	if len(currentPart) > 0 {
		if err := c.uploadPart(ctx, currentPart, dest); err != nil {
			uploadsErr = err
		}
	}

	if uploadsErr != nil {
		logger.Printf("[error] %s abort multipart upload", uploadsErr)
		_, err := c.svc.AbortMultipartUploadWithContext(
			ctx,
			&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(dest.Bucket),
				Key:      aws.String(dest.Key),
				UploadId: aws.String(c.uploadID),
			},
		)
		if err != nil {
			logger.Println("[warn]", err)
		}
		return uploadsErr
	}

	// sort parts by part number
	// If those are not sorted,
	// > InvalidPartOrder: The list of parts was not in ascending order. Parts must be ordered by part number.
	sort.Slice(c.parts, func(i, j int) bool {
		return *c.parts[i].PartNumber < *c.parts[j].PartNumber
	})

	logger.Printf("[info] complete multipart upload: %s id: %s", dest, *mpu.UploadId)
	_, err = c.svc.CompleteMultipartUploadWithContext(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(dest.Bucket),
			Key:             aws.String(dest.Key),
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: c.parts},
			UploadId:        aws.String(c.uploadID),
		},
	)
	return err
}

func (c *Concatenator) uploadPartCopy(ctx context.Context, from, to *S3Object, start, end int64) error {
	c.partNumber++
	logger.Printf("[debug] uploadPartCopy %s(%d-%d) -> %s (part %d)", from, start, end, to, c.partNumber)
	input := &s3.UploadPartCopyInput{
		Bucket:          aws.String(to.Bucket),
		Key:             aws.String(to.Key),
		CopySource:      aws.String(from.Bucket + "/" + from.Key),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		PartNumber:      aws.Int64(c.partNumber),
		UploadId:        aws.String(c.uploadID),
	}
	res, err := c.svc.UploadPartCopyWithContext(ctx, input)
	if err != nil {
		return errors.Wrapf(err, "uploadPartCopy failed: %s %s (part %d)", err, to, c.partNumber)
	} else {
		logger.Printf("[info] uploadPartCopy completed: %s (part %d)", to, c.partNumber)
	}
	return c.updateParts(*res.CopyPartResult.ETag)
}

func (c *Concatenator) uploadPart(ctx context.Context, b []byte, to *S3Object) error {
	c.partNumber++
	logger.Printf("[debug] uploadPart %d bytes -> %s (part %d)", len(b), to, c.partNumber)
	input := &s3.UploadPartInput{
		Body:       bytes.NewReader(b),
		Bucket:     aws.String(to.Bucket),
		Key:        aws.String(to.Key),
		PartNumber: aws.Int64(c.partNumber),
		UploadId:   aws.String(c.uploadID),
	}
	res, err := c.svc.UploadPartWithContext(ctx, input)
	if err != nil {
		return errors.Wrapf(err, "uploadPart failed: %s %s (part%d)", err, to, c.partNumber)
	} else {
		logger.Printf("[info] uploadPart completed: %s (part %d)", to, c.partNumber)
	}
	return c.updateParts(*res.ETag)
}

func (c *Concatenator) getObjectContent(ctx context.Context, obj *S3Object, start, end int64) ([]byte, error) {
	logger.Printf("[debug] getObjectContent %s(%d-%d)", obj, start, end)
	input := &s3.GetObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	input = input.SetResponseContentEncoding("") // read as raw
	res, err := c.svc.GetObject(input)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return ioutil.ReadAll(res.Body) // at most 5MB
}
