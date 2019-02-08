package s3concat

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

const (
	MaxObjects         = 1000
	MultipartThreshold = 5 * 1024 * 1024 // 5MB
)

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
		log.Printf("[debug] %d objects %d bytes at %s", len(ls), size, srcObject)
		list = append(list, ls...)
		totalSize += size
	}
	if n := len(list); n > MaxObjects {
		return fmt.Errorf("too many objects %d > %d", n, MaxObjects)
	}
	if totalSize >= MultipartThreshold {
		return concatByMultipart(ctx, svc, list, dstObj)
	}
	return concatByUpload(ctx, svc, list, dstObj)
}

func listObjects(ctx context.Context, svc *s3.S3, o *S3Object) ([]*S3Object, int64, error) {
	log.Printf("[debug] listObjects %s", o)
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
		log.Printf("[debug] found object %s %d bytes", so, so.Size)
		objs = append(objs, so)
		totalSize += *c.Size
	}
	return objs, totalSize, nil
}

func concatByUpload(ctx context.Context, svc *s3.S3, objs []*S3Object, dst *S3Object) error {
	var size int64
	for _, obj := range objs {
		size += obj.Size
	}
	content := make([]byte, 0, size)
	for _, obj := range objs {
		part, err := getObjectContent(ctx, svc, obj, 0, obj.Size-1)
		if err != nil {
			return err
		}
		content = append(content, part...)
	}
	log.Printf("[info] putObject %d bytes -> %s", len(content), dst)
	_, err := svc.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(dst.Bucket),
			Key:    aws.String(dst.Key),
			Body:   bytes.NewReader(content),
		},
	)
	return err
}

func updateParts(parts []*s3.CompletedPart, etag string, num int64) []*s3.CompletedPart {
	parts = append(parts, &s3.CompletedPart{
		ETag:       aws.String(etag),
		PartNumber: aws.Int64(num),
	})
	log.Printf("[debug] num: %d, ETag: %s", num, etag)
	return parts
}

func concatByMultipart(ctx context.Context, svc *s3.S3, objs []*S3Object, dst *S3Object) error {
	log.Printf("[debug] starting multipart upload to %s", dst)
	mpu, err := svc.CreateMultipartUploadWithContext(
		ctx,
		&s3.CreateMultipartUploadInput{
			Bucket: aws.String(dst.Bucket),
			Key:    aws.String(dst.Key),
		},
	)
	if err != nil {
		return err
	}
	log.Printf("[info] begin multipart upload id %s", *mpu.UploadId)

	parts := make([]*s3.CompletedPart, 0, len(objs))
	var currentPart []byte
	var num int64
	var uploadsErr error
UPLOADS:
	for i, obj := range objs {
		log.Printf("[debug] currentPart %d bytes", len(currentPart))
		if len(currentPart) == 0 && (obj.Size >= MultipartThreshold || i == len(objs)-1) {
			// single object > 5MB or last part
			etag, err := uploadPartCopy(ctx, svc, obj, dst, *mpu.UploadId, &num, 0, obj.Size-1)
			if err != nil {
				uploadsErr = err
				break UPLOADS
			}
			parts = updateParts(parts, etag, num)
			continue
		}

		restSize := int64(MultipartThreshold - len(currentPart))
		if obj.Size <= restSize || obj.Size-restSize < MultipartThreshold {
			// whole of object
			log.Printf("[debug] whole obj.Size=%d, restSize=%d", obj.Size, restSize)
			content, err := getObjectContent(ctx, svc, obj, 0, obj.Size-1)
			if err != nil {
				uploadsErr = err
				break UPLOADS
			}
			currentPart = append(currentPart, content...)
			if len(currentPart) >= MultipartThreshold {
				etag, err := uploadPart(ctx, svc, currentPart, dst, *mpu.UploadId, &num)
				if err != nil {
					uploadsErr = err
					break UPLOADS
				}
				parts = updateParts(parts, etag, num)
				currentPart = currentPart[0:0] // reset
			}
			continue
		}

		// overflow, object head
		log.Printf("[debug] overflow get head obj.Size=%d, restSize=%d", obj.Size, restSize)
		content, err := getObjectContent(ctx, svc, obj, 0, restSize-1)
		if err != nil {
			uploadsErr = err
			break UPLOADS
		}
		currentPart = append(currentPart, content...)
		if len(currentPart) != MultipartThreshold {
			panic("unexpected currentPart size")
		}
		etag, err := uploadPart(ctx, svc, currentPart, dst, *mpu.UploadId, &num)
		if err != nil {
			uploadsErr = err
			break UPLOADS
		}
		parts = updateParts(parts, etag, num)
		currentPart = currentPart[0:0] // reset

		// object tail (must be larger than MultipartThreshold)
		log.Println("[debug] overflow tail")
		etag, err = uploadPartCopy(ctx, svc, obj, dst, *mpu.UploadId, &num, restSize, obj.Size-1)
		if err != nil {
			uploadsErr = err
			break UPLOADS
		}
		parts = updateParts(parts, etag, num)
	}

	// Last part is allowed to be smaller than MultipartThreshold
	if len(currentPart) > 0 {
		etag, err := uploadPart(ctx, svc, currentPart, dst, *mpu.UploadId, &num)
		if err != nil {
			uploadsErr = err
		} else {
			parts = updateParts(parts, etag, num)
		}
	}

	if uploadsErr != nil {
		log.Printf("[error] %s abort multipart upload", uploadsErr)
		_, err := svc.AbortMultipartUploadWithContext(
			ctx,
			&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(dst.Bucket),
				Key:      aws.String(dst.Key),
				UploadId: mpu.UploadId,
			},
		)
		if err != nil {
			log.Println("[warn]", err)
		}
		return uploadsErr
	}

	// sort parts by part number
	// If those are not sorted,
	// > InvalidPartOrder: The list of parts was not in ascending order. Parts must be ordered by part number.
	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})

	log.Printf("[info] complete multipart upload id %s", *mpu.UploadId)
	_, err = svc.CompleteMultipartUploadWithContext(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(dst.Bucket),
			Key:             aws.String(dst.Key),
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
			UploadId:        mpu.UploadId,
		},
	)
	return err
}

func uploadPartCopy(ctx context.Context, svc *s3.S3, from, to *S3Object, uploadId string, num *int64, start, end int64) (string, error) {
	*num++
	log.Printf("[debug] uploadPartCopy %s(%d-%d) -> %s (part %d)", from, start, end, to, *num)
	input := &s3.UploadPartCopyInput{
		Bucket:          aws.String(to.Bucket),
		Key:             aws.String(to.Key),
		CopySource:      aws.String(from.Bucket + "/" + from.Key),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		PartNumber:      aws.Int64(*num),
		UploadId:        aws.String(uploadId),
	}
	res, err := svc.UploadPartCopyWithContext(ctx, input)
	if err != nil {
		return "", errors.Wrapf(err, "UploadPart failed: %s %s (part %d)", err, to, *num)
	} else {
		log.Printf("[info] UploadPartCopy completed: %s (part %d)", to, *num)
	}

	return *res.CopyPartResult.ETag, nil
}

func uploadPart(ctx context.Context, svc *s3.S3, b []byte, to *S3Object, uploadId string, num *int64) (string, error) {
	*num++
	log.Printf("[debug] uploadPart %d bytes -> %s (part %d)", len(b), to, *num)
	input := &s3.UploadPartInput{
		Body:       bytes.NewReader(b),
		Bucket:     aws.String(to.Bucket),
		Key:        aws.String(to.Key),
		PartNumber: aws.Int64(*num),
		UploadId:   aws.String(uploadId),
	}
	res, err := svc.UploadPartWithContext(ctx, input)
	if err != nil {
		return "", errors.Wrapf(err, "UploadPart failed: %s %s (part%d)", err, to, *num)
	} else {
		log.Printf("[info] UploadPart completed: %s (part %d)", to, *num)
	}
	return *res.ETag, nil
}

func getObjectContent(ctx context.Context, svc *s3.S3, obj *S3Object, start, end int64) ([]byte, error) {
	log.Printf("[debug] getObjectContent %s(%d-%d)", obj, start, end)
	input := &s3.GetObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	input = input.SetResponseContentEncoding("") // read as raw
	res, err := svc.GetObject(input)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(res.Body) // at most 5MB
}
