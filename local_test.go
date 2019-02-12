package s3concat_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	s3concat "github.com/fujiwara/go-s3concat"
)

var Bucket = aws.String("mytest")

type File struct {
	Key      string
	Body     []byte
	IsPrefix bool
}

func (f File) Size() int64 {
	return int64(len(f.Body))
}

var Files = map[string]File{
	"1MB":      File{Key: "1MB", Body: randomBody(1024 * 1024)},
	"2MB":      File{Key: "2MB", Body: randomBody(2 * 1024 * 1024)},
	"4MB":      File{Key: "4MB", Body: randomBody(4 * 1024 * 1024)},
	"6MB":      File{Key: "6MB", Body: randomBody(6 * 1024 * 1024)},
	"foo/9999": File{Key: "foo/9999", Body: randomBody(9999)},
	"foo/1234": File{Key: "foo/1234", Body: randomBody(1234)},
	"bar/8888": File{Key: "bar/8888", Body: randomBody(8888)},
}

type Suite struct {
	Src    []File
	Dest   File
	Option *s3concat.Option
}

var Suites []Suite

func init() {
	foo := File{Key: "foo/", IsPrefix: true}
	foo.Body = append(foo.Body, Files["foo/1234"].Body...)
	foo.Body = append(foo.Body, Files["foo/9999"].Body...)
	Files["foo/"] = foo

	foo1 := File{Key: "foo/1", IsPrefix: true}
	foo1.Body = append(foo1.Body, Files["foo/1234"].Body...)
	Files["foo/1"] = foo1

	Suites = []Suite{
		Suite{
			Src:    []File{Files["1MB"], Files["2MB"], Files["4MB"], Files["6MB"]},
			Dest:   File{Key: "concat/13MB"},
			Option: &s3concat.Option{Recursive: false},
		},
		Suite{
			Src:    []File{Files["1MB"], Files["foo/"], Files["2MB"], Files["4MB"], Files["6MB"], Files["bar/8888"]},
			Dest:   File{Key: "concat/13MB_foo_bar"},
			Option: &s3concat.Option{Recursive: true},
		},
		Suite{
			Src:    []File{Files["foo/9999"], Files["foo/1234"]},
			Dest:   File{Key: "concat/9999_1234"},
			Option: &s3concat.Option{Recursive: false},
		},
		Suite{
			Src:    []File{Files["foo/"]},
			Dest:   File{Key: "concat/recr_foo"},
			Option: &s3concat.Option{Recursive: true},
		},
		Suite{
			Src:    []File{Files["foo/1"]},
			Dest:   File{Key: "concat/foo_1234"},
			Option: &s3concat.Option{Recursive: true},
		},
		Suite{
			Src:    []File{Files["foo/1234"], Files["bar/8888"]},
			Dest:   File{Key: "concat/foo_bar"},
			Option: &s3concat.Option{Recursive: false},
		},
		Suite{
			Src:    []File{Files["6MB"], Files["6MB"], Files["4MB"], Files["6MB"]},
			Dest:   File{Key: "concat/24MB"},
			Option: &s3concat.Option{Recursive: false},
		},
	}
}

func (f File) String() string {
	return fmt.Sprintf("s3://%s/%s", *Bucket, f.Key)
}

// for localstack
var sess = session.Must(session.NewSession(&aws.Config{
	Credentials:      credentials.NewStaticCredentials("foo", "bar", ""),
	S3ForcePathStyle: aws.Bool(true),
	Region:           aws.String(endpoints.UsWest2RegionID),
	Endpoint:         aws.String("http://localhost:4572"),
}))

func TestMain(m *testing.M) {
	svc := s3.New(sess)
	svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: Bucket,
	})
	for _, f := range Files {
		if f.IsPrefix {
			continue
		}
		log.Printf("putting %s %d bytes", f.Key, f.Size())
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket: Bucket,
			Key:    aws.String(f.Key),
			Body:   bytes.NewReader(f.Body),
		})
		if err != nil {
			panic(err)
		}
	}

	code := m.Run()

	if code != 0 {
		os.Exit(code)
	}

	// clean up
	var files []File
	for _, f := range Files {
		if strings.HasSuffix(f.Key, "/") {
			continue // dir
		}
		files = append(files, f)
	}
	for _, s := range Suites {
		files = append(files, s.Dest)
	}
	for _, f := range files {
		log.Printf("deleting %s", f.Key)
		svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: Bucket,
			Key:    aws.String(f.Key),
		})
	}
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: Bucket,
	})
	if err != nil {
		panic(err)
	}
}

func TestLocalStack(t *testing.T) {
	svc := s3.New(sess)
	for i, s := range Suites {
		t.Logf("testing suite %d %v", i, s)
		var (
			src     = make([]string, 0, len(s.Src))
			srcSize = int64(0)
		)
		for _, f := range s.Src {
			src = append(src, f.String())
			srcSize += f.Size()
		}
		var srcBody = make([]byte, 0, srcSize)
		for _, f := range s.Src {
			srcBody = append(srcBody, f.Body...)
		}

		err := s3concat.Concat(sess, src, s.Dest.String(), s.Option)
		if err != nil {
			t.Error(err)
			continue
		}
		res, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: Bucket,
			Key:    aws.String(s.Dest.Key),
		})
		if err != nil {
			t.Error(err)
			continue
		}
		concated, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if *res.ContentLength != srcSize {
			t.Errorf("unexpected size %d expect %d", res.ContentLength, srcSize)
		}
		if !bytes.Equal(concated, srcBody) {
			t.Errorf("not equal of %v and %s", s.Src, s.Dest)
		}
	}
}

func randomBody(size int64) []byte {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return buf
}
