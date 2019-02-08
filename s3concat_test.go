package s3concat_test

import (
	"testing"

	s3concat "github.com/fujiwara/go-s3concat"
)

func TestPaseS3Object(t *testing.T) {
	o, err := s3concat.ParseS3URL("s3://example.com/foo/bar.baz")
	if err != nil {
		t.Error(err)
	}
	if o.Bucket != "example.com" || o.Key != "foo/bar.baz" {
		t.Errorf("unexpected parsed %#v", o)
	}
}
