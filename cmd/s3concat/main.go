package main

import (
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	s3concat "github.com/fujiwara/go-s3concat"
)

func main() {
	sess := session.Must(session.NewSession())
	n := len(os.Args)
	if n < 3 {
		log.Println("[error] Usage: s3concat [src]... [dst]")
		os.Exit(1)
	}
	err := s3concat.Concat(sess, os.Args[1:n-1], os.Args[n-1])
	if err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}
