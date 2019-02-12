.PHONY: test testdata test-localstack clean init-locals3

export GO111MODULE := on

test:
	docker-compose up -d
	dockerize -timeout 30s -wait http://localhost:4572 go test -v

clean:
	rm -f cmd/s3concat/s3concat

cmd/s3concat/s3concat: *.go cmd/s3concat/main.go
	cd cmd/s3concat && go build
