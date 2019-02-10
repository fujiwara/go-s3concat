.PHONY: test testdata test-localstack clean init-locals3

test:
	go test -v .

test-localstack:
	docker-compose up -d
	dockerize -timeout 30s -wait http://localhost:4572 go test -v


clean:
	rm -rf test/data/*
