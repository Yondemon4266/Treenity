
.PHONY: gitinette client test setup

gitinette:
	python3 subject/attachments/gitinette

setup:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg

client:
	go run ./cmd/client $(ARGS)
test:
	gotest -v ./...