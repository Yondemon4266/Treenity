
.PHONY: gitinette

gitinette:
	python3 subject/attachments/gitinette

setup:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg


client: $(SOURCES)
	go run ./cmd/client