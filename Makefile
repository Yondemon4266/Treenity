TREENITY_IPC ?= /tmp/treenity.server.12345
TOPIC ?= bocal
SUB_NAME ?= aqua_sub

all: build-server build-client

build-server:
	go build -o server ./cmd/server

gitinette:
	python3 subject/attachments/gitinette

setup:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg


build-client:
	go build -o client ./cmd/client


# consumer aquarium
aquarium: build-client
	./subject/attachments/aquarium ./client $(TREENITY_IPC) $(TOPIC) $(SUB_NAME)

# producer fish
fish: build-client
	./client $(TREENITY_IPC) create $(TOPIC)
	./subject/attachments/fish ./client $(TREENITY_IPC) $(TOPIC)

bubble:
	@if [ -f "subject/attachments/fish" ]; then \
	./server > ipc_path.tmp & \
	sleep 1; \
	SERVER_IPC=$$(cat ipc_path.tmp); \
	./client $$SERVER_IPC create test_topic; \
	for i in $$(seq 1 200); do \
		./subject/attachments/fish ./client $$SERVER_IPC test_topic & \
	done; \
	./subject/attachments/aquarium ./client $$SERVER_IPC test_topic test_client; \
	rm -f ipc_path.tmp; \
	pkill fish; \
else \
	echo "No subject attachments found, please download them first."; \
    fi

client:
	go run ./cmd/client $(ARGS)

clean:
	rm -f server client

re: clean
	$(MAKE) all

test:
	go test -v ./internal/trie/trie.go ./internal/trie/trie_test.go

test-all:
	go test -v ./...

sync:
	@git fetch origin main; \
	BEHIND=$$(git rev-list --count HEAD..origin/main); \
	if [ "$$BEHIND" -gt 0 ]; then \
		echo "Your branch is $$BEHIND commit(s) late"; \
		git rebase origin/main; \
		echo "Rebased main on your branch"; \
	fi; \
	echo "Branch is up to date"

push: sync
	@BRANCH=$$(git branch --show-current); \
	git push origin $$BRANCH; \
	echo "Push $$BRANCH successfully"

.PHONY: all build-server build-client clean re test test-all gitinette setup client aquarium fish sync push