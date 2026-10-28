TREENITY_IPC ?= /tmp/treenity.server.12345
TOPIC ?= bocal
SUB_NAME ?= aqua_sub

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

# ------------------------

client:
	go run ./cmd/client $(ARGS)

test:
	gotest -v ./...

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

.PHONY: gitinette setup build client aquarium fish test sync push