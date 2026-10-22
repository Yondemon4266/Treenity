
gitinette:

	python3 subject/attachments/gitinette

setup:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg

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

.PHONY: gitinette setup client test sync push