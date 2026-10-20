
.PHONY: gitinette

gitinette:
	python3 subject/attachments/gitinette

setup:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg

ifeq (client,$(firstword $(MAKECMDGOALS)))
  # we extract each argument word after 'client'
  CLIENT_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # creates fake target files for each argument so make doesn't crash
  $(eval $(CLIENT_ARGS):;@:)
endif

client:
	go run ./cmd/client $(CLIENT_ARGS)