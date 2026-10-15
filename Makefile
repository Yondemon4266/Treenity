
.PHONY: gitinette

gitinette:
	python3 subject/attachments/gitinette


commit-protection-script:
	git config core.hooksPath scripts/hooks
	chmod +x scripts/hooks/commit-msg
	