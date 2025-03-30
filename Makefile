DIR := $(shell pwd)
GIT_COMMIT := $(shell git rev-parse HEAD)

############ COMMON COMMANDS ############
SRC := $(DIR)/
TEST := $(DIR)/tests

.PHONY: lint
lint:
	uvx ruff check --extend-select I --fix $(SRC)
	uvx ruff check --extend-select I --fix $(TEST)

.PHONY: fmt
fmt:
	uvx ruff format $(SRC)
	uvx ruff format $(TEST)

.PHONY: lint_fmt
lint_fmt: lint fmt

.PHONY: mypy
mypy:
	uvx mypy $(SRC) --namespace-packages --explicit-package-bases
	uvx mypy $(TEST) --namespace-packages --explicit-package-bases

.PHONY: pre-commit-install
pre-commit-install:
	pre-commit install

.PHONY: test
test:
	pytest -s -v $(TEST)
