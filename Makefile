.DEFAULT_GOAL := all

.PHONY: build
build:
	hatch build

.PHONY: publish-test
publish-test:
	hatch publish --repo test --user __token__ --auth ${PYPI_TEST_TOKEN}

.PHONY: publish
publish:
	hatch publish --repo main --user __token__ --auth ${PYPI_TOKEN}

.PHONY: clean
clean:
	rm -rf dist/*

.PHONY: lint
lint:
	ruff check --fix ./src

.PHONY: all
all: clean lint build publish
