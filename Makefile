.PHONY: all docs install format github lint release test test-ci


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d '"' -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/assets/VERSION)
LEAD := $(shell head -n 1 LEAD.md)


all:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

docs:
	python docs/build.py

format:
	black $(PACKAGE) tests

github:
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/issue_template.md
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/pull_request_template.md

install:
	pip install --upgrade -e .

lint:
	black $(PACKAGE) tests --check
	pylama $(PACKAGE) tests

test:
	make lint
	pytest --cov ${PACKAGE} --cov-report term-missing --cov-fail-under 70
