BINARY  := lemon
PKGDIR  := ./cmd/lemon
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GOFLAGS ?=
GO      ?= go
LDFLAGS := -s -w -X github.com/siin/lemon/internal/cli.Version=$(VERSION)

DESTDIR ?=
PREFIX  ?= /usr
BINDIR  ?= $(PREFIX)/bin
INSTALL ?= install

GO_FILES := $(shell find . -name '*.go' -not -path './src/*')

.PHONY: all build test test-unit test-e2e check vet fmt fmtcheck clean install uninstall run help

all: build

## build: compile the lemon binary
build:
	@echo "==> compiling cgo sqlite3 (first build takes ~1min)"
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) $(PKGDIR)

## test: run every test
test: test-unit test-e2e

## test-unit: run package tests
test-unit:
	$(GO) test $(GOFLAGS) ./...

## test-e2e: run integration tests
# The integration suite is added as it lands; an empty tests/ directory is not
# a failure, otherwise `make test` breaks on a fresh checkout.
test-e2e:
	@if [ -n "$$(ls tests/*.go 2>/dev/null)" ]; then \
		$(GO) test -count=1 -timeout 5m ./tests/... $(GOFLAGS); \
	else \
		echo "==> no integration tests yet, skipping"; \
	fi

## check: formatting, static analysis and tests
check: fmtcheck vet test

## vet: static analysis
vet:
	$(GO) vet ./...

## fmt: format sources
fmt:
	$(GO) fmt ./...

## fmtcheck: fail if sources are unformatted
fmtcheck:
	@unformatted=$$(gofmt -l $(GO_FILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; \
	fi

## clean: remove build output
clean:
	rm -f $(BINARY)
	$(GO) clean -cache -testcache 2>/dev/null || true

## install: install to $(DESTDIR)$(BINDIR)/lemon
install: build
	@if [ -e "$(DESTDIR)$(BINDIR)/$(BINARY)" ] && ! ./scripts/ismine.sh "$(DESTDIR)$(BINDIR)/$(BINARY)"; then \
		echo "error: $(DESTDIR)$(BINDIR)/$(BINARY) exists and is not a lemon build."; \
		echo "       Arch's core/lemon (parser generator) may own it."; \
		echo "       Reclaim it with:  sudo pacman -Rns lemon"; \
		echo "       Or force:          LEMON_LEMON_OVERWRITE=1 sudo make install"; \
		exit 1; \
	fi
	$(INSTALL) -Dm755 $(BINARY) $(DESTDIR)$(BINDIR)/$(BINARY)
	@echo "installed $(DESTDIR)$(BINDIR)/$(BINARY)"

## uninstall: remove the installed binary
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(BINARY)

## run: build then show help
run: build
	./$(BINARY) help

## help: list targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
