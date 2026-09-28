#!/bin/sh
# ismine.sh <path-to-binary>
# Succeeds when the binary at <path> was built from github.com/siin/lemon.
# Used by `make install` so we never silently clobber a foreign binary such as
# Arch's core/lemon parser generator.
set -eu

bin=${1:-}
[ -n "$bin" ] || exit 1
[ -e "$bin" ] || exit 1

if command -v go >/dev/null 2>&1; then
	if go version -m "$bin" 2>/dev/null | grep -q 'github.com/siin/lemon'; then
		exit 0
	fi
fi

# Fall back to the build-id marker embedded in the binary.
if command -v strings >/dev/null 2>&1; then
	if strings -a "$bin" 2>/dev/null | grep -q 'lemon-p2p/buildinfo'; then
		exit 0
	fi
fi

exit 1
