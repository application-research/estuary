SHELL=/usr/bin/env bash

GO_BUILD_IMAGE?=golang:1.15
COMMIT := $(shell git rev-parse --short=8 HEAD)

# GITVERSION is the nearest tag plus number of commits and short form of most recent commit since the tag, if any
GITVERSION=$(shell git describe --always --tag --dirty)

unexport GOFLAGS

CLEAN:=
BINS:=

GOFLAGS:=

.PHONY: all
all: build

## FFI

FFI_PATH:=extern/filecoin-ffi/
FFI_DEPS:=.install-filcrypto
FFI_DEPS:=$(addprefix $(FFI_PATH),$(FFI_DEPS))

$(FFI_DEPS): build/.filecoin-install ;

build/.filecoin-install: $(FFI_PATH)
	$(MAKE) -C $(FFI_PATH) $(FFI_DEPS:$(FFI_PATH)%=%)
	@touch $@

MODULES+=$(FFI_PATH)
BUILD_DEPS+=build/.filecoin-install
CLEAN+=build/.filecoin-install

ffi-version-check:
	@[[ "$$(awk '/const Version/{print $$5}' extern/filecoin-ffi/version.go)" -eq 3 ]] || (echo "FFI version mismatch, update submodules"; exit 1)
BUILD_DEPS+=ffi-version-check

.PHONY: ffi-version-check

$(MODULES): build/.update-modules ;
# dummy file that marks the last time modules were updated
build/.update-modules:
	git submodule update --init --recursive
	touch $@

CLEAN+=build/.update-modules

# Once estuary has it's own version cmd add this back in
#ldflags=-X=github.com/application-research/estuary/version.GitVersion=$(GITVERSION)
#ifneq ($(strip $(LDFLAGS)),)
#	ldflags+=-extldflags=$(LDFLAGS)
#endif
#GOFLAGS+=-ldflags="$(ldflags)"

.PHONY: build
build: deps estuary

.PHONY: deps
deps: $(BUILD_DEPS)

.PHONY: estuary
estuary:
	go build

.PHONY: install
install: estuary
	cp estuary /usr/local/bin/estuary

.PHONY: install-estuary-service
install-estuary-service:
	cp scripts/estuary.service /etc/systemd/system/estuary.service
	cp scripts/config.env /etc/estuary/config.env
	cp scripts/log.env /etc/estuary/log.env
	systemctl daemon-reload
	echo "Edit config values in /etc/estuary/config.env before enabling and starting estuary service"
	echo "Run 'sudo systemctl enable --now estuary.service' once ready to enable and start estuary service"

.PHONY: clean
clean:
	rm -rf $(CLEAN) $(BINS)

.PHONY: dist-clean
dist-clean:
	git clean -xdff
	git submodule deinit --all -f
