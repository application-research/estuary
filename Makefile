all: estuary

.PHONY: estuary
estuary:
	go build

install: estuary
	cp estuary /usr/local/bin/estuary
