
.PHONY: all
all: unit-tests regression-tests chaos-tests

.PHONY: unit-tests
unit-tests:
	cd ../;go test

.PHONY: regression-tests
regression-tests:
	cd regression;./run-regression-tests.sh

.PHONY: chaos-tests
chaos-tests:
	cd chaos;./run-chaos-tests.sh