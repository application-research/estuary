
.PHONY: all
all: unit-tests regression-tests chaos-tests

.PHONY: unit-tests
unit-tests:
	cd ../;go test >> tests/data/logs/unit_log_$(shell date "+%Y%m%d%H%M%S").log

.PHONY: regression-tests
regression-tests:
	cd regression;./run-regression-tests.sh >> ../data/logs/regression_log_$(shell date "+%Y.%m.%d-%H.%M.%S").log

.PHONY: chaos-tests
chaos-tests:
	cd chaos;./run-chaos-tests.sh >> ../data/logs/chaos_log_$(shell date "+%Y.%m.%d-%H.%M.%S").log