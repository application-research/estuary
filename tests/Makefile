
.PHONY: all
all: unit-tests regression-tests chaos-tests

.PHONY: unit-tests
unit-tests:
	go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo
	cd unit && ginkgo -r --json-report=../reports/report.json

.PHONY: regression-tests
regression-tests:
	cd regression;./run-regression-tests.sh >> ../reports/regression_log_$(shell date "+%Y.%m.%d-%H.%M.%S").log

.PHONY: chaos-tests
chaos-tests:
	cd chaos;./run-chaos-tests.sh >> ../reports/chaos_log_$(shell date "+%Y.%m.%d-%H.%M.%S").log