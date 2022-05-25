package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type isValidAuthTest struct {
	inpAuthStr string
	expected   bool
}

var validAuthTests = []isValidAuthTest{
	isValidAuthTest{"ESTsomethingARY", false},
	isValidAuthTest{"ESTsomething", false},
	isValidAuthTest{"EST6054be81-a71f-436a-a09d-33338bbc9066dARY", false}, // invalid uuid length
	isValidAuthTest{"EST6054be81-a71f-436a-a09d-8e68bbc9066dARY", true},
	isValidAuthTest{"SECRET8cd64b42-4b71-457d-bbf7-a9a94fc04909SECRET", true},
}

func TestIsValidAuth(t *testing.T) {
	//check your test scenario here
	for _, test := range validAuthTests {
		assert.Equal(t, isValidAuth(test.inpAuthStr), test.expected)
	}
}
