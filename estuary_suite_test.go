package main

import (
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEstuary(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Estuary Suite")
}

var _ = BeforeSuite(func() {
	fmt.Println("BeforeAll")
	os.Args = []string{""}
	go func() {
		main() // just
	}()
})
