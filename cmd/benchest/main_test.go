package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMain will exec each test, one by one
func TestMain(m *testing.M) {
	setupSuite()

	// exec test and this returns an exit code to pass to os
	retCode := m.Run()

	tearDownSuite()
	// If exit code is distinct of zero,
	// the test will be failed (red)
	os.Exit(retCode)
}

func setupSuite() {
	et := os.Getenv("ESTUARY_TOKEN")

	if len(et) == 0 {
		log.Fatal("ESTUARY_TOKEN not set as an environment variable.")
		os.Exit(1)
	}

	log.Printf("ESTUARY_TOKEN found, continuing with tests.")

}

func tearDownSuite() {
}

func run(args []string) {
	app := getApp()

	err := app.Run(args)
	if err != nil {
		os.Exit(1)
	}
}

func TestBenchest_AddFile(t *testing.T) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	args := os.Args[0:1]            // Name of the program.
	args = append(args, "add-file") // Append a flag
	args = append(args, "--host=shuttle-1.estuary.tech")
	run(args)

	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC

	assert.Contains(t, out, "\"FileCID\":", "File not added to gateway.")
}
