package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func initDB() (*DBMgr, func()) {
	mgr, err := NewDBMgr("sqlite=test.db")
	if err != nil {
		panic(err)
	}

	finish := func() {
		os.Remove("test.db")
	}
	return mgr, finish
}

func TestUsersQuery(t *testing.T) {
	mgr, finish := initDB()
	defer finish()

	a := assert.New(t)

	// Creation
	userIn := User{
		Username: "test_user",
		PassHash: "",
	}
	fmt.Printf("creating user: %+v\n", userIn)
	a.NoError(mgr.Users().Create(userIn))

	// Checking existence
	userExists, err := mgr.Users().WithUsername("Test_user").Exists()
	if err != nil {
		t.Fatal(err)
	}
	if !userExists {
		t.Fatal("user doesn't exist")
	}

	// Getting by username
	userOut, err := mgr.Users().WithUsername(userIn.Username).Get()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("retrieved user: %+v\n", userOut)

	// Deletion by ID
	if err := mgr.Users().WithID(userOut.ID).ExpectDelete(); err != nil {
		t.Fatal(err)
	}
}
