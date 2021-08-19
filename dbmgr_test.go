package main

import (
	"fmt"
	"os"
	"testing"
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

	// Creation
	userIn := User{
		Username: "test_user",
		PassHash: "",
	}
	if err := mgr.Users().Create(userIn); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("creating user: %+v\n", userIn)

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
