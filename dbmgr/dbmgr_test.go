package dbmgr

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (q *UsersQuery) sqliteExists() (bool, error) {
	count, err := q.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

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
	db, finish := initDB()
	defer finish()

	a := assert.New(t)

	// Creation
	userIn := User{
		Username: "test_user",
		PassHash: "",
	}
	fmt.Printf("creating user: %+v\n", userIn)
	a.NoError(db.Users().Create(userIn))

	// Checking existence
	userExists, err := db.Users().WithUsername("test_user").sqliteExists()
	if err != nil {
		t.Fatal(err)
	}
	if !userExists {
		t.Fatal("user doesn't exist")
	}

	// Getting by username
	userOut, err := db.Users().WithUsername(userIn.Username).Get()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("retrieved user: %+v\n", userOut)

	// Deletion by ID
	if err := db.Users().WithID(userOut.ID).ExpectDelete(); err != nil {
		t.Fatal(err)
	}
}

func TestContentsQuery(t *testing.T) {
	db, finish := initDB()
	defer finish()

	a := assert.New(t)

	// Creation
	contents := []Content{
		{
			Filename: "a",
		},
		{
			Filename: "b",
		},
		{
			Filename: "c",
		},
	}
	a.NoError(db.Contents().CreateAll(contents))

	count, err := db.Contents().Count()
	a.NoError(err)
	a.Equal(count, int64(3))
}
