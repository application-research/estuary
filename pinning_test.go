package main

import (
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/application-research/estuary/pinner/types"
	"github.com/stretchr/testify/assert"
)

type Conts struct {
	ID uint
}

func TestStatusFilterQuery(t *testing.T) {
	assert := assert.New(t)

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{DryRun: true})
	assert.NoError(err)

	s := map[types.PinningStatus]bool{}

	resp, ok, err := filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("SELECT * FROM `conts`",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[types.PinningStatus]bool{
		types.PinningStatusFailed: true,
	}

	resp, ok, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("SELECT * FROM `conts` WHERE failed",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[types.PinningStatus]bool{
		types.PinningStatusFailed: true,
		types.PinningStatusPinned: true,
	}

	resp, ok, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("SELECT * FROM `conts` WHERE active or failed",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[types.PinningStatus]bool{
		types.PinningStatusPinning: true,
		types.PinningStatusPinned:  true,
	}

	resp, ok, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.False(ok)
	assert.Equal("SELECT * FROM `conts` WHERE not failed",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[types.PinningStatus]bool{
		types.PinningStatusPinning: true,
		types.PinningStatusQueued:  true,
	}

	resp, ok, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("SELECT * FROM `conts` WHERE not active and not failed",
		resp.Find([]Conts{}).Statement.SQL.String())
}
