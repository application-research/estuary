package api

import (
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	pinningstatus "github.com/application-research/estuary/pinner/status"
	"github.com/stretchr/testify/assert"
)

type Conts struct {
	ID uint
}

func TestStatusFilterQuery(t *testing.T) {
	assert := assert.New(t)

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{DryRun: true})
	assert.NoError(err)

	s := map[pinningstatus.PinningStatus]bool{}

	resp, err := filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.Equal("SELECT * FROM `conts`",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[pinningstatus.PinningStatus]bool{
		pinningstatus.PinningStatusFailed: true,
	}

	resp, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.Equal("SELECT * FROM `conts` WHERE failed and not active and not pinning",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[pinningstatus.PinningStatus]bool{
		pinningstatus.PinningStatusFailed: true,
		pinningstatus.PinningStatusPinned: true,
	}

	resp, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.Equal("SELECT * FROM `conts` WHERE (active or failed) and not pinning",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[pinningstatus.PinningStatus]bool{
		pinningstatus.PinningStatusPinning: true,
		pinningstatus.PinningStatusPinned:  true,
	}

	resp, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.Equal("SELECT * FROM `conts` WHERE (active or pinning) and not failed",
		resp.Find([]Conts{}).Statement.SQL.String())

	s = map[pinningstatus.PinningStatus]bool{
		pinningstatus.PinningStatusPinning: true,
		pinningstatus.PinningStatusQueued:  true,
	}

	resp, err = filterForStatusQuery(db, s)
	assert.NoError(err)
	assert.Equal("SELECT * FROM `conts` WHERE pinning and not active and not failed",
		resp.Find([]Conts{}).Statement.SQL.String())
}
