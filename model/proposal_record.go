package model

import "github.com/application-research/estuary/util"

type ProposalRecord struct {
	PropCid util.DbCID `gorm:"index"`
	Data    []byte
}
