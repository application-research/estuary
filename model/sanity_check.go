package model

import (
	"github.com/application-research/estuary/util"
	"gorm.io/gorm"
)

type SanityCheck struct {
	gorm.Model
	ContentID uint       `json:"-" gorm:"uniqueIndex:cnt_blk_cid_index;index;not null"`
	BlockCid  util.DbCID `json:"-" gorm:"uniqueIndex:cnt_blk_cid_index;index;not null"`
	ErrMsg    string     `json:"-" gorm:"type:text;not null"`
}
