package model

import (
	"time"

	"gorm.io/gorm"
)

type ZoneStatus string
type ZoneMessage string

const (
	ZoneStatuAggregating    ZoneStatus = "aggregating"
	ZoneStatusConsolidating ZoneStatus = "consolidation"
	ZoneStatusDone          ZoneStatus = "done"
	ZoneStatusStuck         ZoneStatus = "stuck"
	ZoneStatusOpen          ZoneStatus = "open"

	ZoneMessageOpen          ZoneMessage = "Zone is open for content aggregation"
	ZoneMessageDone          ZoneMessage = "Zone has been aggregated and a deal will be made for it"
	ZoneMessageAggregating   ZoneMessage = "Zone contents are under aggregation processing"
	ZoneMessageConsolidating ZoneMessage = "Zone contents are under consolidation processing"
	ZoneMessageStuck         ZoneMessage = ""
)

type StagingZone struct {
	gorm.Model
	ID        uint        `gorm:"index:id_size_status;index:id_status" json:"id"`
	CreatedAt time.Time   `gorm:"index;not null" json:"createdAt"`
	MinSize   int64       `gorm:"index;not null" json:"minSize"`
	MaxSize   int64       `json:"maxSize"`
	Size      int64       `gorm:"index:size_status;index:id_size_status;index:user_size_status;index;not null" json:"curSize"`
	UserID    uint        `gorm:"index:user_size_status;index;not null" json:"user"`
	ContID    uint        `gorm:"index;not null" json:"contentID"`
	Location  string      `gorm:"index;not null" json:"location"`
	Status    ZoneStatus  `gorm:"index:size_status;index:id_size_status;index:user_size_status;index:id_status" json:"status"`
	Message   ZoneMessage `json:"message" gorm:"type:text"`
}
