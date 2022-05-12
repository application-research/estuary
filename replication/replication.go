package replication

import (
	"sync"
	"time"

	"github.com/application-research/estuary/constants"
	"github.com/application-research/estuary/util"
)

type ContentStagingZone struct {
	ZoneOpened time.Time `json:"zoneOpened"`

	EarliestContent time.Time `json:"earliestContent"`
	CloseTime       time.Time `json:"closeTime"`

	Contents []util.Content `json:"contents"`

	MinSize int64 `json:"minSize"`
	MaxSize int64 `json:"maxSize"`

	MaxItems int `json:"maxItems"`

	CurSize int64 `json:"curSize"`

	User uint `json:"user"`

	ContID   uint   `json:"contentID"`
	Location string `json:"location"`

	Lk sync.Mutex
}

func (cb *ContentStagingZone) DeepCopy() *ContentStagingZone {
	cb.Lk.Lock()
	defer cb.Lk.Unlock()
	cb2 := &ContentStagingZone{
		ZoneOpened:      cb.ZoneOpened,
		EarliestContent: cb.EarliestContent,
		CloseTime:       cb.CloseTime,
		Contents:        make([]util.Content, len(cb.Contents)),
		MinSize:         cb.MinSize,
		MaxSize:         cb.MaxSize,
		MaxItems:        cb.MaxItems,
		CurSize:         cb.CurSize,
		User:            cb.User,
		ContID:          cb.ContID,
		Location:        cb.Location,
	}
	copy(cb2.Contents, cb.Contents)
	return cb2
}

func (cb ContentStagingZone) isReady() bool {
	if cb.CurSize < constants.MinDealSize {
		return false
	}

	// if its above the size requirement, go right ahead
	if cb.CurSize > cb.MinSize {
		return true
	}

	if time.Now().After(cb.CloseTime) {
		return true
	}

	if time.Since(cb.EarliestContent) > constants.MaxContentAge {
		return true
	}

	if len(cb.Contents) >= cb.MaxItems {
		return true
	}

	return false
}

func (cb ContentStagingZone) hasRoomForContent(c util.Content) bool {
	cb.Lk.Lock()
	defer cb.Lk.Unlock()

	if len(cb.Contents) >= cb.MaxItems {
		return false
	}

	return cb.CurSize+c.Size <= cb.MaxSize
}

func (cb ContentStagingZone) hasContent(c util.Content) bool {
	cb.Lk.Lock()
	defer cb.Lk.Unlock()

	for _, cont := range cb.Contents {
		if cont.ID == c.ID {
			return true
		}
	}
	return false
}
