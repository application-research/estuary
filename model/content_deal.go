package model

import (
	"fmt"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"gorm.io/gorm"
)

var ErrNoChannelID = fmt.Errorf("no data transfer channel id in deal")

type ContentDeal struct {
	gorm.Model
	Content          uint       `json:"content" gorm:"index:,option:CONCURRENTLY"`
	UserID           uint       `json:"user_id" gorm:"index:,option:CONCURRENTLY"`
	PropCid          util.DbCID `json:"propCid"`
	DealUUID         string     `json:"dealUuid"`
	Miner            string     `json:"miner"`
	DealID           int64      `json:"dealId"`
	Failed           bool       `json:"failed"`
	Verified         bool       `json:"verified"`
	Slashed          bool       `json:"slashed"`
	FailedAt         time.Time  `json:"failedAt,omitempty"`
	DTChan           string     `json:"dtChan" gorm:"index"`
	TransferStarted  time.Time  `json:"transferStarted"`
	TransferFinished time.Time  `json:"transferFinished"`

	OnChainAt           time.Time   `json:"onChainAt"`
	SealedAt            time.Time   `json:"sealedAt"`
	DealProtocolVersion protocol.ID `json:"deal_protocol_version"`
	MinerVersion        string      `json:"miner_version"`
}

func (cd ContentDeal) MinerAddr() (address.Address, error) {
	return address.NewFromString(cd.Miner)
}

func (cd ContentDeal) ChannelID() (datatransfer.ChannelID, error) {
	if cd.DTChan == "" {
		return datatransfer.ChannelID{}, ErrNoChannelID
	}

	chid, err := filclient.ChannelIDFromString(cd.DTChan)
	if err != nil {
		err = fmt.Errorf("incorrectly formatted data transfer channel ID in contentDeal record: %w", err)
		return datatransfer.ChannelID{}, err
	}
	return *chid, nil
}
