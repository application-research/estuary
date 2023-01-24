package dealstatus

import (
	"fmt"

	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type DealFailureError struct {
	Miner               address.Address
	DealUUID            string
	Phase               string
	Message             string
	Content             uint64
	UserID              uint
	MinerAddress        string
	DealProtocolVersion protocol.ID
	MinerVersion        string
}

func (dfe *DealFailureError) record() *model.DfeRecord {
	return &model.DfeRecord{
		Miner:               dfe.Miner.String(),
		DealUUID:            dfe.DealUUID,
		Phase:               dfe.Phase,
		Message:             dfe.Message,
		Content:             dfe.Content,
		UserID:              dfe.UserID,
		MinerVersion:        dfe.MinerVersion,
		DealProtocolVersion: dfe.DealProtocolVersion,
	}
}

func (dfe *DealFailureError) Error() string {
	return fmt.Sprintf("deal %s with miner %s failed in phase %s: %s", dfe.DealUUID, dfe.Message, dfe.Phase, dfe.Message)
}
