package miner

import (
	"fmt"
	"net/http"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type SuspendMinerBody struct {
	Reason string `json:"reason"`
}

func (mm *MinerManager) SuspendMiner(m address.Address, params SuspendMinerBody, u *util.User) error {
	var sm model.StorageMiner
	if err := mm.db.First(&sm, "address = ?", m.String()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_RECORD_NOT_FOUND,
				Details: fmt.Sprintf("miner: %s was not found", m),
			}
		}
		return err
	}

	if !(u.Perm >= util.PermLevelAdmin || sm.Owner == u.ID) {
		return &util.HttpError{
			Code:    http.StatusUnauthorized,
			Reason:  util.ERR_MINER_NOT_OWNED,
			Details: "user does not own this miner",
		}
	}

	return mm.db.Model(&model.StorageMiner{}).Where("address = ?", m.String()).Updates(map[string]interface{}{
		"suspended":        true,
		"suspended_reason": params.Reason,
	}).Error
}
