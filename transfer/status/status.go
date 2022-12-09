package status

import (
	"context"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/application-research/filclient"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type Updater struct {
	db *gorm.DB
}

func NewUpdater(db *gorm.DB) *Updater {
	return &Updater{db: db}
}

func (udt *Updater) SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error {
	if st == nil {
		return nil
	}

	var deal model.ContentDeal
	if err := udt.db.First(&deal, "id = ?", dealDBID).Error; err != nil {
		return err
	}

	var cont util.Content
	if err := udt.db.First(&cont, "id = ?", deal.Content).Error; err != nil {
		return err
	}

	updates := map[string]interface{}{
		"dt_chan": chanIDOrTransferID,
	}

	switch isStarted {
	case true:
		updates["transfer_started"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("Requested"); s != nil {
			updates["transfer_started"] = s.CreatedTime.Time()
		}
	default:
		updates["transfer_finished"] = time.Now() // boost transfers does not support stages, so we can't get actual timestamps
		if s := st.Stages.GetStage("TransferFinished"); s != nil {
			updates["transfer_finished"] = s.CreatedTime.Time()
		}
	}

	if err := udt.db.Model(model.ContentDeal{}).Where("id = ?", dealDBID).UpdateColumns(updates).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	return nil
}
