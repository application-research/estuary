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

type IUpdater interface {
	SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error
}

type updater struct {
	db *gorm.DB
}

func NewUpdater(db *gorm.DB) IUpdater {
	return &updater{db: db}
}

func (up *updater) SetDataTransferStartedOrFinished(ctx context.Context, dealDBID uint, chanIDOrTransferID string, st *filclient.ChannelState, isStarted bool) error {
	if st == nil {
		return nil
	}

	var deal model.ContentDeal
	if err := up.db.First(&deal, "id = ?", dealDBID).Error; err != nil {
		return err
	}

	var cont util.Content
	if err := up.db.First(&cont, "id = ?", deal.Content).Error; err != nil {
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

	if err := up.db.Model(model.ContentDeal{}).Where("id = ?", dealDBID).UpdateColumns(updates).Error; err != nil {
		return xerrors.Errorf("failed to update deal with channel ID: %w", err)
	}
	return nil
}
