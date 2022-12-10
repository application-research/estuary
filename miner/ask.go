package miner

import (
	"context"
	"fmt"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/lotus/chain/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm/clause"
)

func (mm *MinerManager) GetAsk(ctx context.Context, m address.Address, maxCacheAge time.Duration) (*model.MinerStorageAsk, error) {
	ctx, span := mm.tracer.Start(ctx, "getAsk", trace.WithAttributes(
		attribute.Stringer("miner", m),
	))
	defer span.End()

	var asks []model.MinerStorageAsk
	if err := mm.db.Find(&asks, "miner = ?", m.String()).Error; err != nil {
		return nil, err
	}

	minerVersion, err := mm.updateMinerVersion(ctx, m)
	if err != nil {
		mm.log.Warnf("failed to update miner version: %s", err)
	}

	if len(asks) > 0 && time.Since(asks[0].UpdatedAt) < maxCacheAge {
		ask := asks[0]
		priceBigInt, err := types.BigFromString(ask.Price)
		if err != nil {
			return nil, err
		}
		ask.PriceBigInt = priceBigInt

		verifiedPriceBigInt, err := types.BigFromString(ask.VerifiedPrice)
		if err != nil {
			return nil, err
		}
		ask.VerifiedPriceBigInt = verifiedPriceBigInt

		if ask.MinerVersion == "" {
			ask.MinerVersion = minerVersion
		}
		return &ask, nil
	}

	netask, err := mm.filClient.GetAsk(ctx, m)
	if err != nil {
		return nil, err
	}

	if netask == nil || netask.Ask == nil || netask.Ask.Ask == nil {
		return nil, fmt.Errorf("miner ask has not been properly set")
	}

	nmsa := toDBAsk(netask)
	nmsa.UpdatedAt = time.Now()
	nmsa.MinerVersion = minerVersion

	if err := mm.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "miner"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"price", "verified_price", "min_piece_size", "updated_at", "miner_version"}),
	}).Create(nmsa).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	return nmsa, nil
}

func toDBAsk(netask *network.AskResponse) *model.MinerStorageAsk {
	return &model.MinerStorageAsk{
		Miner:               netask.Ask.Ask.Miner.String(),
		Price:               netask.Ask.Ask.Price.String(),
		VerifiedPrice:       netask.Ask.Ask.VerifiedPrice.String(),
		MinPieceSize:        netask.Ask.Ask.MinPieceSize,
		MaxPieceSize:        netask.Ask.Ask.MaxPieceSize,
		PriceBigInt:         netask.Ask.Ask.Price,
		VerifiedPriceBigInt: netask.Ask.Ask.VerifiedPrice,
	}
}
