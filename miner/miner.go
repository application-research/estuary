package miner

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/application-research/estuary/model"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/labstack/gommon/log"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type miner struct {
	Address             address.Address
	DealProtocolVersion protocol.ID
	Ask                 *model.MinerStorageAsk
}

func (mm *MinerManager) randomMinerListForDeal(ctx context.Context, n int, pieceSize abi.PaddedPieceSize, exclude map[address.Address]bool, filterByPrice bool) ([]miner, error) {
	var dbminers []model.StorageMiner
	if err := mm.db.Find(&dbminers, "not suspended").Error; err != nil {
		return nil, err
	}

	out := make([]miner, 0)
	if len(dbminers) == 0 {
		return out, nil
	}

	rand.Shuffle(len(dbminers), func(i, j int) {
		dbminers[i], dbminers[j] = dbminers[j], dbminers[i]
	})

	for _, dbm := range dbminers {
		if len(out) >= n {
			break
		}

		if exclude[dbm.Address.Addr] {
			continue
		}

		proto, err := mm.GetDealProtocolForMiner(ctx, dbm.Address.Addr)
		if err != nil {
			log.Warnf("getting deal protocol for %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		ask, err := mm.GetAsk(ctx, dbm.Address.Addr, time.Minute*30)
		if err != nil {
			log.Errorf("getting ask from %s failed: %s", dbm.Address.Addr, err)
			continue
		}

		if filterByPrice {
			if ask.PriceIsTooHigh(mm.cfg.Deal.IsVerified) {
				continue
			}
		}

		if ask.SizeIsCloseEnough(pieceSize) {
			out = append(out, miner{Address: dbm.Address.Addr, DealProtocolVersion: proto, Ask: ask})
			exclude[dbm.Address.Addr] = true
		}
	}
	return out, nil
}

func (mm *MinerManager) updateMinerVersion(ctx context.Context, m address.Address) (string, error) {
	vers, err := mm.filClient.GetMinerVersion(ctx, m)
	if err != nil {
		return "", err
	}

	if vers != "" {
		if err := mm.db.Model(model.StorageMiner{}).Where("address = ?", m.String()).Update("version", vers).Error; err != nil {
			return "", err
		}
	}
	return vers, nil
}

func (mm *MinerManager) GetDealProtocolForMiner(ctx context.Context, miner address.Address) (protocol.ID, error) {
	proto, err := mm.filClient.DealProtocolForMiner(ctx, miner)
	if err != nil {
		return "", err
	}

	_, ok := mm.cfg.Deal.EnabledDealProtocolsVersions[proto]
	if !ok {
		return "", fmt.Errorf("miner deal protocol:%s is not currently enabeld", proto)
	}
	return proto, nil
}
