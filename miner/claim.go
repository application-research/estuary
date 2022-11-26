package miner

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/sigs"
)

type ClaimMinerBody struct {
	Miner address.Address `json:"miner"`
	Claim string          `json:"claim"`
	Name  string          `json:"name"`
}

func (mm *MinerManager) ClaimMiner(ctx context.Context, params ClaimMinerBody, u *util.User) error {
	var sm []model.StorageMiner
	if err := mm.db.Find(&sm, "address = ?", params.Miner.String()).Error; err != nil {
		return err
	}

	minfo, err := mm.api.StateMinerInfo(ctx, params.Miner, types.EmptyTSK)
	if err != nil {
		return err
	}

	acckey, err := mm.api.StateAccountKey(ctx, minfo.Worker, types.EmptyTSK)
	if err != nil {
		return err
	}

	sigb, err := hex.DecodeString(params.Claim)
	if err != nil {
		return err
	}

	if len(sigb) < 2 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_SIG,
			Details: "miner claim string is below accepted lenght",
		}
	}

	sig := &crypto.Signature{
		Type: crypto.SigType(sigb[0]),
		Data: sigb[1:],
	}

	msg := mm.GetMsgForMinerClaim(params.Miner, u.ID)
	if err := sigs.Verify(sig, acckey, msg); err != nil {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_SIG,
			Details: err.Error(),
		}
	}

	if len(sm) == 0 {
		// This is a new miner, need to run some checks first
		if err := mm.checkNewMiner(ctx, minfo, params.Miner); err != nil {
			return err
		}

		return mm.db.Create(&model.StorageMiner{
			Address: util.DbAddr{Addr: params.Miner},
			Name:    params.Name,
			Owner:   u.ID,
		}).Error
	}
	return mm.db.Model(model.StorageMiner{}).Where("id = ?", sm[0].ID).UpdateColumn("owner", u.ID).Error
}

func (mm *MinerManager) GetMsgForMinerClaim(miner address.Address, uid uint) []byte {
	return []byte(fmt.Sprintf("---- user %d owns miner %s ----", uid, miner))
}

func (mm *MinerManager) checkNewMiner(ctx context.Context, minfo api.MinerInfo, addr address.Address) error {
	if minfo.PeerId == nil {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_NO_PEER_ID,
			Details: "miner has no peer ID set",
		}
	}

	if len(minfo.Multiaddrs) == 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_NO_MULTI_ADDR,
			Details: "miner has no addresses set on chain",
		}
	}

	pow, err := mm.api.StateMinerPower(ctx, addr, types.EmptyTSK)
	if err != nil {
		return fmt.Errorf("could not check miners power: %w", err)
	}

	if pow == nil {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_NO_POWER,
			Details: "no miner power details were found",
		}
	}

	if types.BigCmp(pow.MinerPower.QualityAdjPower, types.NewInt(1<<40)) < 0 {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_POWER_BELOW_1TIB,
			Details: "miner must have at least 1TiB of power to be considered by estuary",
		}
	}

	ask, err := mm.filClient.GetAsk(ctx, addr)
	if err != nil {
		return fmt.Errorf("failed to get ask from miner: %w", err)
	}

	if ask == nil || ask.Ask == nil || ask.Ask.Ask == nil {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_NO_ASK,
			Details: "miner ask has not been properly set",
		}
	}

	if !ask.Ask.Ask.VerifiedPrice.Equals(big.NewInt(0)) {
		return &util.HttpError{
			Code:    http.StatusBadRequest,
			Reason:  util.ERR_INVALID_MINER_CLAIM_ASK_VERIFIED_PRICE_IS_NOT_ZERO,
			Details: "miners verified deal price is not zero",
		}
	}
	return nil
}
