package filclient

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
)

type paychApiProvider struct {
	api.Gateway
	wallet *wallet.LocalWallet
	mp     *MsgPusher
}

func (a *paychApiProvider) MpoolPushMessage(ctx context.Context, msg *types.Message, maxFee *api.MessageSendSpec) (*types.SignedMessage, error) {
	return a.mp.MpoolPushMessage(ctx, msg, maxFee)
}

func (a *paychApiProvider) WalletHas(ctx context.Context, addr address.Address) (bool, error) {
	return a.wallet.WalletHas(ctx, addr)
}

func (a *paychApiProvider) WalletSign(ctx context.Context, addr address.Address, data []byte) (*crypto.Signature, error) {
	return a.wallet.WalletSign(ctx, addr, data, api.MsgMeta{Type: api.MTUnknown})
}
