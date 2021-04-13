package filclient

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"golang.org/x/xerrors"
)

// a simple nonce tracking message pusher that assumes it is the only thing with access to the given key
type MsgPusher struct {
	gapi api.Gateway
	w    *wallet.LocalWallet

	nlk    sync.Mutex
	nonces map[address.Address]uint64
}

func NewMsgPusher(gapi api.Gateway, w *wallet.LocalWallet) *MsgPusher {
	return &MsgPusher{
		gapi:   gapi,
		w:      w,
		nonces: make(map[address.Address]uint64),
	}
}

func (mp *MsgPusher) MpoolPushMessage(ctx context.Context, msg *types.Message, maxFee *api.MessageSendSpec) (*types.SignedMessage, error) {
	mp.nlk.Lock()
	defer mp.nlk.Unlock()

	kaddr, err := mp.gapi.StateAccountKey(ctx, msg.From, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	n, ok := mp.nonces[kaddr]
	if !ok {
		act, err := mp.gapi.StateGetActor(ctx, kaddr, types.EmptyTSK)
		if err != nil {
			return nil, err
		}

		n = act.Nonce
		mp.nonces[kaddr] = n
	}

	msg.Nonce = n

	estim, err := mp.gapi.GasEstimateMessageGas(ctx, msg, &api.MessageSendSpec{}, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("failed to estimate gas: %w", err)
	}

	estim.GasFeeCap = abi.NewTokenAmount(4000000000)
	estim.GasPremium = big.Mul(estim.GasPremium, big.NewInt(2))

	sig, err := mp.w.WalletSign(ctx, kaddr, estim.Cid().Bytes(), api.MsgMeta{Type: api.MTChainMsg})
	if err != nil {
		return nil, err
	}

	smsg := &types.SignedMessage{
		Message:   *estim,
		Signature: *sig,
	}

	_, err = mp.gapi.MpoolPush(ctx, smsg)
	if err != nil {
		return nil, err
	}

	mp.nonces[kaddr]++

	return smsg, nil
}
