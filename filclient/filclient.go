package filclient

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	cario "github.com/filecoin-project/go-commp-utils/pieceio/cario"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	gst "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/clientutils"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/requestvalidation"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/paych"
	rpcstmgr "github.com/filecoin-project/lotus/chain/stmgr/rpc"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	paychmgr "github.com/filecoin-project/lotus/paychmgr"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	graphsync "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	storeutil "github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/host"
	inet "github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/xerrors"

	commp "github.com/filecoin-project/go-fil-commp-hashhash"

	cborutil "github.com/filecoin-project/go-cbor-util"
)

var Tracer trace.Tracer = otel.Tracer("filclient")

var log = logging.Logger("filclient")

const DealProtocol = "/fil/storage/mk/1.1.0"
const QueryAskProtocol = "/fil/storage/ask/1.1.0"
const DealStatusProtocol = "/fil/storage/status/1.1.0"
const RetrievalQueryProtocol = "/fil/retrieval/qry/1.0.0"

type FilClient struct {
	mpusher *MsgPusher

	pchmgr *paychmgr.Manager

	host host.Host

	api api.Gateway

	wallet *wallet.LocalWallet

	ClientAddr address.Address

	blockstore blockstore.Blockstore

	dataTransfer datatransfer.Manager

	computePieceComm GetPieceCommFunc

	RetrievalProgressLogging bool
}

type GetPieceCommFunc func(ctx context.Context, payloadCid cid.Cid, bstore blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error)

func NewClient(h host.Host, api api.Gateway, w *wallet.LocalWallet, addr address.Address, bs blockstore.Blockstore, ds datastore.Batching, ddir string) (*FilClient, error) {
	ctx, shutdown := context.WithCancel(context.Background())

	mpusher := NewMsgPusher(api, w)

	smapi := rpcstmgr.NewRPCStateManager(api)

	pchds := namespace.Wrap(ds, datastore.NewKey("paych"))
	store := paychmgr.NewStore(pchds)

	papi := &paychApiProvider{
		Gateway: api,
		wallet:  w,
		mp:      mpusher,
	}

	pchmgr := paychmgr.NewManager(ctx, shutdown, smapi, store, papi)
	if err := pchmgr.Start(); err != nil {
		return nil, err
	}

	gse := graphsync.New(context.Background(),
		gsnet.NewFromLibp2pHost(h),
		storeutil.LoaderForBlockstore(bs),
		storeutil.StorerForBlockstore(bs),
		graphsync.MaxInProgressRequests(200),
	)

	tpt := gst.NewTransport(h.ID(), gse)
	dtn := dtnet.NewFromLibp2pHost(h)

	dtRestartConfig := dtimpl.ChannelRestartConfig(channelmonitor.Config{
		/* old values for old stuff
		MonitorPushChannels:    true,
		AcceptTimeout:          time.Second * 30,
		MinBytesTransferred:    4 << 10,
		ChecksPerInterval:      20,
		RestartBackoff:         time.Second * 20,
		MaxConsecutiveRestarts: 10,
		CompleteTimeout:        time.Second * 90,
		*/

		AcceptTimeout:          time.Minute * 30,
		RestartDebounce:        time.Second * 10,
		RestartBackoff:         time.Second * 20,
		MaxConsecutiveRestarts: 15,
		//RestartAckTimeout:      time.Second * 30,
		CompleteTimeout: time.Minute * 40,

		// Called when a restart completes successfully
		//OnRestartComplete func(id datatransfer.ChannelID)
	})
	mgr, err := dtimpl.NewDataTransfer(ds, filepath.Join(ddir, "cidlistsdir"), dtn, tpt, dtRestartConfig)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&requestvalidation.StorageDataTransferVoucher{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&retrievalmarket.DealProposal{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&retrievalmarket.DealPayment{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherResultType(&retrievalmarket.DealResponse{})
	if err != nil {
		return nil, err
	}

	if err := mgr.Start(context.TODO()); err != nil {
		return nil, err
	}

	/*
		mgr.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
			fmt.Printf("[%s] event: %s %d %s %s\n", time.Now().Format("15:04:05"), event.Message, event.Code, datatransfer.Events[event.Code], event.Timestamp.Format("15:04:05"))
			fmt.Printf("channelstate: %s %s\n", datatransfer.Statuses[channelState.Status()], channelState.Message())
		})
	*/

	return &FilClient{
		host:             h,
		api:              api,
		wallet:           w,
		ClientAddr:       addr,
		blockstore:       bs,
		dataTransfer:     mgr,
		pchmgr:           pchmgr,
		mpusher:          mpusher,
		computePieceComm: GeneratePieceCommitment,
	}, nil
}

func (fc *FilClient) SetPieceCommFunc(pcf GetPieceCommFunc) {
	fc.computePieceComm = pcf
}

func (fc *FilClient) streamToMiner(ctx context.Context, maddr address.Address, protocol protocol.ID) (inet.Stream, error) {
	ctx, span := Tracer.Start(ctx, "streamToMiner", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	mpid, err := fc.connectToMiner(ctx, maddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to miner: %s", err)
	}

	s, err := fc.host.NewStream(ctx, mpid, protocol)
	if err != nil {
		return nil, xerrors.Errorf("failed to open stream to peer: %w", err)
	}

	return s, nil
}

type ApiError struct {
	Under error
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("api error: %s", e.Under)
}

func (e *ApiError) Unwrap() error {
	return e.Under
}

func (fc *FilClient) connectToMiner(ctx context.Context, maddr address.Address) (peer.ID, error) {
	minfo, err := fc.api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return "", &ApiError{err}
	}

	if minfo.PeerId == nil {
		return "", fmt.Errorf("miner %s has no peer ID set", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return "", fmt.Errorf("miner %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}

	if err := fc.host.Connect(ctx, peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}); err != nil {
		return "", err
	}

	return *minfo.PeerId, nil
}

func (fc *FilClient) GetMinerVersion(ctx context.Context, maddr address.Address) (string, error) {
	pid, err := fc.connectToMiner(ctx, maddr)
	if err != nil {
		return "", err
	}

	agent, err := fc.host.Peerstore().Get(pid, "AgentVersion")
	if err != nil {
		return "", nil
	}

	return agent.(string), nil
}

func (fc *FilClient) GetAsk(ctx context.Context, maddr address.Address) (*network.AskResponse, error) {
	ctx, span := Tracer.Start(ctx, "doGetAsk", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	s, err := fc.streamToMiner(ctx, maddr, QueryAskProtocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	areq := &network.AskRequest{maddr}

	if err := cborutil.WriteCborRPC(s, areq); err != nil {
		return nil, xerrors.Errorf("failed to send query ask request: %w", err)
	}

	var resp network.AskResponse
	if err := cborutil.ReadCborRPC(s, &resp); err != nil {
		return nil, xerrors.Errorf("failed to read query response: %w", err)
	}

	return &resp, nil
}

const epochsPerHour = 60 * 2

func ComputePrice(askPrice types.BigInt, size abi.PaddedPieceSize, duration abi.ChainEpoch) (*abi.TokenAmount, error) {
	cost := big.Mul(big.Div(big.Mul(big.NewInt(int64(size)), askPrice), big.NewInt(1<<30)), big.NewInt(int64(duration)))

	return (*abi.TokenAmount)(&cost), nil
}

func (fc *FilClient) MakeDeal(ctx context.Context, miner address.Address, data cid.Cid, price types.BigInt, minSize abi.PaddedPieceSize, duration abi.ChainEpoch, verified bool) (*network.Proposal, error) {
	ctx, span := Tracer.Start(ctx, "makeDeal", trace.WithAttributes(
		attribute.Stringer("miner", miner),
		attribute.Stringer("price", price),
		attribute.Int64("minSize", int64(minSize)),
		attribute.Int64("duration", int64(duration)),
		attribute.Stringer("cid", data),
	))
	defer span.End()

	commP, size, err := fc.computePieceComm(ctx, data, fc.blockstore)
	if err != nil {
		return nil, err
	}

	if size.Padded() < minSize {
		padded, err := ZeroPadPieceCommitment(commP, size, minSize.Unpadded())
		if err != nil {
			return nil, err
		}

		commP = padded
		size = minSize.Unpadded()
	}

	head, err := fc.api.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	collBounds, err := fc.api.StateDealProviderCollateralBounds(ctx, size.Padded(), verified, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	// set provider collateral 10% above minimum to avoid fluctuations causing deal failure
	provCol := big.Div(big.Mul(collBounds.Min, big.NewInt(11)), big.NewInt(10))

	// give miners a week to seal and commit the sector
	dealStart := head.Height() + (epochsPerHour * 24 * 7)

	end := dealStart + duration

	pricePerEpoch := big.Div(big.Mul(big.NewInt(int64(size.Padded())), price), big.NewInt(1<<30))

	label, err := clientutils.LabelField(data)
	if err != nil {
		return nil, xerrors.Errorf("failed to construct label field: %w", err)
	}

	proposal := &market.DealProposal{
		PieceCID:     commP,
		PieceSize:    size.Padded(),
		VerifiedDeal: verified,
		Client:       fc.ClientAddr,
		Provider:     miner,

		Label: label,

		StartEpoch: dealStart,
		EndEpoch:   end,

		StoragePricePerEpoch: pricePerEpoch,
		ProviderCollateral:   provCol,
		ClientCollateral:     big.Zero(),
	}

	raw, err := cborutil.Dump(proposal)
	if err != nil {
		return nil, err
	}
	sig, err := fc.wallet.WalletSign(ctx, fc.ClientAddr, raw, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, err
	}

	sigprop := &market.ClientDealProposal{
		Proposal:        *proposal,
		ClientSignature: *sig,
	}

	return &network.Proposal{
		DealProposal: sigprop,
		Piece: &storagemarket.DataRef{
			TransferType: storagemarket.TTGraphsync,
			Root:         data,
		},
		FastRetrieval: true,
	}, nil
}

func (fc *FilClient) SendProposal(ctx context.Context, netprop *network.Proposal) (*network.SignedResponse, error) {
	ctx, span := Tracer.Start(ctx, "sendProposal")
	defer span.End()

	s, err := fc.streamToMiner(ctx, netprop.DealProposal.Proposal.Provider, DealProtocol)
	if err != nil {
		return nil, xerrors.Errorf("opening stream to miner: %w", err)
	}

	defer s.Close()

	if err := cborutil.WriteCborRPC(s, netprop); err != nil {
		return nil, xerrors.Errorf("failed to write proposal to miner: %w", err)
	}

	var resp network.SignedResponse
	if err := cborutil.ReadCborRPC(s, &resp); err != nil {
		return nil, xerrors.Errorf("failed to read response from miner: %w", err)
	}

	return &resp, nil
}

func GeneratePieceCommitment(ctx context.Context, payloadCid cid.Cid, bstore blockstore.Blockstore) (cid.Cid, abi.UnpaddedPieceSize, error) {
	cario := cario.NewCarIO()
	preparedCar, err := cario.PrepareCar(context.Background(), bstore, payloadCid, shared.AllSelector())
	if err != nil {
		return cid.Undef, 0, err
	}

	writer := new(commp.Calc)
	err = preparedCar.Dump(writer)
	if err != nil {
		return cid.Undef, 0, err
	}

	commpc, size, err := writer.Digest()
	if err != nil {
		return cid.Undef, 0, err
	}

	commCid, err := commcid.DataCommitmentV1ToCID(commpc)
	if err != nil {
		return cid.Undef, 0, err
	}

	return commCid, abi.PaddedPieceSize(size).Unpadded(), nil
}

func ZeroPadPieceCommitment(c cid.Cid, curSize abi.UnpaddedPieceSize, toSize abi.UnpaddedPieceSize) (cid.Cid, error) {

	rawPaddedCommp, err := commp.PadCommP(
		// we know how long a pieceCid "hash" is, just blindly extract the trailing 32 bytes
		c.Hash()[len(c.Hash())-32:],
		uint64(curSize.Padded()),
		uint64(toSize.Padded()),
	)
	if err != nil {
		return cid.Undef, err
	}
	return commcid.DataCommitmentV1ToCID(rawPaddedCommp)
}

func (fc *FilClient) DealStatus(ctx context.Context, miner address.Address, propCid cid.Cid) (*storagemarket.ProviderDealState, error) {
	cidb, err := cborutil.Dump(propCid)
	if err != nil {
		return nil, err
	}

	sig, err := fc.wallet.WalletSign(ctx, fc.ClientAddr, cidb, api.MsgMeta{Type: api.MTUnknown})
	if err != nil {
		return nil, xerrors.Errorf("signing status request failed: %w", err)
	}

	req := &network.DealStatusRequest{
		Proposal:  propCid,
		Signature: *sig,
	}

	s, err := fc.streamToMiner(ctx, miner, DealStatusProtocol)
	if err != nil {
		return nil, err
	}

	if err := cborutil.WriteCborRPC(s, req); err != nil {
		return nil, xerrors.Errorf("failed to write status request: %w", err)
	}

	var resp network.DealStatusResponse
	if err := cborutil.ReadCborRPC(s, &resp); err != nil {
		return nil, xerrors.Errorf("reading response: %w", err)
	}

	// TODO: check the signatures and stuff?

	return &resp.DealState, nil
}

func (fc *FilClient) minerPeer(ctx context.Context, miner address.Address) (peer.ID, error) {
	minfo, err := fc.api.StateMinerInfo(ctx, miner, types.EmptyTSK)
	if err != nil {
		return "", err
	}
	if minfo.PeerId == nil {
		return "", fmt.Errorf("miner has no peer id")
	}

	return *minfo.PeerId, nil
}

func (fc *FilClient) minerOwner(ctx context.Context, miner address.Address) (address.Address, error) {
	minfo, err := fc.api.StateMinerInfo(ctx, miner, types.EmptyTSK)
	if err != nil {
		return address.Undef, err
	}
	if minfo.PeerId == nil {
		return address.Undef, fmt.Errorf("miner has no peer id")
	}

	return minfo.Owner, nil
}

type ChannelState struct {
	//datatransfer.Channel

	// SelfPeer returns the peer this channel belongs to
	SelfPeer   peer.ID `json:"selfPeer"`
	RemotePeer peer.ID `json:"remotePeer"`

	// Status is the current status of this channel
	Status    datatransfer.Status `json:"status"`
	StatusStr string              `json:"statusMessage"`

	// Sent returns the number of bytes sent
	Sent uint64 `json:"sent"`

	// Received returns the number of bytes received
	Received uint64 `json:"received"`

	// Message offers additional information about the current status
	Message string `json:"message"`

	BaseCid string `json:"baseCid"`

	ChannelID datatransfer.ChannelID `json:"channelId"`

	// Vouchers returns all vouchers sent on this channel
	//Vouchers []datatransfer.Voucher

	// VoucherResults are results of vouchers sent on the channel
	//VoucherResults []datatransfer.VoucherResult

	// LastVoucher returns the last voucher sent on the channel
	//LastVoucher datatransfer.Voucher

	// LastVoucherResult returns the last voucher result sent on the channel
	//LastVoucherResult datatransfer.VoucherResult

	// ReceivedCids returns the cids received so far on the channel
	//ReceivedCids []cid.Cid

	// Queued returns the number of bytes read from the node and queued for sending
	//Queued uint64
}

func ChannelStateConv(st datatransfer.ChannelState) *ChannelState {
	return &ChannelState{
		SelfPeer:   st.SelfPeer(),
		RemotePeer: st.OtherPeer(),
		Status:     st.Status(),
		StatusStr:  datatransfer.Statuses[st.Status()],
		Sent:       st.Sent(),
		Received:   st.Received(),
		Message:    st.Message(),
		BaseCid:    st.BaseCID().String(),
		ChannelID:  st.ChannelID(),
		//Vouchers:          st.Vouchers(),
		//VoucherResults:    st.VoucherResults(),
		//LastVoucher:       st.LastVoucher(),
		//LastVoucherResult: st.LastVoucherResult(),
		//ReceivedCids:      st.ReceivedCids(),
		//Queued:            st.Queued(),
	}
}

func (fc *FilClient) TransfersInProgress(ctx context.Context) (map[datatransfer.ChannelID]datatransfer.ChannelState, error) {
	return fc.dataTransfer.InProgressChannels(ctx)
}

func (fc *FilClient) TransferStatus(ctx context.Context, chanid *datatransfer.ChannelID) (*ChannelState, error) {
	st, err := fc.dataTransfer.ChannelState(ctx, *chanid)
	if err != nil {
		return nil, err
	}

	return ChannelStateConv(st), nil
}

var ErrNoTransferFound = fmt.Errorf("no transfer found")

func (fc *FilClient) TransferStatusForContent(ctx context.Context, content cid.Cid, miner address.Address) (*ChannelState, error) {
	start := time.Now()
	defer func() {
		log.Infof("check transfer status took: %s", time.Since(start))
	}()
	mpid, err := fc.minerPeer(ctx, miner)
	if err != nil {
		return nil, err
	}

	inprog, err := fc.dataTransfer.InProgressChannels(ctx)
	if err != nil {
		return nil, err
	}

	for chanid, state := range inprog {
		if chanid.Responder == mpid {
			if state.IsPull() {
				// this isnt a storage deal transfer...
				continue
			}
			if state.BaseCID() == content {
				return ChannelStateConv(state), nil
			}
		}
	}

	return nil, ErrNoTransferFound
}

func (fc *FilClient) RestartTransfer(ctx context.Context, chanid *datatransfer.ChannelID) error {
	return fc.dataTransfer.RestartDataTransferChannel(ctx, *chanid)
}

func (fc *FilClient) StartDataTransfer(ctx context.Context, miner address.Address, propCid cid.Cid, dataCid cid.Cid) (*datatransfer.ChannelID, error) {
	ctx, span := Tracer.Start(ctx, "startDataTransfer")
	defer span.End()

	mpid, err := fc.connectToMiner(ctx, miner)
	if err != nil {
		return nil, xerrors.Errorf("getting miner peer: %w", err)
	}

	voucher := &requestvalidation.StorageDataTransferVoucher{Proposal: propCid}

	fc.host.ConnManager().Protect(mpid, "transferring")

	chanid, err := fc.dataTransfer.OpenPushDataChannel(ctx, mpid, voucher, dataCid, shared.AllSelector())
	if err != nil {
		return nil, xerrors.Errorf("opening push data channel: %w", err)
	}

	return &chanid, nil
}

func (fc *FilClient) SubscribeToDataTransferEvents(f datatransfer.Subscriber) func() {
	return fc.dataTransfer.SubscribeToEvents(f)
}

type Balance struct {
	Account         address.Address `json:"account"`
	Balance         types.FIL       `json:"balance"`
	MarketEscrow    types.FIL       `json:"marketEscrow"`
	MarketLocked    types.FIL       `json:"marketLocked"`
	MarketAvailable types.FIL       `json:"marketAvailable"`

	VerifiedClientBalance *abi.StoragePower `json:"verifiedClientBalance"`
}

func (fc *FilClient) Balance(ctx context.Context) (*Balance, error) {
	act, err := fc.api.StateGetActor(ctx, fc.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	market, err := fc.api.StateMarketBalance(ctx, fc.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	vcstatus, err := fc.api.StateVerifiedClientStatus(ctx, fc.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	avail := types.BigSub(market.Escrow, market.Locked)

	return &Balance{
		Account:               fc.ClientAddr,
		Balance:               types.FIL(act.Balance),
		MarketEscrow:          types.FIL(market.Escrow),
		MarketLocked:          types.FIL(market.Locked),
		MarketAvailable:       types.FIL(avail),
		VerifiedClientBalance: vcstatus,
	}, nil
}

type LockFundsResp struct {
	MsgCid cid.Cid
}

func (fc *FilClient) LockMarketFunds(ctx context.Context, amt types.FIL) (*LockFundsResp, error) {

	act, err := fc.api.StateGetActor(ctx, fc.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	if types.BigCmp(types.BigInt(amt), act.Balance) > 0 {
		return nil, fmt.Errorf("not enough funds to add: %s < %s", types.FIL(act.Balance), amt)
	}

	encAddr, err := cborutil.Dump(&fc.ClientAddr)
	if err != nil {
		return nil, err
	}

	msg := &types.Message{
		From:   fc.ClientAddr,
		To:     builtin.StorageMarketActorAddr,
		Method: builtin.MethodsMarket.AddBalance,
		Value:  types.BigInt(amt),
		Params: encAddr,
		Nonce:  act.Nonce,
	}

	smsg, err := fc.mpusher.MpoolPushMessage(ctx, msg, &api.MessageSendSpec{})
	if err != nil {
		return nil, err
	}

	return &LockFundsResp{
		MsgCid: smsg.Cid(),
	}, nil
}

func (fc *FilClient) CheckChainDeal(ctx context.Context, dealid abi.DealID) (bool, *api.MarketDeal, error) {
	deal, err := fc.api.StateMarketStorageDeal(ctx, dealid, types.EmptyTSK)
	if err != nil {
		nfs := fmt.Sprintf("deal %d not found", dealid)
		if strings.Contains(err.Error(), nfs) {
			return false, nil, nil
		}

		return false, nil, err
	}

	return true, deal, nil
}

func (fc *FilClient) CheckOngoingTransfer(ctx context.Context, miner address.Address, st *ChannelState) (outerr error) {
	defer func() {
		// TODO: this is only here because for some reason restarting a data transfer can just panic
		// https://github.com/filecoin-project/go-data-transfer/issues/150
		if e := recover(); e != nil {
			outerr = fmt.Errorf("panic while checking transfer: %s", e)
		}
	}()
	// make sure we at least have an open connection to the miner
	if fc.host.Network().Connectedness(st.RemotePeer) != inet.Connected {
		// try reconnecting
		mpid, err := fc.connectToMiner(ctx, miner)
		if err != nil {
			return xerrors.Errorf("failed to reconnect to miner: %w", err)
		}

		if mpid != st.RemotePeer {
			return fmt.Errorf("miner peer ID is different than RemotePeer in data transfer channel")
		}
	}

	return fc.dataTransfer.RestartDataTransferChannel(ctx, st.ChannelID)

}

func (fc *FilClient) RetrievalQuery(ctx context.Context, maddr address.Address, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	ctx, span := Tracer.Start(ctx, "retrievalQuery", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	s, err := fc.streamToMiner(ctx, maddr, RetrievalQueryProtocol)
	if err != nil {
		return nil, err
	}

	q := &retrievalmarket.Query{
		PayloadCID: pcid,
	}

	if err := cborutil.WriteCborRPC(s, q); err != nil {
		return nil, err
	}

	var resp retrievalmarket.QueryResponse
	if err := cborutil.ReadCborRPC(s, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (fc *FilClient) getPaychWithMinFunds(ctx context.Context, dest address.Address) (address.Address, error) {

	avail, err := fc.pchmgr.AvailableFundsByFromTo(fc.ClientAddr, dest)
	if err != nil {
		return address.Undef, err
	}

	reqBalance, err := types.ParseFIL("0.01")
	if err != nil {
		return address.Undef, err
	}
	fmt.Println("available", avail.ConfirmedAmt)

	if types.BigCmp(avail.ConfirmedAmt, types.BigInt(reqBalance)) >= 0 {
		return *avail.Channel, nil
	}

	amount := abi.TokenAmount(types.BigMul(types.BigInt(reqBalance), types.NewInt(2)))

	fmt.Println("getting payment channel: ", fc.ClientAddr, dest, amount)
	pchaddr, mcid, err := fc.pchmgr.GetPaych(ctx, fc.ClientAddr, dest, amount)
	if err != nil {
		return address.Undef, xerrors.Errorf("failed to get payment channel: %w", err)
	}

	fmt.Println("got payment channel: ", pchaddr, mcid)
	if !mcid.Defined() {
		if pchaddr == address.Undef {
			return address.Undef, xerrors.Errorf("GetPaych returned nothing")
		}

		return pchaddr, nil
	}

	return fc.pchmgr.GetPaychWaitReady(ctx, mcid)
}

type RetrievalStats struct {
	Peer         peer.ID
	Size         uint64
	Duration     time.Duration
	AverageSpeed uint64
	TotalPayment abi.TokenAmount
	NumPayments  int
	AskPrice     abi.TokenAmount

	// TODO: we should be able to get this if we hook into the graphsync event stream
	//TimeToFirstByte time.Duration
}

func (fc *FilClient) RetrieveContent(ctx context.Context, miner address.Address, proposal *retrievalmarket.DealProposal) (*RetrievalStats, error) {
	log.Infof("starting retrieval with miner: %s", miner)

	ctx, span := Tracer.Start(ctx, "fcRetrieveContent")
	defer span.End()

	// Stats
	startTime := time.Now()
	totalPayment := abi.NewTokenAmount(0)

	mpID, err := fc.minerPeer(ctx, miner)
	if err != nil {
		return nil, xerrors.Errorf("failed to get miner peer: %w", err)
	}

	minerOwner, err := fc.minerOwner(ctx, miner)
	if err != nil {
		return nil, err
	}

	// Get the payment channel and create a lane for this retrieval
	pchAddr, err := fc.getPaychWithMinFunds(ctx, minerOwner)
	if err != nil {
		return nil, xerrors.Errorf("failed to get payment channel: %w", err)
	}
	pchLane, err := fc.pchmgr.AllocateLane(pchAddr)
	if err != nil {
		return nil, xerrors.Errorf("failed to allocate lane: %w", err)
	}

	var chanid datatransfer.ChannelID
	var chanidMu sync.Mutex

	// Set up incoming events handler

	// The next nonce (incrementing unique ID starting from 0) for the next voucher
	var nonce uint64 = 0

	// Used to limit prints to the console about received status
	lastReceivedUpdate := time.Now()

	// dtRes receives either an error (failure) or nil (success) which is waited
	// on and handled below before exiting the function
	dtRes := make(chan error, 1)

	finish := func(err error) {
		select {
		case dtRes <- err:
		default:
		}
	}

	unsubscribe := fc.dataTransfer.SubscribeToEvents(func(event datatransfer.Event, state datatransfer.ChannelState) {
		// Copy chanid so it can be used later in the callback
		chanidMu.Lock()
		chanidLk := chanid
		chanidMu.Unlock()

		// Skip all events that aren't related to this channel
		if state.ChannelID() != chanidLk {
			return
		}

		switch event.Code {
		case datatransfer.NewVoucherResult:
			switch resType := state.LastVoucherResult().(type) {
			case *retrievalmarket.DealResponse:
				switch resType.Status {
				case retrievalmarket.DealStatusAccepted:
					log.Info("deal accepted")

				// Respond with a payment voucher when funds are requested
				case retrievalmarket.DealStatusFundsNeeded:
					log.Infof("sending payment voucher (nonce: %v, amount: %v)", nonce, resType.PaymentOwed)

					totalPayment = types.BigAdd(totalPayment, resType.PaymentOwed)

					vres, err := fc.pchmgr.CreateVoucher(ctx, pchAddr, paych.SignedVoucher{
						ChannelAddr: pchAddr,
						Lane:        pchLane,
						Nonce:       nonce,
						Amount:      totalPayment,
					})
					if err != nil {
						finish(err)
					}

					if types.BigCmp(vres.Shortfall, big.NewInt(0)) > 0 {
						finish(xerrors.Errorf("not enough funds remaining in payment channel (shortfall = %s)", vres.Shortfall))
					}

					if err := fc.dataTransfer.SendVoucher(ctx, chanidLk, &retrievalmarket.DealPayment{
						ID:             proposal.ID,
						PaymentChannel: pchAddr,
						PaymentVoucher: vres.Voucher,
					}); err != nil {
						finish(xerrors.Errorf("failed to send payment voucher: %w", err))
					}

					nonce++
				case retrievalmarket.DealStatusFundsNeededUnseal:
					finish(xerrors.Errorf("received unexpected payment request for unsealing data"))
				default:
					log.Debugf("unrecognized voucher response status: %v", retrievalmarket.DealStatuses[resType.Status])
				}
			default:
				log.Debugf("unrecognized voucher response type: %v", resType)
			}
		case datatransfer.DataReceivedProgress:
			if fc.RetrievalProgressLogging && time.Since(lastReceivedUpdate) >= time.Millisecond*100 {
				fmt.Printf("received: %v\r", state.Received())
				lastReceivedUpdate = time.Now()
			}
		case datatransfer.DataReceived:
			// Ignore this
		case datatransfer.FinishTransfer:
			finish(nil)
		case datatransfer.Cancel:
			finish(xerrors.Errorf("data transfer canceled"))
		default:
			log.Debugf("unrecognized data transfer event: %v", datatransfer.Events[event.Code])
		}
	})
	defer unsubscribe()

	// Submit the retrieval deal proposal to the miner
	chanidMu.Lock()
	chanid, err = fc.dataTransfer.OpenPullDataChannel(ctx, mpID, proposal, proposal.PayloadCID, shared.AllSelector())
	chanidMu.Unlock()
	if err != nil {
		return nil, err
	}
	// Wait for the retrieval to finish before exiting the function
	select {
	case err := <-dtRes:
		if err != nil {
			return nil, xerrors.Errorf("data transfer error: %w", err)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Compile the retrieval stats

	state, err := fc.dataTransfer.ChannelState(ctx, chanid)
	if err != nil {
		return nil, xerrors.Errorf("could not get channel state: %w", err)
	}

	duration := time.Since(startTime)
	speed := uint64(float64(state.Received()) / duration.Seconds())

	return &RetrievalStats{
		Peer:         state.OtherPeer(),
		Size:         state.Received(),
		Duration:     duration,
		AverageSpeed: speed,
		TotalPayment: totalPayment,
		NumPayments:  int(nonce),
		AskPrice:     proposal.PricePerByte,
	}, nil
}
