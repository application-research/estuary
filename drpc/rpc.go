package drpc

import (
	"github.com/application-research/estuary/pinner/types"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Hello struct {
	Host   string
	PeerID string

	DiskSpaceFree int64

	Address  address.Address
	AddrInfo peer.AddrInfo
	Private  bool
}

type Command struct {
	Op           string
	Params       CmdParams
	TraceCarrier *TraceCarrier `json:",omitempty"`
}

// HasTraceCarrier returns true iff Command `c` contains a trace.
func (c *Command) HasTraceCarrier() bool {
	return !(c.TraceCarrier == nil)
}

type CmdParams struct {
	AddPin                 *AddPin                 `json:",omitempty"`
	ComputeCommP           *ComputeCommP           `json:",omitempty"`
	TakeContent            *TakeContent            `json:",omitempty"`
	AggregateContent       *AggregateContent       `json:",omitempty"`
	StartTransfer          *StartTransfer          `json:",omitempty"`
	PrepareForDataRequest  *PrepareForDataRequest  `json:",omitempty"`
	CleanupPreparedRequest *CleanupPreparedRequest `json:",omitempty"`
	ReqTxStatus            *ReqTxStatus            `json:",omitempty"`
	SplitContent           *SplitContent           `json:",omitempty"`
	RetrieveContent        *RetrieveContent        `json:",omitempty"`
	UnpinContent           *UnpinContent           `json:",omitempty"`
	RestartTransfer        *RestartTransfer        `json:",omitempty"`
}

const CMD_ComputeCommP = "ComputeCommP"

type ComputeCommP struct {
	Data cid.Cid
}

const CMD_AddPin = "AddPin"

type AddPin struct {
	DBID   uint
	UserId uint
	Cid    cid.Cid
	Peers  []*peer.AddrInfo
}

const CMD_TakeContent = "TakeContent"

type TakeContent struct {
	Contents []ContentFetch
	Sources  []peer.AddrInfo
}

const CMD_AggregateContent = "AggregateContent"

type AggregateContent struct {
	DBID     uint
	UserID   uint
	Contents []uint
	Root     cid.Cid
	ObjData  []byte
}

const CMD_StartTransfer = "StartTransfer"

type StartTransfer struct {
	DealDBID  uint
	ContentID uint
	Miner     address.Address
	PropCid   cid.Cid
	DataCid   cid.Cid
}

const CMD_PrepareForDataRequest = "PrepareForDataRequest"

type PrepareForDataRequest struct {
	DealDBID    uint
	AuthToken   string
	ProposalCid cid.Cid
	PayloadCid  cid.Cid
	Size        uint64
}

const CMD_CleanupPreparedRequest = "CleanupPreparedRequest"

type CleanupPreparedRequest struct {
	DealDBID  uint
	AuthToken string
}

const CMD_ReqTxStatus = "ReqTxStatus"

type ReqTxStatus struct {
	DealDBID uint
	ChanID   string
}

const CMD_SplitContent = "SplitContent"

type SplitContent struct {
	Content uint
	Size    int64
}

const CMD_RetrieveContent = "RetrieveContent"

type StorageDeal struct {
	Miner  address.Address
	DealID int64
}

type RetrieveContent struct {
	Content uint
	Cid     cid.Cid
	Deals   []StorageDeal
}

const CMD_UnpinContent = "UnpinContent"

type UnpinContent struct {
	Contents []uint
}

const CMD_RestartTransfer = "RestartTransfer"

type RestartTransfer struct {
	ChanID datatransfer.ChannelID
}

type ContentFetch struct {
	ID     uint
	Cid    cid.Cid
	UserID uint
}

type Message struct {
	Op           string
	Params       MsgParams
	TraceCarrier *TraceCarrier `json:",omitempty"`
	Handle       string
}

// HasTraceCarrier returns true iff Message `m` contains a trace.
func (m *Message) HasTraceCarrier() bool {
	return !(m.TraceCarrier == nil)
}

type MsgParams struct {
	UpdatePinStatus *UpdatePinStatus `json:",omitempty"`
	PinComplete     *PinComplete     `json:",omitempty"`
	CommPComplete   *CommPComplete   `json:",omitempty"`
	TransferStatus  *TransferStatus  `json:",omitempty"`
	TransferStarted *TransferStarted `json:",omitempty"`
	ShuttleUpdate   *ShuttleUpdate   `json:",omitempty"`
	GarbageCheck    *GarbageCheck    `json:",omitempty"`
	SplitComplete   *SplitComplete   `json:",omitempty"`
}

const OP_UpdatePinStatus = "UpdatePinStatus"

type UpdatePinStatus struct {
	DBID   uint
	Status types.PinningStatus
}

type PinObj struct {
	Cid  cid.Cid
	Size int
}

const OP_PinComplete = "PinComplete"

type PinComplete struct {
	DBID uint
	Size int64

	Objects []PinObj
}

const OP_CommPComplete = "CommPComplete"

type CommPComplete struct {
	Data    cid.Cid
	CommP   cid.Cid
	CarSize uint64
	Size    abi.UnpaddedPieceSize
}

const OP_TransferStarted = "TransferStarted"

type TransferStarted struct {
	DealDBID uint
	Chanid   string
}

const OP_TransferStatus = "TransferStatus"

type TransferStatus struct {
	Message  string
	Chanid   string
	DealDBID uint
	State    *filclient.ChannelState
	Failed   bool
}

const OP_ShuttleUpdate = "ShuttleUpdate"

type ShuttleUpdate struct {
	BlockstoreSize uint64
	BlockstoreFree uint64
	NumPins        int64
	PinQueueSize   int
}

const OP_GarbageCheck = "GarbageCheck"

type GarbageCheck struct {
	Contents []uint
}

const OP_SplitComplete = "SplitComplete"

type SplitComplete struct {
	ID uint
}
