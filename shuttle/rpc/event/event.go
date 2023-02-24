package event

import (
	"github.com/application-research/estuary/pinner/status"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// add new shuttle operation topic here, so estaury consumers can be registered for them
var MessageTopics = map[string]bool{
	OP_UpdatePinStatus:  true,
	OP_PinComplete:      true,
	OP_CommPComplete:    true,
	OP_CommPFailed:      true,
	OP_TransferStarted:  true,
	OP_TransferFinished: true,
	OP_TransferStatus:   true,
	OP_ShuttleUpdate:    true,
	OP_GarbageCheck:     true,
	OP_SplitComplete:    true,
	OP_SplitFailed:      true,
	OP_SanityCheck:      true,
}

// add new estuary command topic here, so shuttle consumers can be registered for them
var CommandTopics = map[string]bool{
	CMD_ComputeCommP:           true,
	CMD_AddPin:                 true,
	CMD_TakeContent:            true,
	CMD_AggregateContent:       true,
	CMD_StartTransfer:          true,
	CMD_PrepareForDataRequest:  true,
	CMD_CleanupPreparedRequest: true,
	CMD_ReqTxStatus:            true,
	CMD_SplitContent:           true,
	CMD_RetrieveContent:        true,
	CMD_UnpinContent:           true,
	CMD_RestartTransfer:        true,
}

type Hello struct {
	Host                  string
	PeerID                string
	DiskSpaceFree         int64
	Address               address.Address
	AddrInfo              peer.AddrInfo
	Private               bool
	ContentAddingDisabled bool
	QueueEngEnabled       bool
}

type Hi struct {
	QueueEngEnabled bool
}

type Command struct {
	Op           string
	Params       CmdParams
	TraceCarrier *TraceCarrier `json:",omitempty"`
	Handle       string
}

type Message struct {
	Op           string
	Params       MsgParams
	TraceCarrier *TraceCarrier `json:",omitempty"`
	Handle       string
}

// HasTraceCarrier returns true iff Command `c` contains a trace.
func (c *Command) HasTraceCarrier() bool {
	return !(c.TraceCarrier == nil)
}

type CmdParams struct {
	AddPin                 *AddPin                 `json:",omitempty"`
	ComputeCommP           *ComputeCommP           `json:",omitempty"`
	TakeContent            *TakeContent            `json:",omitempty"`
	AggregateContent       *AggregateContents      `json:",omitempty"`
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
	DBID   uint64
	UserId uint
	Cid    cid.Cid
	Peers  []*peer.AddrInfo
}

const CMD_TakeContent = "TakeContent"

type TakeContent struct {
	Contents []ContentFetch
}

const CMD_AggregateContent = "AggregateContent"

type AggregateContents struct {
	DBID     uint64
	UserID   uint
	Contents []AggregateContent
}

type AggregateContent struct {
	ID   uint64
	CID  cid.Cid
	Name string
}

const CMD_StartTransfer = "StartTransfer"

type StartTransfer struct {
	DealDBID  uint
	ContentID uint64
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
	Content uint64
	Size    int64
}

const CMD_RetrieveContent = "RetrieveContent"

type StorageDeal struct {
	Miner  address.Address
	DealID int64
}

type RetrieveContent struct {
	UserID  uint
	Content uint64
	Cid     cid.Cid
	Deals   []StorageDeal
}

const CMD_UnpinContent = "UnpinContent"

type UnpinContent struct {
	Contents []uint64
}

const CMD_RestartTransfer = "RestartTransfer"

type RestartTransfer struct {
	ChanID    datatransfer.ChannelID
	DealDBID  uint
	ContentID uint64
}

type ContentFetch struct {
	ID     uint64
	Cid    cid.Cid
	UserID uint
	Peers  []*peer.AddrInfo
}

// HasTraceCarrier returns true iff Message `m` contains a trace.
func (m *Message) HasTraceCarrier() bool {
	return !(m.TraceCarrier == nil)
}

type MsgParams struct {
	UpdatePinStatus  *UpdatePinStatus           `json:",omitempty"`
	PinComplete      *PinComplete               `json:",omitempty"`
	CommPComplete    *CommPComplete             `json:",omitempty"`
	CommPFailed      *CommPFailed               `json:",omitempty"`
	TransferStatus   *TransferStatus            `json:",omitempty"`
	TransferStarted  *TransferStartedOrFinished `json:",omitempty"`
	TransferFinished *TransferStartedOrFinished `json:",omitempty"`
	ShuttleUpdate    *ShuttleUpdate             `json:",omitempty"`
	GarbageCheck     *GarbageCheck              `json:",omitempty"`
	SplitComplete    *SplitComplete             `json:",omitempty"`
	SplitFailed      *SplitFailed               `json:",omitempty"`
	SanityCheck      *SanityCheck               `json:",omitempty"`
}

const OP_UpdatePinStatus = "UpdateContentPinStatus"

type UpdatePinStatus struct {
	DBID   uint64
	Status status.PinningStatus
}

type PinObj struct {
	Cid  cid.Cid
	Size uint64
}

const OP_PinComplete = "PinComplete"

type PinComplete struct {
	DBID    uint64
	Size    int64
	CID     cid.Cid
	Objects []PinObj
}

const OP_CommPComplete = "CommPComplete"

type CommPComplete struct {
	Data    cid.Cid
	CommP   cid.Cid
	CarSize uint64
	Size    abi.UnpaddedPieceSize
}

const OP_CommPFailed = "OP_CommPFailed"

type CommPFailed struct {
	Data cid.Cid
}

const OP_TransferStarted = "TransferStarted"
const OP_TransferFinished = "TransferFinished"

type TransferStartedOrFinished struct {
	DealDBID uint
	Chanid   string
	State    *filclient.ChannelState
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
	Contents []uint64
}

const OP_SplitComplete = "SplitComplete"

type SplitComplete struct {
	ID uint64
}

const OP_SplitFailed = "OP_SplitFailed"

type SplitFailed struct {
	ID uint64
}

const OP_SanityCheck = "SanityCheck"

type SanityCheck struct {
	CID    cid.Cid
	ErrMsg string
}
