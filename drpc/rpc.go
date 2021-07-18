package drpc

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Hello struct {
	Host   string
	PeerID string
}

type Command struct {
	Op     string
	Params CmdParams
}

type CmdParams struct {
	AddPinOp *AddPinOp
}

type AddPinOp struct {
	DBID   uint
	UserId uint
	Cid    cid.Cid
	Peers  []peer.AddrInfo
}

type Message struct {
	Op     string
	Params MsgParams
}

type MsgParams struct {
	UpdatePinStatus *UpdatePinStatus `json:",omitempty"`
	PinComplete     *PinComplete     `json:",omitempty"`
}

const OP_UpdatePinStatus = "UpdatePinStatus"

type UpdatePinStatus struct {
	DBID   uint
	Status string
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
