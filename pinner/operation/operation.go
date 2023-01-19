package operation

import (
	"sync"
	"time"

	"github.com/application-research/estuary/pinner/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type AddrInfoString struct {
	ID    string
	Addrs []string
}

func SerializePeers(peers []*peer.AddrInfo) []AddrInfoString {
	adInfos := []AddrInfoString{}
	for _, p := range peers {
		adInfo := AddrInfoString{ID: p.ID.String()}
		for _, a := range p.Addrs {
			if a != nil {
				adInfo.Addrs = append(adInfo.Addrs, a.String())
			}
		}
		adInfos = append(adInfos, adInfo)
	}
	return adInfos
}

func UnSerializePeers(prs []AddrInfoString) []*peer.AddrInfo {
	peers := make([]*peer.AddrInfo, 0)
	for _, pr := range prs {
		pID, err := peer.Decode(pr.ID)
		if err != nil {
			continue // do not fail pinning because of peers
		}

		addrs := make([]multiaddr.Multiaddr, 0)
		for _, addr := range pr.Addrs {
			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				continue // do not fail pinning because of peers
			}
			addrs = append(addrs, maddr)
		}
		peers = append(peers, &peer.AddrInfo{ID: pID, Addrs: addrs})
	}
	return peers
}

// TODO: some of these fields are overkill for the generalized pin manager
// thing, but are still in use by the primary estuary node. Should probably
// find a way to decouple this better
type PinningOperation struct {
	Obj   cid.Cid
	Name  string
	Peers []AddrInfoString
	Meta  string

	Status types.PinningStatus

	UserId  uint
	ContId  uint64
	Replace uint

	LastUpdate time.Time

	Started     time.Time
	NumFetched  int
	SizeFetched int64
	FetchErr    error
	EndTime     time.Time

	Location string

	SkipLimiter bool

	lk sync.Mutex

	MakeDeal bool
}

func (po *PinningOperation) Fail(err error) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.FetchErr = err
	po.EndTime = time.Now()
	po.Status = types.PinningStatusFailed
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) SetStatus(st types.PinningStatus) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.Status = st
	po.LastUpdate = time.Now()
}

func (po *PinningOperation) Complete() {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.EndTime = time.Now()
	po.LastUpdate = time.Now()
	po.Status = types.PinningStatusPinned
}

func (po *PinningOperation) UpdateProgress(size int64) {
	po.lk.Lock()
	defer po.lk.Unlock()

	po.NumFetched++
	po.SizeFetched += size
}
