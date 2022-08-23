package util

import (
	"encoding/binary"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

type CarObject struct {
	Cid  cid.Cid
	Size uint64
}

func CalculateCarSize(data cid.Cid, objects []CarObject) (uint64, error) {
	// Calculate header size
	header := car.CarHeader{
		Roots:   []cid.Cid{data},
		Version: 1,
	}
	size, err := car.HeaderSize(&header)
	if err != nil {
		return 0, err
	}

	// Calculate size of each block + metadata
	buf := make([]byte, 8)
	for _, o := range objects {
		bz := o.Cid.Bytes()
		sum := uint64(len(bz))           // size of cid
		sum += o.Size                    // size of block data
		n := binary.PutUvarint(buf, sum) // size of var int of cid + block size
		size += sum + uint64(n)
	}

	return size, nil
}
