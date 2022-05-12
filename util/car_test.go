package util

import (
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-car"
	"github.com/stretchr/testify/require"
)

func TestCalculateCarSize(t *testing.T) {
	ctx := context.Background()

	ds := dss.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	rseed := 5
	rawSize := 2 * 1024 * 1024
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(rawSize))
	nd, err := ImportFile(dserv, source)
	require.NoError(t, err)

	payloadCid := nd.Cid()
	selectiveCar := car.NewSelectiveCar(
		ctx,
		bs,
		[]car.Dag{{Root: payloadCid, Selector: shared.AllSelector()}},
		car.TraverseLinksOnlyOnce(),
	)

	objects := make([]CarObject, 0, 0)
	preparedCar, err := selectiveCar.Prepare(func(block car.Block) error {
		objects = append(objects, CarObject{
			Cid:  block.BlockCID,
			Size: uint64(len(block.Data)),
		})
		return nil
	})
	require.NoError(t, err)

	err = preparedCar.Dump(ctx, io.Discard)
	require.NoError(t, err)

	size, err := CalculateCarSize(payloadCid, objects)
	require.NoError(t, err)
	require.Equal(t, int(preparedCar.Size()), int(size))
}
