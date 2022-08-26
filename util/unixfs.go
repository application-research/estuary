package util

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cidutil"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfs "github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
)

// TODO: change to Blake3 eventually
var DefaultHashFunction = uint64(mh.SHA2_256)

/// Prepare a file to be addressed by IPFS.
func ImportFile(dserv ipld.DAGService, fi io.Reader) (ipld.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = DefaultHashFunction

	spl := chunker.NewSizeSplitter(fi, 1024*1024)
	dbp := ihelper.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: dserv,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}

func TryExtractFSNode(nd ipld.Node) (*unixfs.FSNode, error) {
	switch nd := nd.(type) {
	case *merkledag.ProtoNode:
		n, err := unixfs.FSNodeFromBytes(nd.Data())
		if err != nil {
			return nil, err
		}
		if n.Type() == unixfs.TSymlink {
			return nil, fmt.Errorf("symlinks not supported")
		}
		return n, nil // success!
	case *merkledag.RawNode:
	default:
		return nil, errors.New("unknown node type")
	}
	return nil, errors.New("unknown node type")
}
