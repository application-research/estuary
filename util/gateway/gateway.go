package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	gopath "path"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	mdagipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfs"
	uio "github.com/ipfs/go-unixfs/io"
	"github.com/ipfs/go-unixfsnode"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	ipldbasicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/schema"
	"golang.org/x/xerrors"
)

type GatewayHandler struct {
	bs       blockstore.Blockstore
	dserv    mdagipld.DAGService
	resolver resolver.Resolver
}

type httpError struct {
	Code    int
	Message string
}

func NewGatewayHandler(bs blockstore.Blockstore) *GatewayHandler {

	bsvc := blockservice.New(bs, nil)
	ipldFetcher := bsfetcher.NewFetcherConfig(bsvc)

	ipldFetcher.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return ipldbasicnode.Prototype.Any, nil
	})

	resolver := resolver.NewBasicResolver(ipldFetcher.WithReifier(unixfsnode.Reify))
	return &GatewayHandler{
		bs:       bs,
		dserv:    merkledag.NewDAGService(bsvc),
		resolver: resolver,
	}
}

func (gw *GatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := gw.handleRequest(r.Context(), w, r); err != nil {
		http.Error(w, "error: "+err.Error(), 500)
		return
	}
}

func (gw *GatewayHandler) handleRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cc, err := gw.resolvePath(ctx, r.URL.Path)
	if err != nil {
		return fmt.Errorf("path resolution failed: %w", err)
	}

	output := "unixfs"

	switch output {
	case "unixfs":
		return gw.serveUnixfs(ctx, cc, w, r)
	default:
		return fmt.Errorf("requested output type unsupported")
	}
}

func (gw *GatewayHandler) serveUnixfs(ctx context.Context, cc cid.Cid, w http.ResponseWriter, req *http.Request) error {
	nd, err := gw.dserv.Get(ctx, cc)
	if err != nil {
		return err
	}

	switch nd := nd.(type) {
	case *merkledag.ProtoNode:
		n, err := unixfs.FSNodeFromBytes(nd.Data())
		if err != nil {
			return err
		}
		if n.IsDir() {
			return gw.serveUnixfsDir(ctx, nd, w, req)
		}
		if n.Type() == unixfs.TSymlink {
			return fmt.Errorf("symlinks not supported")
		}
	case *merkledag.RawNode:
	default:
		return errors.New("unknown node type")
	}

	dr, err := uio.NewDagReader(ctx, nd, gw.dserv)
	if err != nil {
		return err
	}

	err = gw.sniffMimeType(w, dr)
	if err != nil {
		return err
	}

	http.ServeContent(w, req, cc.String(), time.Time{}, dr)
	return nil
}

func (gw *GatewayHandler) sniffMimeType(w http.ResponseWriter, dr uio.DagReader) error {
	// see kubo https://github.com/ipfs/kubo/blob/df222053856d3967ff0b4d6bc513bdb66ceedd6f/core/corehttp/gateway_handler_unixfs_file.go
	// see http ServeContent https://cs.opensource.google/go/go/+/refs/tags/go1.19.2:src/net/http/fs.go;l=221;drc=1f068f0dc7bc997446a7aac44cfc70746ad918e0

	// Calculate deterministic value for Content-Type HTTP header
	// (we prefer to do it here, rather than using implicit sniffing in http.ServeContent)
	var ctype string
	// uses https://github.com/gabriel-vasile/mimetype library to determine the content type.
	// Fixes https://github.com/ipfs/kubo/issues/7252
	mimeType, err := mimetype.DetectReader(dr)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
		return err
	}

	ctype = mimeType.String()
	_, err = dr.Seek(0, io.SeekStart)
	if err != nil {
		http.Error(w, "seeker can't seek", http.StatusInternalServerError)
		return err
	}
	// Strip the encoding from the HTML Content-Type header and let the
	// browser figure it out.
	//
	// Fixes https://github.com/ipfs/kubo/issues/2203
	if strings.HasPrefix(ctype, "text/html;") {
		ctype = "text/html"
	}
	// Setting explicit Content-Type to avoid mime-type sniffing on the client
	// (unifies behavior across gateways and web browsers)
	w.Header().Set("Content-Type", ctype)
	return nil
}

func (gw *GatewayHandler) serveUnixfsDir(ctx context.Context, n mdagipld.Node, w http.ResponseWriter, req *http.Request) error {
	// TODO: something less ugly
	dir, err := uio.NewDirectoryFromNode(gw.dserv, n)
	if err != nil {
		return err
	}

	nd, err := dir.Find(ctx, "index.html")
	switch {
	case err == nil:
		dr, err := uio.NewDagReader(ctx, nd, gw.dserv)
		if err != nil {
			return err
		}

		http.ServeContent(w, req, "index.html", time.Time{}, dr)
		return nil
	default:
		return err
	case xerrors.Is(err, os.ErrNotExist):

	}

	fmt.Fprintf(w, "<html><body><ul>")

	requestURI, err := url.ParseRequestURI(req.RequestURI)

	if err := dir.ForEachLink(ctx, func(lnk *mdagipld.Link) error {
		href := gopath.Join(requestURI.Path, lnk.Name)
		fmt.Fprintf(w, "<li><a href=\"%s\">%s</a></li>", href, lnk.Name)
		return nil
	}); err != nil {
		return err
	}

	fmt.Fprintf(w, "</ul></body></html>")
	return nil
}

func (gw *GatewayHandler) resolvePath(ctx context.Context, p string) (cid.Cid, error) {
	proto, _, _, err := ParsePath(p) // a sanity check
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to parse request path: %w", err)
	}

	pp, err := path.ParsePath(p)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to parse request path: %w", err)
	}

	cc, segs, err := gw.resolver.ResolveToLastNode(ctx, pp)
	if err != nil {
		return cid.Undef, err
	}

	switch proto {
	case "ipfs":
		if len(segs) > 0 {
			return cid.Undef, fmt.Errorf("pathing into ipld nodes not supported")
		}
		return cc, nil
	default:
		return cid.Undef, fmt.Errorf("unsupported protocol: %s", proto)
	}
}

func ParsePath(p string) (string, cid.Cid, []string, error) {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) < 2 {
		return "", cid.Undef, nil, fmt.Errorf("invalid gateway path")
	}

	protocol := parts[0]

	cc, err := cid.Decode(parts[1])
	if err != nil {
		return "", cid.Undef, nil, fmt.Errorf("invalid cid in path: %w", err)
	}

	return protocol, cc, parts[2:], nil
}
