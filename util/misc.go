package util

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/application-research/filclient"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/labstack/echo/v4"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/trace"
)

func CanRestartTransfer(st *filclient.ChannelState) bool {
	switch st.Status {
	case datatransfer.Cancelled,
		datatransfer.Failed,
		datatransfer.Completed:
		return false
	default:
		return true
	}
}

func TransferFailed(st *filclient.ChannelState) (bool, string) {
	msg, _ := datatransfer.Statuses[st.Status]
	switch st.Status {
	case datatransfer.Cancelled, datatransfer.Failed:
		return true, msg
	default:
		return false, msg
	}
}

func ParseDealLabel(s string) (cid.Cid, error) {
	return cid.Decode(s)
}

func FilterUnwalkableLinks(links []*ipld.Link) []*ipld.Link {
	out := make([]*ipld.Link, 0, len(links))

	for _, l := range links {
		if CidIsUnwalkable(l.Cid) {
			continue
		}
		out = append(out, l)
	}

	return out
}

func CidIsUnwalkable(c cid.Cid) bool {
	pref := c.Prefix()
	if pref.MhType == multihash.IDENTITY {
		return true
	}

	if pref.Codec == cid.FilCommitmentSealed || pref.Codec == cid.FilCommitmentUnsealed {
		return true
	}

	return false
}

func ErrorIfContentAddingDisabled(isContentAddingDisabled bool) error {
	if isContentAddingDisabled {
		return &HttpError{
			Code:    http.StatusBadRequest,
			Reason:  ERR_CONTENT_ADDING_DISABLED,
			Details: "uploading content to this node is not allowed at the moment",
		}
	}
	return nil
}

// required for car uploads
func WithContentLengthCheck(f func(echo.Context) error) func(echo.Context) error {
	return func(c echo.Context) error {
		if c.Request().Header.Get("Content-Length") == "" {
			return &HttpError{
				Code:    http.StatusLengthRequired,
				Reason:  ERR_CONTENT_LENGTH_REQUIRED,
				Details: "uploading car content requires Content-Length header value to be set",
			}
		}
		return f(c)
	}
}

type Binder struct{}

func (b Binder) Bind(i interface{}, c echo.Context) error {
	defer c.Request().Body.Close()
	if err := json.NewDecoder(c.Request().Body).Decode(i); err != nil {
		return &HttpError{
			Code:    http.StatusBadRequest,
			Reason:  ERR_INVALID_INPUT,
			Details: fmt.Sprintf("one or more params has an invalid data type or not supported: %s", err),
		}
	}
	return nil
}

func JSONPayloadMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := checkContentType(c.Request().Header, "application/json"); err != nil {
			return err
		}
		return next(c)
	}
	}
}

func WithMultipartFormDataChecker(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := checkContentType(c.Request().Header, "multipart/form-data"); err != nil {
			return err
		}
		return next(c)
	}
}

func checkContentType(header http.Header, expectedContentType string) error {
	if header.Get("Content-Type") != expectedContentType {
		return &HttpError{
			Code: http.StatusUnsupportedMediaType,
			Reason: ERR_UNSUPPORTED_CONTENT_TYPE,
			Details: fmt.Sprintf("this endpoint only supports %s paylods", expectedContentType),
		}
	}
	return nil
}

func DumpBlockstoreTo(ctx context.Context, tc trace.Tracer, from, to blockstore.Blockstore) error {
	ctx, span := tc.Start(ctx, "blockstoreCopy")
	defer span.End()

	// TODO: smarter batching... im sure ive written this logic before, just gotta go find it
	keys, err := from.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	var batch []blocks.Block

	for k := range keys {
		blk, err := from.Get(ctx, k)
		if err != nil {
			return err
		}

		batch = append(batch, blk)

		if len(batch) > 500 {
			if err := to.PutMany(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := to.PutMany(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}
