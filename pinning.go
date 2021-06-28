package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	ma "github.com/multiformats/go-multiaddr"
)

func (s *Server) pinContent(obj cid.Cid, name string, cols []*Collection, peers []ma.Multiaddr) (uint, error) {
	panic("NYI")

}

type ipfsPin struct {
	Cid     cid.Cid                `json:"cid"`
	Name    string                 `json:"name"`
	Origins []string               `json:"origins"`
	Meta    map[string]interface{} `json:"meta"`
}

type ipfsPinStatus struct {
	Requestid string                 `json:"requestid"`
	Status    string                 `json:"status"`
	Created   time.Time              `json:"created"`
	Pin       ipfsPin                `json:"pin"`
	Delegates []string               `json:"delegates"`
	Info      map[string]interface{} `json:"info"`
}

// pinning api /pins endpoint
func (s *Server) handleListPins(e echo.Context, u *User) error {
	_, span := s.tracer.Start(e.Request().Context(), "handleListPins")
	defer span.End()

	qcids := e.QueryParam("cids")
	qname := e.QueryParam("name")
	qstatus := e.QueryParam("status")
	qbefore := e.QueryParam("before")
	qafter := e.QueryParam("after")
	qlimit := e.QueryParam("limit")

	q := s.DB.Model(Content{})

	if qcids != "" {
		var cids []cid.Cid
		for _, cstr := range strings.Split(qcids, ",") {
			c, err := cid.Decode(cstr)
			if err != nil {
				return err
			}
			cids = append(cids, c)
		}

		q = q.Where("cid in ?", cids)
	}

	if qname != "" {
		q = q.Where("name = ?", qname)
	}

	if qbefore != "" {
		beftime, err := time.Parse(time.RFC3339, qbefore)
		if err != nil {
			return err
		}

		q = q.Where("createdAt <= ?", beftime)
	}

	if qafter != "" {
		aftime, err := time.Parse(time.RFC3339, qafter)
		if err != nil {
			return err
		}

		q = q.Where("createdAt > ?", aftime)
	}

	if qlimit != "" {
		limit, err := strconv.Atoi(qlimit)
		if err != nil {
			return err
		}
		q = q.Limit(limit)
	}

	var contents []Content
	if err := q.Scan(&contents).Error; err != nil {
		return err
	}

	if qstatus != "" {
		/*
		   - queued     # pinning operation is waiting in the queue; additional info can be returned in info[status_details]
		   - pinning    # pinning in progress; additional info can be returned in info[status_details]
		   - pinned     # pinned successfully
		   - failed     # pinning service was unable to finish pinning operation; additional info can be found in info[status_details]
		*/
		statuses := strings.Split(qstatus, ",")
		for _, s := range statuses {
			switch s {
			case "queued", "pinning", "pinned", "failed":
			default:
				return fmt.Errorf("unrecognized pin status in query: %q", s)
			}
		}

		return fmt.Errorf("filtering pins by status not yet supported, please bother whyrusleeping")
	}

	var out []ipfsPinStatus
	for _, c := range contents {
		st := ipfsPinStatus{
			Pin: ipfsPin{
				Cid:  c.Cid.CID,
				Name: c.Name,
			},
			Requestid: fmt.Sprint(c.ID),
			Status:    "pinned",
			Created:   c.CreatedAt,
		}

		out = append(out, st)
	}

	return e.JSON(200, map[string]interface{}{
		"count":   len(out),
		"results": out,
	})
}

/*
{

    "cid": "QmCIDToBePinned",
    "name": "PreciousData.pdf",
    "origins":

[

    "/ip4/203.0.113.142/tcp/4001/p2p/QmSourcePeerId",
    "/ip4/203.0.113.114/udp/4001/quic/p2p/QmSourcePeerId"

],
"meta":

    {
        "app_id": "99986338-1113-4706-8302-4420da6158aa"
    }

}
*/

func (s *Server) handleAddPin(e echo.Context, u *User) error {
	var pin ipfsPin
	if err := e.Bind(&pin); err != nil {
		return err
	}

	return nil
}
