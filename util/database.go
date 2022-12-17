package util

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type DbAddrInfo struct {
	AddrInfo peer.AddrInfo
}

func (dba *DbAddrInfo) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("DbAddrInfo must be bytes")
	}

	if len(b) == 0 {
		return nil
	}

	var addrInfo peer.AddrInfo
	if err := json.Unmarshal(b, &addrInfo); err != nil {
		return err
	}

	dba.AddrInfo = addrInfo
	return nil
}

func (dba DbAddrInfo) Value() (driver.Value, error) {
	return dba.AddrInfo.MarshalJSON()
}

type DbAddr struct {
	Addr address.Address
}

func (dba *DbAddr) Scan(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("DbAddrs must be strings")
	}

	addr, err := address.NewFromString(s)
	if err != nil {
		return err
	}

	dba.Addr = addr
	return nil
}

func (dba DbAddr) Value() (driver.Value, error) {
	return dba.Addr.String(), nil
}

type DbCID struct {
	CID cid.Cid
}

func (dbc *DbCID) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("dbcids must get bytes!")
	}

	if len(b) == 0 {
		return nil
	}

	c, err := cid.Cast(b)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}

func (dbc DbCID) Value() (driver.Value, error) {
	return dbc.CID.Bytes(), nil
}

func (dbc DbCID) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbc.CID.String())
}

func (dbc *DbCID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	c, err := cid.Decode(s)
	if err != nil {
		return err
	}

	dbc.CID = c
	return nil
}
