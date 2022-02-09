package util

import (
	"github.com/ipfs/go-cid"
)

type ContentInCollection struct {
	Collection     string `json:"collection"`
	CollectionPath string `json:"collectionPath"`
}

type ContentAddIpfsBody struct {
	ContentInCollection

	Root  string   `json:"root"`
	Name  string   `json:"name"`
	Peers []string `json:"peers"`
}

type ContentAddResponse struct {
	Cid       string   `json:"cid"`
	EstuaryId uint     `json:"estuaryId"`
	Providers []string `json:"providers"`
}

type ContentCreateBody struct {
	ContentInCollection

	Root     cid.Cid `json:"root"`
	Name     string  `json:"name"`
	Location string  `json:"location"`
}

type ContentCreateResponse struct {
	ID uint `json:"id"`
}
