package server

import (
	"time"

	contentmanager "github.com/application-research/estuary/server/contentmanager"
	"github.com/application-research/estuary/util"
)

type DataExport struct {
	Version string
	Date    time.Time

	Version1 ExportVersion1
}

type ExportVersion1 struct {
	Contents []util.Content
	Deals    []contentmanager.ContentDeal
}

func (s *Server) exportUserData(uid uint) (*DataExport, error) {
	var contents []util.Content
	if err := s.DB.Find(&contents, "user_id = ?", uid).Error; err != nil {
		return nil, err
	}

	var conts []uint
	for _, c := range contents {
		conts = append(conts, c.ID)
	}

	var deals []contentmanager.ContentDeal
	if err := s.DB.Find(&deals, "content in ?", conts).Error; err != nil {
		return nil, err
	}

	return &DataExport{
		Version: "v0.0.1",
		Date:    time.Now(),
		Version1: ExportVersion1{
			Contents: contents,
			Deals:    deals,
		},
	}, nil
}
