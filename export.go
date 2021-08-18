package main

import "time"

type DataExport struct {
	Version string
	Date    time.Time

	Version1 ExportVersion1
}

type ExportVersion1 struct {
	Contents []Content
	Deals    []contentDeal
}

func (s *Server) exportUserData(uid uint) (*DataExport, error) {
	// + GetContentsWithUserID
	var contents []Content
	if err := s.DB.Find(&contents, "user_id = ?", uid).Error; err != nil {
		return nil, err
	}

	var conts []uint
	for _, c := range contents {
		conts = append(conts, c.ID)
	}

	// + GetDealsByContentIDs
	var deals []contentDeal
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
