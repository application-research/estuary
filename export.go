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
	contents, err := s.DB.Contents().WithUserID(uid).GetAll()
	if err != nil {
		return nil, err
	}

	var conts []uint
	for _, c := range contents {
		conts = append(conts, c.ID)
	}

	// + GetDealsByContentIDs
	deals, err := s.DB.Deals().WithContentIDs(conts).GetAll()
	if err != nil {
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
