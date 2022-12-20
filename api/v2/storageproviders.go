package api

import (
	"encoding/hex"
	"net/http"
	"time"

	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/labstack/echo/v4"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *apiV2) handleAddStorageProvider(c echo.Context) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	name := c.QueryParam("name")
	if err := s.DB.Clauses(&clause.OnConflict{UpdateAll: true}).Create(&model.StorageMiner{
		Address: util.DbAddr{Addr: m},
		Name:    name,
	}).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]string{})
}

func (s *apiV2) handleRemoveStorageProvider(c echo.Context) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	if err := s.DB.Unscoped().Where("address = ?", m.String()).Delete(&model.StorageMiner{}).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleSuspendStorageProvider godoc
// @Summary      Suspend Storage Provider
// @Description  This endpoint lets a user suspend a storage provider.
// @Tags         sp
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      miner.SuspendMinerBody  true   "Suspend Storage Provider Body"
// @Param        sp           path      string  true   "Storage Provider to suspend"
// @Router       /storage-providers/suspend/{sp} [post]
func (s *apiV2) handleSuspendStorageProvider(c echo.Context, u *util.User) error {
	var body miner.SuspendMinerBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	if err := s.minerManager.SuspendMiner(m, body, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleUnsuspendStorageProvider godoc
// @Summary      Unuspend Storage Provider
// @Description  This endpoint lets a user unsuspend a Storage Provider.
// @Tags         sp
// @Produce      json
// @Success      200
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        sp           path      string  true   "Storage Provider to unsuspend"
// @Router      /storage-providers/unsuspend/{sp} [put]
func (s *apiV2) handleUnsuspendStorageProvider(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	if err := s.minerManager.UnSuspendMiner(m, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleStorageProvidersSetInfo godoc
// @Summary      Set Storage Provider Info
// @Description  This endpoint lets a user set storage provider info.
// @Tags         sp
// @Produce      json
// @Success      200
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        params           body      miner.MinerSetInfoParams  true   "Storage Provider set info params"
// @Param        sp           path      string  true   "Storage Provider to set info for"
// @Router       /storage-providers/set-info/{sp} [put]
func (s *apiV2) handleStorageProvidersSetInfo(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	var params miner.MinerSetInfoParams
	if err := c.Bind(&params); err != nil {
		return err
	}

	if err := s.minerManager.SetMinerInfo(m, params, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

type storageProviderResp struct {
	Addr            address.Address `json:"addr"`
	Name            string          `json:"name"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason,omitempty"`
	Version         string          `json:"version"`
}

// handleGetStorageProviders godoc
// @Summary      Get all storage providers
// @Description  This endpoint returns all storage providers
// @Tags         sp
// @Produce      json
// @Success      200  {object}  []storageProviderResp
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /storage-providers [get]
func (s *apiV2) handleGetStorageProviders(c echo.Context) error {
	var miners []model.StorageMiner
	if err := s.DB.Find(&miners).Error; err != nil {
		return err
	}

	out := make([]storageProviderResp, len(miners))
	for i, m := range miners {
		out[i].Addr = m.Address.Addr
		out[i].Suspended = m.Suspended
		out[i].SuspendedReason = m.SuspendedReason
		out[i].Name = m.Name
		out[i].Version = m.Version
	}

	return c.JSON(http.StatusOK, out)
}

func (s *apiV2) handleGetStorageProviderDealStats(c echo.Context) error {
	sml, err := s.minerManager.ComputeSortedMinerList()
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, sml)
}

func (s *apiV2) handleStorageProviderTransferDiagnostics(c echo.Context) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	minerTransferDiagnostics, err := s.FilClient.MinerTransferDiagnostics(c.Request().Context(), m)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, minerTransferDiagnostics)
}

// handleGetStorageProviderFailures godoc
// @Summary      Get all storage providers
// @Description  This endpoint returns all storage providers
// @Tags         sp
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        sp  path      string  true  "Filter by storage provider"
// @Router       /storage-providers/failures/{sp} [get]
func (s *apiV2) handleGetStorageProviderFailures(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	var merrs []model.DfeRecord
	if err := s.DB.Limit(1000).Order("created_at desc").Find(&merrs, "miner = ?", maddr.String()).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, merrs)
}

type storageProviderDealsResp struct {
	ID               uint       `json:"id"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	Content          uint       `json:"content"`
	PropCid          util.DbCID `json:"propCid"`
	Miner            string     `json:"miner"`
	DealID           int64      `json:"dealId"`
	Failed           bool       `json:"failed"`
	Verified         bool       `json:"verified"`
	FailedAt         time.Time  `json:"failedAt,omitempty"`
	DTChan           string     `json:"dtChan"`
	TransferStarted  time.Time  `json:"transferStarted"`
	TransferFinished time.Time  `json:"transferFinished"`
	OnChainAt        time.Time  `json:"onChainAt"`
	SealedAt         time.Time  `json:"sealedAt"`
	ContentCid       util.DbCID `json:"contentCid"`
}

// handleGetStorageProviderDeals godoc
// @Summary      Get all storage providers deals
// @Description  This endpoint returns all storage providers deals
// @Tags				 sp
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        sp          path      string  true   "Filter by storage provider"
// @Param        ignore-failed  query     string  false  "Ignore Failed"
// @Router       /storage-providers/deals/{sp} [get]
func (s *apiV2) handleGetStorageProviderDeals(c echo.Context) error {
	maddr, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	q := s.DB.Model(model.ContentDeal{}).Order("created_at desc").
		Joins("left join contents on contents.id = content_deals.content").
		Where("miner = ?", maddr.String())

	if c.QueryParam("ignore-failed") != "" {
		q = q.Where("not content_deals.failed")
	}

	var deals []storageProviderDealsResp
	if err := q.Select("contents.cid as content_cid, content_deals.*").Scan(&deals).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, deals)
}

type storageProviderStatsResp struct {
	Miner           address.Address `json:"miner"`
	Name            string          `json:"name"`
	Version         string          `json:"version"`
	UsedByEstuary   bool            `json:"usedByEstuary"`
	DealCount       int64           `json:"dealCount"`
	ErrorCount      int64           `json:"errorCount"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason"`

	ChainInfo *storageProviderChainInfo `json:"chainInfo"`
}

type storageProviderChainInfo struct {
	PeerID    string   `json:"peerId"`
	Addresses []string `json:"addresses"`

	Owner  string `json:"owner"`
	Worker string `json:"worker"`
}

// handleGetStorageProviderStats godoc
// @Summary      Get storage provider stats
// @Description  This endpoint returns storage provider stats
// @Tags				 sp
// @Produce      json
// @Success      200  {object}  string
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        sp  path      string  true  "Filter by storage provider"
// @Router       /storage-providers/stats/{sp} [get]
func (s *apiV2) handleGetStorageProviderStats(c echo.Context) error {
	ctx, span := s.tracer.Start(c.Request().Context(), "handleGetStorageProviderStats")
	defer span.End()

	maddr, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	minfo, err := s.Api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return err
	}

	ci := storageProviderChainInfo{
		Owner:  minfo.Owner.String(),
		Worker: minfo.Worker.String(),
	}

	if minfo.PeerId != nil {
		ci.PeerID = minfo.PeerId.String()
	}
	for _, a := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(a)
		if err != nil {
			return err
		}
		ci.Addresses = append(ci.Addresses, ma.String())
	}

	var m model.StorageMiner
	if err := s.DB.First(&m, "address = ?", maddr.String()).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return c.JSON(http.StatusOK, &storageProviderStatsResp{
				Miner:         maddr,
				UsedByEstuary: false,
			})
		}
		return err
	}

	var dealscount int64
	if err := s.DB.Model(&model.ContentDeal{}).Where("miner = ?", maddr.String()).Count(&dealscount).Error; err != nil {
		return err
	}

	var errorcount int64
	if err := s.DB.Model(&model.DfeRecord{}).Where("miner = ?", maddr.String()).Count(&errorcount).Error; err != nil {
		return err
	}

	return c.JSON(http.StatusOK, &storageProviderStatsResp{
		Miner:           maddr,
		UsedByEstuary:   true,
		DealCount:       dealscount,
		ErrorCount:      errorcount,
		Suspended:       m.Suspended,
		SuspendedReason: m.SuspendedReason,
		Name:            m.Name,
		Version:         m.Version,
		ChainInfo:       &ci,
	})
}

// handleStorageProviderQueryAsk godoc
// @Summary      Query Ask
// @Description  This endpoint returns the ask for a given CID
// @Tags         deals
// @Produce      json
// @Success      200    {object}  string
// @Failure      400   {object}  util.HttpError
// @Failure      500   {object}  util.HttpError
// @Param        cid  path      string  true  "CID"
// @router       /storage-providers/storage/query/{cid} [get]
func (s *apiV2) handleStorageProviderQueryAsk(c echo.Context) error {
	addr, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	ask, err := s.minerManager.GetAsk(c.Request().Context(), addr, 0)
	if err != nil {
		return c.JSON(500, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, ask)
}

type claimResponse struct {
	Success bool `json:"success"`
}

// handleClaimStorageProvider godoc
// @Summary      Claim Storage Provider
// @Description  This endpoint lets a user claim a storage provider
// @Tags         sp
// @Produce      json
// @Success      200  {object}  claimResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      miner.ClaimMinerBody  true   "Claim Storage Provider Body"
// @Router       /storage-providers/claim [post]
func (s *apiV2) handleClaimStorageProvider(c echo.Context, u *util.User) error {
	ctx := c.Request().Context()

	var cmb miner.ClaimMinerBody
	if err := c.Bind(&cmb); err != nil {
		return err
	}

	if err := s.minerManager.ClaimMiner(ctx, cmb, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, claimResponse{Success: true})
}

type claimMsgResponse struct {
	Hexmsg string `json:"hexmsg"`
}

// handleGetClaimStorageProviderMsg godoc
// @Summary      Get Claim Storage Provider
// @Description  This endpoint lets a user get the message in order to claim a storage provider
// @Tags         sp
// @Produce      json
// @Success      200    {object}  claimMsgResponse
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        sp  path     string  true  "Storage Provider claim message"
// @Router       /storage-providers/claim/{sp} [get]
func (s *apiV2) handleGetClaimStorageProviderMsg(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("sp"))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, claimMsgResponse{
		Hexmsg: hex.EncodeToString(s.minerManager.GetMsgForMinerClaim(m, u.ID)),
	})
}
