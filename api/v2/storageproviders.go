package api

import (
	"net/http"

	"github.com/application-research/estuary/miner"
	"github.com/application-research/estuary/model"
	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/labstack/echo/v4"
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

// handleGetStorageProvider godoc
// @Summary      Get all storage providers
// @Description  This endpoint returns all storage providers
// @Tags         public,net
// @Produce      json
// @Success      200  {object}  []storageProviderResp
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /storage-providers [get]
func (s *apiV2) handleGetStorageProvider(c echo.Context) error {
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

func (s *apiV2) handleGetStorageProviderStats(c echo.Context) error {
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
