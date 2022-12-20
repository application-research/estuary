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

func (s *apiV2) handleAdminAddMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
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

func (s *apiV2) handleAdminRemoveMiner(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.DB.Unscoped().Where("address = ?", m.String()).Delete(&model.StorageMiner{}).Error; err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleSuspendMiner godoc
// @Summary      Suspend Miner
// @Description  This endpoint lets a user suspend a miner.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        req           body      miner.SuspendMinerBody  true   "Suspend Miner Body"
// @Param        miner           path      string  true   "Miner to suspend"
// @Router       /miner/suspend/{miner} [post]
func (s *apiV2) handleSuspendMiner(c echo.Context, u *util.User) error {
	var body miner.SuspendMinerBody
	if err := c.Bind(&body); err != nil {
		return err
	}

	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.minerManager.SuspendMiner(m, body, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleUnsuspendMiner godoc
// @Summary      Unuspend Miner
// @Description  This endpoint lets a user unsuspend a miner.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        miner           path      string  true   "Miner to unsuspend"
// @Router       /miner/unsuspend/{miner} [put]
func (s *apiV2) handleUnsuspendMiner(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	if err := s.minerManager.UnSuspendMiner(m, u); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{})
}

// handleMinersSetInfo godoc
// @Summary      Set Miner Info
// @Description  This endpoint lets a user set miner info.
// @Tags         miner
// @Produce      json
// @Success      200  {object}  emptyResp
// @Failure      400  {object}  util.HttpError
// @Failure      500  {object}  util.HttpError
// @Param        params           body      miner.MinerSetInfoParams  true   "Miner set info params"
// @Param        miner           path      string  true   "Miner to set info for"
// @Router       /miner/set-info/{miner} [put]
func (s *apiV2) handleMinersSetInfo(c echo.Context, u *util.User) error {
	m, err := address.NewFromString(c.Param("miner"))
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

type minerResp struct {
	Addr            address.Address `json:"addr"`
	Name            string          `json:"name"`
	Suspended       bool            `json:"suspended"`
	SuspendedReason string          `json:"suspendedReason,omitempty"`
	Version         string          `json:"version"`
}

// handleAdminGetMiners godoc
// @Summary      Get all miners
// @Description  This endpoint returns all miners
// @Tags         public,net
// @Produce      json
// @Success      200  {object}  string
// @Failure      400           {object}  util.HttpError
// @Failure      500           {object}  util.HttpError
// @Router       /public/miners [get]
func (s *apiV2) handleAdminGetMiners(c echo.Context) error {
	var miners []model.StorageMiner
	if err := s.DB.Find(&miners).Error; err != nil {
		return err
	}

	out := make([]minerResp, len(miners))
	for i, m := range miners {
		out[i].Addr = m.Address.Addr
		out[i].Suspended = m.Suspended
		out[i].SuspendedReason = m.SuspendedReason
		out[i].Name = m.Name
		out[i].Version = m.Version
	}

	return c.JSON(http.StatusOK, out)
}

func (s *apiV2) handleAdminGetMinerStats(c echo.Context) error {
	sml, err := s.minerManager.ComputeSortedMinerList()
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, sml)
}

func (s *apiV2) handleMinerTransferDiagnostics(c echo.Context) error {
	m, err := address.NewFromString(c.Param("miner"))
	if err != nil {
		return err
	}

	minerTransferDiagnostics, err := s.FilClient.MinerTransferDiagnostics(c.Request().Context(), m)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, minerTransferDiagnostics)
}
