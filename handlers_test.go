package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/application-research/estuary/util/gateway"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/whyrusleeping/memo"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel"
)

type MockServer struct {
	mock.Mock
	CM MockContentManager
}

type MockContentManager struct {
	mock.Mock
}

type PinningSuite struct {
	suite.Suite
	e          *echo.Echo
	pinning    *echo.Group
	mockServer MockServer
}

//TODO: add users

func (ps *PinningSuite) TestHandleListPins(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/pinning/pins", nil)
	rec := httptest.NewRecorder()
	c := ps.e.NewContext(req, rec)
	s := &Server{
		Node:        nd,
		Api:         api,
		StagingMgr:  sbmgr,
		tracer:      otel.Tracer("api"),
		cacher:      memo.NewCacher(),
		gwayHandler: gateway.NewGatewayHandler(nd.Blockstore),
	}
	// c.SetPath("/pinning/pins")
	c.SetParamNames("email")
	c.SetParamValues("jon@labstack.com")

	h := &handler{mockDB}

	// Assertions
	if assert.NoError(t, ps.mockServer.handleListPins(c)) {
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, userJSON, rec.Body.String())
	}
}

func TestPinning(t *testing.T) {
	suite.Run(t, new(PinningSuite))
}
