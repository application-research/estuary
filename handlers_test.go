package main

import (
	"fmt"
	"github.com/application-research/estuary/node"
	"github.com/application-research/estuary/stagingbs"
	"github.com/application-research/estuary/util/gateway"
	"github.com/application-research/filclient"
	"github.com/filecoin-project/lotus/api"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/whyrusleeping/memo"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"os"
	"testing"
)

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

func TestServer_handleAdd(t *testing.T) {
	skipCI(t)
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAdd(tt.args.c, tt.args.u), fmt.Sprintf("handleAdd(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAddIpfs(t *testing.T) {
	skipCI(t)
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAddIpfs(tt.args.c, tt.args.u), fmt.Sprintf("handleAddIpfs(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}

func TestServer_handleAddCar(t *testing.T) {
	skipCI(t)
	type fields struct {
		tracer      trace.Tracer
		Node        *node.Node
		DB          *gorm.DB
		FilClient   *filclient.FilClient
		Api         api.Gateway
		CM          *ContentManager
		StagingMgr  *stagingbs.StagingBSMgr
		gwayHandler *gateway.GatewayHandler
		cacher      *memo.Cacher
	}
	type args struct {
		c echo.Context
		u *User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				tracer:      tt.fields.tracer,
				Node:        tt.fields.Node,
				DB:          tt.fields.DB,
				FilClient:   tt.fields.FilClient,
				Api:         tt.fields.Api,
				CM:          tt.fields.CM,
				StagingMgr:  tt.fields.StagingMgr,
				gwayHandler: tt.fields.gwayHandler,
				cacher:      tt.fields.cacher,
			}
			tt.wantErr(t, s.handleAddCar(tt.args.c, tt.args.u), fmt.Sprintf("handleAddCar(%v, %v)", tt.args.c, tt.args.u))
		})
	}
}
