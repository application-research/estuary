package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/urfave/cli/v2"

	"github.com/application-research/estuary/util"
)

var log = logging.Logger("shuttle-proxy")

type Proxy struct {
	ControllerUrl string
}

func main() {

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Value: ":3205",
		},
		&cli.StringFlag{
			Name:  "controller",
			Value: "https://api.estuary.tech",
		},
		&cli.BoolFlag{
			Name:  "logging",
			Value: true,
		},
	}
	app.Action = func(cctx *cli.Context) error {
		logging := cctx.Bool("logging")
		p := &Proxy{
			ControllerUrl: cctx.String("controller"),
		}

		// upload.estuary.tech
		// routes to whatever shuttle is best
		// this is entirely because you cant return a 302 to a file upload request
		e := echo.New()

		if logging {
			e.Use(middleware.Logger())
		}

		e.Use(middleware.CORS())

		e.HTTPErrorHandler = util.ErrorHandler

		e.POST("/content/add", p.handleContentAdd)
		e.POST("/content/add-car", p.handleAddCar)

		return e.Start(cctx.String("listen"))
	}

	app.RunAndExitOnError()
}

func (p *Proxy) getViewer(auth string) (*util.ViewerResponse, int, error) {
	req, err := http.NewRequest("GET", p.ControllerUrl+"/viewer", nil)
	if err != nil {
		return nil, 500, err
	}

	req.Header.Set("Authorization", "Bearer "+auth)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 500, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, resp.StatusCode, fmt.Errorf("auth check failed")
	}

	var rb util.ViewerResponse
	if err := json.NewDecoder(resp.Body).Decode(&rb); err != nil {
		return nil, 500, err
	}

	return &rb, 0, nil
}

func (p *Proxy) getEndpoints(c echo.Context) ([]string, error) {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return nil, err
	}

	view, code, err := p.getViewer(auth)
	if err != nil {
		// TODO: match error format of shuttles
		return nil, c.String(code, err.Error())
	}

	if len(view.Settings.UploadEndpoints) == 0 {
		return nil, fmt.Errorf("all upload endpoints are unavailable")
	}

	return view.Settings.UploadEndpoints, nil
}
func (p *Proxy) handleContentAdd(c echo.Context) error {
	eps, err := p.getEndpoints(c)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", eps[0], c.Request().Body)
	if err != nil {
		return err
	}

	req.Header = c.Request().Header.Clone()
	req.Header.Set("Shuttle-Proxy", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	c.Response().WriteHeader(resp.StatusCode)

	io.Copy(c.Response().Writer, resp.Body)

	return nil
}

func (p *Proxy) handleAddCar(c echo.Context) error {
	auth, err := util.ExtractAuth(c)
	if err != nil {
		return err
	}

	view, code, err := p.getViewer(auth)
	if err != nil {
		// TODO: match error format of shuttles
		return c.String(code, err.Error())
	}

	if len(view.Settings.UploadEndpoints) == 0 {
		log.Errorf("no upload endpoints")
		return c.JSON(500, map[string]string{
			"error": "all upload endpoints are unavailable",
		})
	}

	ep := view.Settings.UploadEndpoints[0]
	req, err := http.NewRequest("POST", ep, c.Request().Body)
	if err != nil {
		return err
	}

	req.Header = c.Request().Header.Clone()
	req.Header.Set("Shuttle-Proxy", "true")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	c.Response().WriteHeader(resp.StatusCode)

	io.Copy(c.Response().Writer, resp.Body)

	return nil
}
