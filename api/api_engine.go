package api

import (
	"runtime/pprof"
	"time"

	httpprof "net/http/pprof"

	"github.com/application-research/estuary/config"
	esmetrics "github.com/application-research/estuary/metrics"
	"github.com/application-research/estuary/util"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	echoSwagger "github.com/swaggo/echo-swagger"
	"go.opentelemetry.io/otel/trace"
)

type IRegister interface {
	RegisterRoutes(en *echo.Echo)
}

type ApiEngine struct {
	eng *echo.Echo
	cfg *config.Estuary
}

func NewEngine(cfg *config.Estuary, tcr trace.Tracer) *ApiEngine {
	e := echo.New()
	e.Binder = new(util.Binder)
	e.Pre(middleware.RemoveTrailingSlash())

	if cfg.Logging.ApiEndpointLogging {
		e.Use(middleware.Logger())
	}

	e.Use(util.TracingMiddleware(tcr))
	e.Use(util.AppVersionMiddleware(cfg.AppVersion))
	e.HTTPErrorHandler = util.ErrorHandler

	e.Use(middleware.Recover())

	e.GET("/debug/pprof/:prof", func(c echo.Context) error {
		httpprof.Handler(c.Param("prof")).ServeHTTP(c.Response().Writer, c.Request())
		return nil
	})
	e.GET("/debug/cpuprofile", func(c echo.Context) error {
		if err := pprof.StartCPUProfile(c.Response()); err != nil {
			return err
		}

		defer pprof.StopCPUProfile()

		select {
		case <-c.Request().Context().Done():
			return c.Request().Context().Err()
		case <-time.After(time.Second * 30):
		}
		return nil
	})

	phandle := promhttp.Handler()
	e.GET("/debug/metrics/prometheus", func(e echo.Context) error {
		phandle.ServeHTTP(e.Response().Writer, e.Request())
		return nil
	})

	exporter := esmetrics.Exporter()
	e.GET("/debug/metrics/opencensus", func(e echo.Context) error {
		exporter.ServeHTTP(e.Response().Writer, e.Request())
		return nil
	})

	e.Use(middleware.CORS())

	if !cfg.DisableSwaggerEndpoint {
		e.GET("/swagger/*", echoSwagger.WrapHandler)
	}
	return &ApiEngine{eng: e, cfg: cfg}
}

func (apiEng *ApiEngine) Start() error {
	return apiEng.eng.Start(apiEng.cfg.ApiListen)
}

func (apiEng *ApiEngine) RegisterAPI(api IRegister) {
	api.RegisterRoutes(apiEng.eng)
}
