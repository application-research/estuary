package util

import (
	"github.com/labstack/echo/v4"
)

func AppVersionMiddleware(appVersion string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Response().Header().Set("X-AppVersion", appVersion)
			return next(c)
		}
	}
}
