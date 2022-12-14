package util

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/time/rate"
)

func ConfigureRateLimiter(rateLimit rate.Limit) middleware.RateLimiterConfig {
	config := middleware.DefaultRateLimiterConfig

	config.IdentifierExtractor = func(ctx echo.Context) (string, error) {
		var id string
		user, ok := ctx.Get("user").(User)
		if !ok {
			id = ctx.RealIP()
		} else {
			id = user.UUID
		}
		return id, nil
	}

	config.Store = middleware.NewRateLimiterMemoryStore(rateLimit)

	return config
}
