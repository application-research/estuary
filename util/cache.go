package util

import (
	"fmt"
	"strings"

	"github.com/labstack/echo/v4"
)

// cacheKey returns a key based on the request being made, the user associated to it, and optional tags
// this key is used when calling Get or Add from a cache
func CacheKey(c echo.Context, u *User, tags ...string) string {
	paramNames := strings.Join(c.ParamNames(), ",")
	paramVals := strings.Join(c.ParamValues(), ",")
	tagsString := strings.Join(tags, ",")
	if u != nil {
		return fmt.Sprintf("URL=%s ParamNames=%s ParamVals=%s user=%d tags=%s", c.Request().URL, paramNames, paramVals, u.ID, tagsString)
	} else {
		return fmt.Sprintf("URL=%s ParamNames=%s ParamVals=%s tags=%s", c.Request().URL, paramNames, paramVals, tagsString)
	}
}
