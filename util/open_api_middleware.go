package util

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

func OpenApiMiddleware(log *zap.SugaredLogger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			err := next(c)
			if err == nil {
				return nil
			}

			var httpRespErr *HttpError
			if xerrors.As(err, &httpRespErr) {
				log.Errorf("handler error: %s", err)
				return c.JSON(httpRespErr.Code, &HttpErrorResponse{
					Error: HttpError{
						Reason:  httpRespErr.Reason,
						Details: httpRespErr.Details,
					},
				})
			}

			var echoErr *echo.HTTPError
			if xerrors.As(err, &echoErr) {
				return c.JSON(echoErr.Code, &HttpErrorResponse{
					Error: HttpError{
						Reason:  http.StatusText(echoErr.Code),
						Details: echoErr.Message.(string),
					},
				})
			}

			log.Errorf("handler error: %s", err)
			return c.JSON(http.StatusInternalServerError, &HttpErrorResponse{
				Error: HttpError{
					Reason:  http.StatusText(http.StatusInternalServerError),
					Details: err.Error(),
				},
			})
		}
	}
}
