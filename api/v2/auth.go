package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

func (s *apiV2) AuthRequired(level int) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {

			//	Check first if the Token is available. We should not continue if the
			//	token isn't even available.
			auth, err := util.ExtractAuth(c)
			if err != nil {
				return err
			}

			ctx, span := s.tracer.Start(c.Request().Context(), "authCheck")
			defer span.End()
			c.SetRequest(c.Request().WithContext(ctx))

			u, err := s.checkTokenAuth(auth)
			if err != nil {
				return err
			}

			span.SetAttributes(attribute.Int("user", int(u.ID)))

			if u.AuthToken.UploadOnly && level >= util.PermLevelUser {
				s.log.Warnw("api key is upload only", "user", u.ID, "perm", u.Perm, "required", level)

				return &util.HttpError{
					Code:    http.StatusForbidden,
					Reason:  util.ERR_NOT_AUTHORIZED,
					Details: "api key is upload only",
				}
			}

			if u.Perm >= level {
				c.Set("user", u)
				return next(c)
			}

			s.log.Warnw("user not authorized", "user", u.ID, "perm", u.Perm, "required", level)

			return &util.HttpError{
				Code:    http.StatusForbidden,
				Reason:  util.ERR_NOT_AUTHORIZED,
				Details: "user not authorized",
			}
		}
	}
}

func (s *apiV2) checkTokenAuth(token string) (*util.User, error) {
	var authToken util.AuthToken
	tokenHash := util.GetTokenHash(token)
	if err := s.DB.First(&authToken, "token = ? OR token_hash = ?", token, tokenHash).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusUnauthorized,
				Reason:  util.ERR_INVALID_TOKEN,
				Details: "api key does not exist",
			}
		}
		return nil, err
	}

	if authToken.Expiry.Before(time.Now()) {
		return nil, &util.HttpError{
			Code:    http.StatusUnauthorized,
			Reason:  util.ERR_TOKEN_EXPIRED,
			Details: fmt.Sprintf("token for user %d expired %s", authToken.User, authToken.Expiry),
		}
	}

	var user util.User
	if err := s.DB.First(&user, "id = ?", authToken.User).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, &util.HttpError{
				Code:    http.StatusUnauthorized,
				Reason:  util.ERR_INVALID_TOKEN,
				Details: "no user exists for the spicified api key",
			}
		}
		return nil, err
	}

	user.AuthToken = authToken
	return &user, nil
}
