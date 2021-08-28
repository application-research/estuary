package util

import (
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

const (
	ERR_INVALID_TOKEN           = "ERR_INVALID_TOKEN"
	ERR_TOKEN_EXPIRED           = "ERR_TOKEN_EXPIRED"
	ERR_AUTH_MISSING            = "ERR_AUTH_MISSING"
	ERR_INVALID_AUTH            = "ERR_INVALID_AUTH"
	ERR_AUTH_MISSING_BEARER     = "ERR_AUTH_MISSING_BEARER"
	ERR_NOT_AUTHORIZED          = "ERR_NOT_AUTHORIZED"
	ERR_INVALID_INVITE          = "ERR_INVALID_INVITE"
	ERR_USERNAME_TAKEN          = "ERR_USERNAME_TAKEN"
	ERR_USER_CREATION_FAILED    = "ERR_USER_CREATION_FAILED"
	ERR_USER_NOT_FOUND          = "ERR_USER_NOT_FOUND"
	ERR_INVALID_PASSWORD        = "ERR_INVALID_PASSWORD"
	ERR_INVITE_ALREADY_USED     = "ERR_INVITE_ALREADY_USED"
	ERR_CONTENT_ADDING_DISABLED = "ERR_CONTENT_ADDING_DISABLED"
	ERR_INVALID_INPUT           = "ERR_INVALID_INPUT"
)

type HttpError struct {
	Code    int
	Message string
	Details string
}

func (he HttpError) Error() string {
	return he.Message
}

const (
	PermLevelUser  = 2
	PermLevelAdmin = 10
)

func ExtractAuth(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	if auth == "" {
		return "", &HttpError{
			Code:    403,
			Message: ERR_AUTH_MISSING,
		}
	}

	parts := strings.Split(auth, " ")
	if len(parts) != 2 {
		return "", &HttpError{
			Code:    403,
			Message: ERR_INVALID_AUTH,
		}
	}

	if parts[0] != "Bearer" {
		return "", &HttpError{
			Code:    403,
			Message: ERR_AUTH_MISSING_BEARER,
		}
	}

	return parts[1], nil
}

type UserSettings struct {
	Replication  int  `json:"replication"`
	Verified     bool `json:"verified"`
	DealDuration int  `json:"dealDuration"`

	MaxStagingWait       time.Duration `json:"maxStagingWait"`
	FileStagingThreshold int64         `json:"fileStagingThreshold"`

	ContentAddingDisabled bool `json:"contentAddingDisabled"`
	DealMakingDisabled    bool `json:"dealMakingDisabled"`

	UploadEndpoints []string `json:"uploadEndpoints"`
}

type ViewerResponse struct {
	Username string   `json:"username"`
	Perms    int      `json:"perms"`
	ID       uint     `json:"id"`
	Address  string   `json:"address,omitempty"`
	Miners   []string `json:"miners"`

	AuthExpiry time.Time

	Settings UserSettings `json:"settings"`
}

type AddFileResponse struct {
	Cid       string   `json:"cid"`
	EstuaryId uint     `json:"estuaryId"`
	Providers []string `json:"providers"`
}
