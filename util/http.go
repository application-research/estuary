package util

import (
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("util")

const (
	ERR_INVALID_TOKEN           = "ERR_INVALID_TOKEN"
	ERR_TOKEN_EXPIRED           = "ERR_TOKEN_EXPIRED"
	ERR_AUTH_MISSING            = "ERR_AUTH_MISSING"
	ERR_WRONG_AUTH_FORMAT       = "ERR_WRONG_AUTH_FORMAT"
	ERR_INVALID_AUTH            = "ERR_INVALID_AUTH"
	ERR_AUTH_MISSING_BEARER     = "ERR_AUTH_MISSING_BEARER"
	ERR_NOT_AUTHORIZED          = "ERR_NOT_AUTHORIZED"
	ERR_MINER_NOT_OWNED         = "ERR_MINER_NOT_OWNED"
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
	PermLevelUpload = 1
	PermLevelUser   = 2
	PermLevelAdmin  = 10
)

// isValidAuth checks if authStr is a valid
// returns false if authStr is not in a valid format
// returns true otherwise
func isValidAuth(authStr string) bool {
	matchEst, _ := regexp.MatchString("^EST(.+)ARY$", authStr)
	matchSecret, _ := regexp.MatchString("^SECRET(.+)SECRET$", authStr)
	if !matchEst && !matchSecret {
		return false
	}
	// only get the uuid from the string
	uuidStr := strings.ReplaceAll(authStr, "SECRET", "")
	uuidStr = strings.ReplaceAll(uuidStr, "EST", "")
	uuidStr = strings.ReplaceAll(uuidStr, "ARY", "")

	// check if uuid is valid
	_, err := uuid.Parse(uuidStr)
	if err != nil {
		return false
	}

	return true
}

func ExtractAuth(c echo.Context) (string, error) {
	auth := c.Request().Header.Get("Authorization")
	//	undefined will be the auth value if ESTUARY_TOKEN cookie is removed.
	if auth == "" || auth == "undefined" {
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

	//	if auth is not missing, check format first before extracting
	if !isValidAuth(parts[1]) {
		return "", &HttpError{
			Code:    403,
			Message: ERR_WRONG_AUTH_FORMAT,
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
	Miners   []string `json:"miners,omitempty"`

	AuthExpiry time.Time

	Settings UserSettings `json:"settings"`
}

func ErrorHandler(err error, ctx echo.Context) {
	log.Errorf("handler error: %s", err)
	var herr *HttpError
	if xerrors.As(err, &herr) {
		res := map[string]string{
			"error": herr.Message,
		}
		if herr.Details != "" {
			res["details"] = herr.Details
		}
		ctx.JSON(herr.Code, res)
		return
	}

	var echoErr *echo.HTTPError
	if xerrors.As(err, &echoErr) {
		ctx.JSON(echoErr.Code, map[string]interface{}{
			"error": echoErr.Message,
		})
		return
	}

	// TODO: returning all errors out to the user smells potentially bad
	_ = ctx.JSON(500, map[string]interface{}{
		"error": err.Error(),
	})
}
