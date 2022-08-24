package util

import (
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("util")

//#nosec G101 -- This is a false positive
const (
	ERR_INVALID_TOKEN              = "ERR_INVALID_TOKEN"
	ERR_TOKEN_EXPIRED              = "ERR_TOKEN_EXPIRED"
	ERR_AUTH_MISSING               = "ERR_AUTH_MISSING"
	ERR_WRONG_AUTH_FORMAT          = "ERR_WRONG_AUTH_FORMAT"
	ERR_INVALID_AUTH               = "ERR_INVALID_AUTH"
	ERR_AUTH_MISSING_BEARER        = "ERR_AUTH_MISSING_BEARER"
	ERR_NOT_AUTHORIZED             = "ERR_NOT_AUTHORIZED"
	ERR_MINER_NOT_OWNED            = "ERR_MINER_NOT_OWNED"
	ERR_INVALID_INVITE             = "ERR_INVALID_INVITE"
	ERR_USERNAME_TAKEN             = "ERR_USERNAME_TAKEN"
	ERR_USER_CREATION_FAILED       = "ERR_USER_CREATION_FAILED"
	ERR_USER_NOT_FOUND             = "ERR_USER_NOT_FOUND"
	ERR_INVALID_PASSWORD           = "ERR_INVALID_PASSWORD"
	ERR_INVITE_ALREADY_USED        = "ERR_INVITE_ALREADY_USED"
	ERR_CONTENT_ADDING_DISABLED    = "ERR_CONTENT_ADDING_DISABLED"
	ERR_INVALID_INPUT              = "ERR_INVALID_INPUT"
	ERR_CONTENT_SIZE_OVER_LIMIT    = "ERR_CONTENT_SIZE_OVER_LIMIT"
	ERR_PEERING_PEERS_ADD_ERROR    = "ERR_PEERING_PEERS_ADD_ERROR"
	ERR_PEERING_PEERS_REMOVE_ERROR = "ERR_PEERING_PEERS_REMOVE_ERROR"
	ERR_PEERING_PEERS_START_ERROR  = "ERR_PEERING_PEERS_START_ERROR"
	ERR_PEERING_PEERS_STOP_ERROR   = "ERR_PEERING_PEERS_STOP_ERROR"
	ERR_CONTENT_NOT_FOUND          = "ERR_CONTENT_NOT_FOUND"
	ERR_INVALID_PINNING_STATUS     = "ERR_INVALID_PINNING_STATUS"
	ERR_INVALID_QUERY_PARAM_VALUE  = "ERR_INVALID_QUERY_PARAM_VALUE"
	ERR_CONTENT_LENGTH_REQUIRED    = "ERR_CONTENT_LENGTH_REQUIRED"
)

type HttpError struct {
	Code    int    `json:"code,omitempty"`
	Reason  string `json:"reason"`
	Details string `json:"details"`
}

func (he HttpError) Error() string {
	if he.Details == "" {
		return he.Reason
	}
	return he.Reason + ": " + he.Details
}

type HttpErrorResponse struct {
	Error HttpError `json:"error"`
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
			Code:    http.StatusUnauthorized,
			Reason:  ERR_AUTH_MISSING,
			Details: "no api key was specified",
		}
	}

	parts := strings.Split(auth, " ")
	if len(parts) != 2 {
		return "", &HttpError{
			Code:    http.StatusUnauthorized,
			Reason:  ERR_INVALID_AUTH,
			Details: "invalid api key was specified",
		}
	}

	if parts[0] != "Bearer" {
		return "", &HttpError{
			Code:    http.StatusUnauthorized,
			Reason:  ERR_AUTH_MISSING_BEARER,
			Details: "invalid api key was specified",
		}
	}
	return parts[1], nil
}

type UserSettings struct {
	Replication           int            `json:"replication"`
	Verified              bool           `json:"verified"`
	DealDuration          abi.ChainEpoch `json:"dealDuration"`
	MaxStagingWait        time.Duration  `json:"maxStagingWait"`
	FileStagingThreshold  int64          `json:"fileStagingThreshold"`
	ContentAddingDisabled bool           `json:"contentAddingDisabled"`
	DealMakingDisabled    bool           `json:"dealMakingDisabled"`
	UploadEndpoints       []string       `json:"uploadEndpoints"`
	Flags                 int            `json:"flags"`
}

type ViewerResponse struct {
	Username   string       `json:"username"`
	Perms      int          `json:"perms"`
	ID         uint         `json:"id"`
	Address    string       `json:"address,omitempty"`
	Miners     []string     `json:"miners,omitempty"`
	AuthExpiry time.Time    `json:"auth_expiry,omitempty"`
	Settings   UserSettings `json:"settings"`
}

func ErrorHandler(err error, ctx echo.Context) {
	var httpRespErr *HttpError
	if xerrors.As(err, &httpRespErr) {
		log.Errorf("handler error: %s", err)
		if err := ctx.JSON(httpRespErr.Code, HttpErrorResponse{Error: *httpRespErr}); err != nil {
			log.Errorf("handler error: %s", err)
			return
		}
		return
	}

	var echoErr *echo.HTTPError
	if xerrors.As(err, &echoErr) {
		if err := ctx.JSON(echoErr.Code, HttpErrorResponse{
			Error: HttpError{
				Code:    echoErr.Code,
				Reason:  http.StatusText(echoErr.Code),
				Details: echoErr.Message.(string),
			},
		}); err != nil {
			log.Errorf("handler error: %s", err)
			return
		}
		return
	}

	log.Errorf("handler error: %s", err)
	if err := ctx.JSON(http.StatusInternalServerError, HttpErrorResponse{
		Error: HttpError{
			Code:    http.StatusInternalServerError,
			Reason:  http.StatusText(http.StatusInternalServerError),
			Details: err.Error(),
		},
	}); err != nil {
		log.Errorf("handler error: %s", err)
		return
	}
}
