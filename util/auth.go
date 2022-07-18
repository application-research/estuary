package util

import (
	"fmt"
	"net/http"
)

func isEntityOwner(uID, entityID uint, entity string) error {
	if uID != entityID {
		return HttpError{
			Code:    http.StatusForbidden,
			Reason:  ERR_NOT_AUTHORIZED,
			Details: fmt.Sprintf("User (%d) is not authorized for %s (%d)", uID, entity, entityID),
		}
	}
	return nil
}

func IsCollectionOwner(uID, entityID uint) error {
	return isEntityOwner(uID, entityID, "collection")
}

func IsContentOwner(uID, entityID uint) error {
	return isEntityOwner(uID, entityID, "content")
}
