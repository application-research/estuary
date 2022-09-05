package collections

import (
	"fmt"
	"net/http"
	"time"

	"github.com/application-research/estuary/util"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type Collection struct {
	ID        uint      `gorm:"primarykey" json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	UUID string `gorm:"index" json:"uuid"`

	Name        string `json:"name"`
	Description string `json:"description"`
	UserID      uint   `json:"userId"`
	CID         string `json:"cid"`
}

type CollectionRef struct {
	ID         uint `gorm:"primaryKey"`
	CreatedAt  time.Time
	Collection uint    `gorm:"index:,option:CONCURRENTLY; not null"`
	Content    uint    `gorm:"index:,option:CONCURRENTLY;not null"`
	Path       *string `gorm:"not null"`
}

func GetCollection(coluuid string, db *gorm.DB, u *util.User) (Collection, error) {
	var col Collection
	if err := db.First(&col, "uuid = ?", coluuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return Collection{}, &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("collection with ID(%s) was not found", coluuid),
			}
		}
	}
	if err := util.IsCollectionOwner(u.ID, col.UserID); err != nil {
		return Collection{}, err
	}
	return col, nil
}
