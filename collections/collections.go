package collections

import (
	"fmt"
	"net/http"
	"strings"
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

// refs = collections.GetContentsInPath(path, s.DB, u)
func GetContentsInPath(coluuid string, path string, db *gorm.DB, u *util.User) ([]CollectionRef, error) {
	col, err := GetCollection(coluuid, db, u)
	if err != nil {
		return []CollectionRef{}, err
	}

	refs := []CollectionRef{}
	if err := db.Model(CollectionRef{}).
		Where("collection = ?", col.ID).
		Scan(&refs).Error; err != nil {
		return []CollectionRef{}, err
	}

	if len(refs) == 0 {
		return []CollectionRef{}, fmt.Errorf("no contents on specified path for collection")
	}

	var selectedRefs []CollectionRef
	for _, ref := range refs {
		if strings.HasPrefix(*ref.Path, path) {
			selectedRefs = append(selectedRefs, ref)
		}
	}

	return selectedRefs, nil
}
