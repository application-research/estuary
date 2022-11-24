package buckets

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
)

type Bucket struct {
	ID        uint      `gorm:"primarykey" json:"-"`
	CreatedAt time.Time `json:"createdAt"`

	UUID string `gorm:"index" json:"uuid"`

	Name        string `json:"name"`
	Description string `json:"description"`
	UserID      uint   `json:"userId"`
	CID         string `json:"cid"`
}

type BucketRef struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	Bucket    uint    `gorm:"index:,option:CONCURRENTLY; not null"`
	Content   uint    `gorm:"index:,option:CONCURRENTLY;not null"`
	Path      *string `gorm:"not null"`
}

func GetBucket(bucketuuid string, db *gorm.DB, u *util.User) (Bucket, error) {
	var bucket Bucket
	if err := db.First(&bucket, "uuid = ?", bucketuuid).Error; err != nil {
		if xerrors.Is(err, gorm.ErrRecordNotFound) {
			return Bucket{}, &util.HttpError{
				Code:    http.StatusNotFound,
				Reason:  util.ERR_CONTENT_NOT_FOUND,
				Details: fmt.Sprintf("bucket with ID(%s) was not found", bucketuuid),
			}
		}
	}
	if err := util.IsBucketOwner(u.ID, bucket.UserID); err != nil {
		return Bucket{}, err
	}
	return bucket, nil
}

// refs = buckets.GetContentsInPath(path, s.DB, u)
func GetContentsInPath(bucketuuid string, path string, db *gorm.DB, u *util.User) ([]BucketRef, error) {
	bucket, err := GetBucket(bucketuuid, db, u)
	if err != nil {
		return []BucketRef{}, err
	}

	refs := []BucketRef{}
	if err := db.Model(BucketRef{}).
		Where("bucket = ?", bucket.ID).
		Scan(&refs).Error; err != nil {
		return []BucketRef{}, err
	}

	if len(refs) == 0 {
		return []BucketRef{}, fmt.Errorf("no contents on specified path for bucket")
	}

	var selectedRefs []BucketRef
	for _, ref := range refs {
		if strings.HasPrefix(*ref.Path, path) {
			selectedRefs = append(selectedRefs, ref)
		}
	}

	return selectedRefs, nil
}
