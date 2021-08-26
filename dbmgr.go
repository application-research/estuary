package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	gocid "github.com/ipfs/go-cid"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type DBSortOrder int

const (
	OrderDescending DBSortOrder = 0
	OrderAscending  DBSortOrder = 1
)

type DBMgr struct{ DB *gorm.DB }

func (mgr *DBMgr) Users() *UsersQuery {
	return NewUsersQuery(mgr.DB)
}

func (mgr *DBMgr) AuthTokens() *AuthTokensQuery {
	return NewAuthTokensQuery(mgr.DB)
}

func (mgr *DBMgr) Contents() *ContentsQuery {
	return NewContentsQuery(mgr.DB)
}

func (mgr *DBMgr) Objects() *ObjectsQuery {
	return NewObjectsQuery(mgr.DB)
}

func (mgr *DBMgr) ObjRefs() *ObjRefsQuery {
	return NewObjRefsQuery(mgr.DB)
}

func (mgr *DBMgr) Deals() *DealsQuery {
	return NewDealsQuery(mgr.DB)
}

func (mgr *DBMgr) Collections() *CollectionsQuery {
	return NewCollectionsQuery(mgr.DB)
}

func (mgr *DBMgr) CollectionRefs() *CollectionRefsQuery {
	return NewCollectionRefsQuery(mgr.DB)
}

func (mgr *DBMgr) DFERecords() *DFERecordsQuery {
	return NewDFERecordsQuery(mgr.DB)
}

func (mgr *DBMgr) ProposalRecords() *ProposalRecordsQuery {
	return NewProposalRecordsQuery(mgr.DB)
}

func (mgr *DBMgr) InviteCodes() *InviteCodesQuery {
	return NewInviteCodesQuery(mgr.DB)
}

func (mgr *DBMgr) StorageMiners() *StorageMinersQuery {
	return NewStorageMinersQuery(mgr.DB)
}

func (mgr *DBMgr) RetrievalSuccessRecords() *RetrievalSuccessRecordsQuery {
	return NewRetrievalSuccessRecordsQuery(mgr.DB)
}

func (mgr *DBMgr) RetrievalFailureRecords() *RetrievalFailureRecordsQuery {
	return NewRetrievalFailureRecordsQuery(mgr.DB)
}

func NewDBMgr(dbval string) (*DBMgr, error) {
	parts := strings.SplitN(dbval, "=", 2)
	if len(parts) == 1 {
		return nil, fmt.Errorf("format for database string is 'DBTYPE=PARAMS'")
	}

	var dial gorm.Dialector
	switch parts[0] {
	case "sqlite":
		dial = sqlite.Open(parts[1])
	case "postgres":
		dial = postgres.Open(parts[1])
	default:
		return nil, fmt.Errorf("unsupported or unrecognized db type: %s", parts[0])
	}

	db, err := gorm.Open(dial, &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, err
	}

	sqldb, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqldb.SetMaxIdleConns(80)
	sqldb.SetMaxOpenConns(99)
	sqldb.SetConnMaxIdleTime(time.Hour)

	db.AutoMigrate(&Content{})
	db.AutoMigrate(&Object{})
	db.AutoMigrate(&ObjRef{})
	db.AutoMigrate(&Collection{})
	db.AutoMigrate(&CollectionRef{})

	db.AutoMigrate(&contentDeal{})
	db.AutoMigrate(&dfeRecord{})
	db.AutoMigrate(&PieceCommRecord{})
	db.AutoMigrate(&proposalRecord{})
	db.AutoMigrate(&retrievalFailureRecord{})
	db.AutoMigrate(&retrievalSuccessRecord{})

	db.AutoMigrate(&minerStorageAsk{})
	db.AutoMigrate(&storageMiner{})

	db.AutoMigrate(&User{})
	db.AutoMigrate(&AuthToken{})
	db.AutoMigrate(&InviteCode{})

	db.AutoMigrate(&Shuttle{})

	var count int64
	if err := db.Model(&storageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	if count == 0 {
		// TODO: this could go into its own generic function, potentially batch
		// these insertions
		fmt.Println("adding default miner list to database...")
		for _, m := range defaultMiners {
			db.Create(&storageMiner{Address: util.DbAddr{Addr: m}})
		}

	}

	return &DBMgr{db}, nil
}

// USERS

type UsersQuery struct{ DB *gorm.DB }

func NewUsersQuery(db *gorm.DB) *UsersQuery {
	return &UsersQuery{DB: db.Model(&User{})}
}

func (q *UsersQuery) WithUsername(username string) *UsersQuery {
	q.DB = q.DB.Where("username = ?", username)
	return q
}

func (q *UsersQuery) WithID(id uint) *UsersQuery {
	q.DB = q.DB.Where("id = ?", id)
	return q
}

func (q *UsersQuery) Create(user User) error {
	return q.DB.Create(&user).Error
}

func (q *UsersQuery) Get() (User, error) {
	var user User
	if err := q.DB.Take(&user).Error; err != nil {
		return User{}, err
	}
	return user, nil
}

func (q *UsersQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (q *UsersQuery) Exists() (bool, error) {
	count, err := q.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Errors if none were deleted
func (q *UsersQuery) ExpectDelete() error {
	res := q.DB.Delete(&User{})
	if err := res.Error; err != nil {
		return err
	}

	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}

// AUTH TOKENS

type AuthTokensQuery struct{ DB *gorm.DB }

func NewAuthTokensQuery(db *gorm.DB) *AuthTokensQuery {
	return &AuthTokensQuery{DB: db.Model(&AuthToken{})}
}

func (q *AuthTokensQuery) Create(authToken AuthToken) error {
	return q.DB.Create(&authToken).Error
}

// CONTENTS

type ContentsQuery struct{ DB *gorm.DB }

func NewContentsQuery(db *gorm.DB) *ContentsQuery {
	return &ContentsQuery{DB: db.Model(&Content{})}
}

func (q *ContentsQuery) WithID(id uint) *ContentsQuery {
	q.DB = q.DB.Where("id = ?", id)
	return q
}

func (q *ContentsQuery) WithActive(active bool) *ContentsQuery {
	if active {
		q.DB = q.DB.Where("active")
	} else {
		q.DB = q.DB.Where("NOT active")
	}
	return q
}

func (q *ContentsQuery) WithUserID(userID uint) *ContentsQuery {
	q.DB = q.DB.Where("user_id = ?", userID)
	return q
}

func (q *ContentsQuery) WithCid(cid gocid.Cid) *ContentsQuery {
	q.DB = q.DB.Where("cid = ?", cidToBytes(cid))
	return q
}

func (q *ContentsQuery) WithCids(cids []gocid.Cid) *ContentsQuery {
	q.DB = q.DB.Where("cid IN ?", cidsToBytes(cids))
	return q
}

func (q *ContentsQuery) WithAggregate(aggregate bool) *ContentsQuery {
	if aggregate {
		q.DB = q.DB.Where("aggregate")
	} else {
		q.DB = q.DB.Where("NOT aggregate")
	}
	return q
}

func (q *ContentsQuery) WithAggregatedIn(contentID uint) *ContentsQuery {
	q.DB = q.DB.Where("aggregated_in = ?", contentID)
	return q
}

func (q *ContentsQuery) Limit(limit int) *ContentsQuery {
	q.DB = q.DB.Limit(limit)
	return q
}

func (q *ContentsQuery) Offset(offset int) *ContentsQuery {
	q.DB = q.DB.Offset(offset)
	return q
}

// TODO: order functions can probably be simplified
func (q *ContentsQuery) OrderByCreationDate(order DBSortOrder) *ContentsQuery {
	if order == OrderDescending {
		q.DB = q.DB.Order("created_at DESC")
	} else {
		q.DB = q.DB.Order("created_at ASC")
	}
	return q
}

func (q *ContentsQuery) OrderByID(order DBSortOrder) *ContentsQuery {
	if order == OrderDescending {
		q.DB = q.DB.Order("id DESC")
	} else {
		q.DB = q.DB.Order("id ASC")
	}
	return q
}

func (q *ContentsQuery) CreateAll(contents []Content) error {
	return q.DB.Create(&contents).Error
}

func (q *ContentsQuery) Get() (Content, error) {
	var content Content
	if err := q.DB.Take(&content).Error; err != nil {
		return Content{}, err
	}
	return content, nil
}

func (q *ContentsQuery) GetAll() ([]Content, error) {
	var contents []Content
	if err := q.DB.Find(&contents).Error; err != nil {
		return nil, nil
	}
	return contents, nil
}

func (q *ContentsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (q *ContentsQuery) Delete() error {
	return q.DB.Delete(&Content{}).Error
}

// OBJECTS

type ObjectsQuery struct{ DB *gorm.DB }

func NewObjectsQuery(db *gorm.DB) *ObjectsQuery {
	return &ObjectsQuery{DB: db.Model(&Object{})}
}

func (q *ObjectsQuery) WithCid(cid gocid.Cid) *ObjectsQuery {
	q.DB = q.DB.Where("cid = ?", cidToBytes(cid))
	return q
}

func (q *ObjectsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (q *ObjectsQuery) Exists() (bool, error) {
	count, err := q.Count()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// TODO: simplify by using other abstracted functions instead
func (q *ObjectsQuery) DeleteUnreferenced(ids []uint) error {
	return q.DB.Where(
		"(?) = 0 AND id in ?",
		q.DB.Model(&ObjRef{}).Where("object = objects.id").Select("count(1)"), ids,
	).Delete(Object{}).Error
}

// OBJ REFS

type ObjRefsQuery struct{ DB *gorm.DB }

func NewObjRefsQuery(db *gorm.DB) *ObjRefsQuery {
	return &ObjRefsQuery{DB: db.Model(&ObjRef{})}
}

func (q *ObjRefsQuery) WithPinID(pinID uint) *ObjRefsQuery {
	q.DB = q.DB.Where("pin = ?", pinID)
	return q
}

func (q *ObjRefsQuery) Delete() error {
	return q.DB.Delete(&ObjRef{}).Error
}

// DEALS

type DealsQuery struct{ DB *gorm.DB }

func NewDealsQuery(db *gorm.DB) *DealsQuery {
	return &DealsQuery{DB: db.Model(&contentDeal{})}
}

func (q *DealsQuery) WithID(id uint) *DealsQuery {
	q.DB = q.DB.Where("id = ?", id)
	return q
}

func (q *DealsQuery) WithSuccessful(valid bool) *DealsQuery {
	if valid {
		q.DB = q.DB.Where("deal_id > 0")
	} else {
		q.DB = q.DB.Where("deal_id <= 0")
	}
	return q
}

func (q *DealsQuery) WithFailed(failed bool) *DealsQuery {
	if failed {
		q.DB = q.DB.Where("failed")
	} else {
		q.DB = q.DB.Where("not failed")
	}
	return q
}

func (q *DealsQuery) WithPropCid(propCid gocid.Cid) *DealsQuery {
	q.DB = q.DB.Where("prop_cid = ?", cidToBytes(propCid))
	return q
}

func (q *DealsQuery) WithContentID(contentID uint) *DealsQuery {
	q.DB = q.DB.Where("content = ?", contentID)
	return q
}

func (q *DealsQuery) WithContentIDs(contentIDs []uint) *DealsQuery {
	q.DB = q.DB.Where("content IN ?", contentIDs)
	return q
}

func (q *DealsQuery) Get() (contentDeal, error) {
	var deal contentDeal
	if err := q.DB.Take(&deal).Error; err != nil {
		return contentDeal{}, err
	}
	return deal, nil
}

func (q *DealsQuery) GetAll() ([]contentDeal, error) {
	var deals []contentDeal
	if err := q.DB.Find(&deals).Error; err != nil {
		return nil, err
	}
	return deals, nil
}

func (q *DealsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// COLLECTIONS

type CollectionsQuery struct{ DB *gorm.DB }

func NewCollectionsQuery(db *gorm.DB) *CollectionsQuery {
	return &CollectionsQuery{DB: db.Model(&Collection{})}
}

func (q *CollectionsQuery) WithUUID(uuid string) *CollectionsQuery {
	q.DB = q.DB.Where("uuid = ?", uuid)
	return q
}

func (q *CollectionsQuery) WithUserID(userID uint) *CollectionsQuery {
	q.DB = q.DB.Where("user_id = ?", userID)
	return q
}

func (q *CollectionsQuery) Get() (Collection, error) {
	var collection Collection
	if err := q.DB.Take(&collection).Error; err != nil {
		return Collection{}, err
	}

	return collection, nil
}

// COLLECTION REFS

type CollectionRefsQuery struct{ DB *gorm.DB }

func NewCollectionRefsQuery(db *gorm.DB) *CollectionRefsQuery {
	return &CollectionRefsQuery{DB: db.Model(&CollectionRef{})}
}

func (q *CollectionRefsQuery) Create(collectionRef CollectionRef) error {
	return q.DB.Create(&collectionRef).Error
}

// DFE RECORDS

type DFERecordsQuery struct{ DB *gorm.DB }

func NewDFERecordsQuery(db *gorm.DB) *DFERecordsQuery {
	return &DFERecordsQuery{DB: db.Model(&dfeRecord{})}
}

func (q *DFERecordsQuery) WithContent(contentID uint) *DFERecordsQuery {
	q.DB = q.DB.Where("content = ?", contentID)
	return q
}

func (q *DFERecordsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// PROPOSAL RECORDS

type ProposalRecordsQuery struct{ DB *gorm.DB }

func NewProposalRecordsQuery(db *gorm.DB) *ProposalRecordsQuery {
	return &ProposalRecordsQuery{DB: db.Model(&proposalRecord{})}
}

func (q *ProposalRecordsQuery) WithPropCid(cid gocid.Cid) *ProposalRecordsQuery {
	q.DB = q.DB.Where("prop_cid = ?", cidToBytes(cid))
	return q
}

func (q *ProposalRecordsQuery) Get() (proposalRecord, error) {
	var record proposalRecord
	if err := q.DB.Take(&record).Error; err != nil {
		return proposalRecord{}, err
	}
	return record, nil
}

// INVITE CODES

type InviteCodesQuery struct{ DB *gorm.DB }

func NewInviteCodesQuery(db *gorm.DB) *InviteCodesQuery {
	return &InviteCodesQuery{DB: db.Model(&InviteCode{})}
}

type ClaimedInvite struct {
	Code      string
	Username  string
	ClaimedBy string
}

func (q *InviteCodesQuery) GetClaimedInvites() ([]ClaimedInvite, error) {
	var invites []ClaimedInvite
	if err := q.DB.
		Select("code, username, (?) as claimed_by", q.DB.Table("users").Select("username").Where("id = invite_codes.claimed_by")).
		//Where("claimed_by IS NULL").
		Joins("left join users on users.id = invite_codes.created_by").
		Scan(&invites).Error; err != nil {
		return nil, err
	}
	return invites, nil
}

func (q *InviteCodesQuery) Create(invite InviteCode) error {
	return q.DB.Create(&invite).Error
}

// STORAGE MINERS

type StorageMinersQuery struct{ DB *gorm.DB }

func NewStorageMinersQuery(db *gorm.DB) *StorageMinersQuery {
	return &StorageMinersQuery{DB: db.Model(&storageMiner{})}
}

func (q *StorageMinersQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// RETRIEVAL SUCCESS RECORDS

type RetrievalSuccessRecordsQuery struct{ DB *gorm.DB }

func NewRetrievalSuccessRecordsQuery(db *gorm.DB) *RetrievalSuccessRecordsQuery {
	return &RetrievalSuccessRecordsQuery{DB: db.Model(&retrievalSuccessRecord{})}
}

func (q *RetrievalSuccessRecordsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// RETRIEVAL FAILURE RECORDS

type RetrievalFailureRecordsQuery struct{ DB *gorm.DB }

func NewRetrievalFailureRecordsQuery(db *gorm.DB) *RetrievalFailureRecordsQuery {
	return &RetrievalFailureRecordsQuery{DB: db.Model(&retrievalFailureRecord{})}
}

func (q *RetrievalFailureRecordsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// HELPER FUNCTIONS

func bytesToCid(bytes []byte) (gocid.Cid, error) {
	if len(bytes) == 0 {
		return gocid.Undef, nil
	}

	cid, err := gocid.Cast(bytes)
	if err != nil {
		return gocid.Undef, err
	}

	return cid, nil
}

func bytesToCids(bytesList [][]byte) ([]gocid.Cid, error) {
	var cids []gocid.Cid
	for _, bytes := range bytesList {
		cid, err := bytesToCid(bytes)
		if err != nil {
			return nil, err
		}
		cids = append(cids, cid)
	}

	return cids, nil
}

func cidToBytes(cid gocid.Cid) []byte {
	return cid.Bytes()
}

func cidsToBytes(cids []gocid.Cid) [][]byte {
	var bytesList [][]byte
	for _, cid := range cids {
		bytesList = append(bytesList, cidToBytes(cid))
	}

	return bytesList
}
