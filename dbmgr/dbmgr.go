package dbmgr

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	gocid "github.com/ipfs/go-cid"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	ErrNotFiltered = errors.New("actions requires query to be filtered, but no filters were applied")
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

func (mgr *DBMgr) PieceCommRecords() *PieceCommRecordsQuery {
	return NewPieceCommRecordsQuery(mgr.DB)
}

func (mgr *DBMgr) MinerStorageAsks() *MinerStorageAsksQuery {
	return NewMinerStorageAsksQuery(mgr.DB)
}

func (mgr *DBMgr) Shuttles() *ShuttlesQuery {
	return NewShuttlesQuery(mgr.DB)
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

	_, err = migrateSchemas(db)
	if err != nil {
		return nil, err
	}

	var count int64
	if err := db.Model(&StorageMiner{}).Count(&count).Error; err != nil {
		return nil, err
	}

	//miners from minerX spreadsheet
	minerStrs := []string{
		"f02620",
		"f023971",
		"f022142",
		"f019551",
		"f01240",
		"f01247",
		"f01278",
		"f071624",
		"f0135078",
		"f022352",
		"f014768",
		"f022163",
		"f09848",
		"f02576",
		"f02606",
		"f019041",
		"f010617",
		"f023467",
		"f01276",
		"f02401",
		"f02387",
		"f019104",
		"f099608",
		"f062353",
		"f07998",
		"f019362",
		"f019100",
		"f014409",
		"f066596",
		"f01234",
		"f058369",
		"f08399",
		"f021716",
		"f010479",
		"f08403",
		"f01277",
		"f015927",
	}

	var defaultMiners []address.Address
	for _, s := range minerStrs {
		a, err := address.NewFromString(s)
		if err != nil {
			panic(err)
		}

		defaultMiners = append(defaultMiners, a)
	}

	if count == 0 {
		// TODO: this could go into its own generic function, potentially batch
		// these insertions
		fmt.Println("adding default miner list to database...")
		for _, m := range defaultMiners {
			db.Create(&StorageMiner{Address: util.DbAddr{Addr: m}})
		}

	}

	return &DBMgr{db}, nil
}

func migrateSchemas(db *gorm.DB) (*DBMgr, error) {
	schemas := []interface{}{
		&Content{},
		&Object{},
		&ObjRef{},
		&Collection{},
		&CollectionRef{},
		&Deal{},
		&DFERecord{},
		&PieceCommRecord{},
		&ProposalRecord{},
		&RetrievalFailureRecord{},
		&RetrievalSuccessRecord{},
		&MinerStorageAsk{},
		&StorageMiner{},
		&User{},
		&AuthToken{},
		&InviteCode{},
		&Shuttle{},
	}
	for schema := range schemas {
		err := db.AutoMigrate(schema)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// USERS

type UsersQuery struct {
	DB       *gorm.DB
	filtered bool
}
type UserID uint
type User struct {
	gorm.Model

	UUID     string `gorm:"unique"`
	Username string `gorm:"unique"`
	PassHash string

	UserEmail string

	Address util.DbAddr

	Perm  int
	Flags int

	StorageDisabled bool
}

func NewUsersQuery(db *gorm.DB) *UsersQuery {
	return &UsersQuery{DB: db.Model(User{})}
}

func (q *UsersQuery) WithUsername(username string) *UsersQuery {
	q.DB = q.DB.Where("username = ?", username)
	q.filtered = true
	return q
}

func (q *UsersQuery) WithID(id uint) *UsersQuery {
	q.DB = q.DB.Where("id = ?", id)
	q.filtered = true
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
	var exists bool
	if err := q.DB.Raw("SELECT EXISTS(?)", q.DB).Take(&exists).Error; err != nil {
		return false, err
	}
	return exists, nil
}

// Errors if none were deleted
func (q *UsersQuery) ExpectDelete() error {
	if !q.filtered {
		return ErrNotFiltered
	}

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
type AuthToken struct {
	gorm.Model
	Token  string `gorm:"unique"`
	User   UserID
	Expiry time.Time
}

func NewAuthTokensQuery(db *gorm.DB) *AuthTokensQuery {
	return &AuthTokensQuery{DB: db.Model(AuthToken{})}
}

func (q *AuthTokensQuery) Create(authToken AuthToken) error {
	return q.DB.Create(&authToken).Error
}

// CONTENTS

type ContentsQuery struct {
	DB       *gorm.DB
	filtered bool
}
type ContentID uint
type Content struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Cid         util.DbCID
	Name        string
	UserID      UserID
	Description string
	Size        int64
	Type        util.ContentType
	Active      bool
	Offloaded   bool
	Replication int

	AggregatedIn ContentID
	Aggregate    bool

	Pinning bool
	PinMeta string

	Failed bool

	Location string
}

func NewContentsQuery(db *gorm.DB) *ContentsQuery {
	return &ContentsQuery{DB: db.Model(Content{})}
}

func (q *ContentsQuery) WithID(id uint) *ContentsQuery {
	q.DB = q.DB.Where("id = ?", id)
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithActive(active bool) *ContentsQuery {
	if active {
		q.DB = q.DB.Where("active")
	} else {
		q.DB = q.DB.Where("NOT active")
	}
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithUserID(userID uint) *ContentsQuery {
	q.DB = q.DB.Where("user_id = ?", userID)
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithCid(cid gocid.Cid) *ContentsQuery {
	q.DB = q.DB.Where("cid = ?", cidToBytes(cid))
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithCids(cids []gocid.Cid) *ContentsQuery {
	q.DB = q.DB.Where("cid IN ?", cidsToBytes(cids))
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithAggregate(aggregate bool) *ContentsQuery {
	if aggregate {
		q.DB = q.DB.Where("aggregate")
	} else {
		q.DB = q.DB.Where("NOT aggregate")
	}
	q.filtered = true
	return q
}

func (q *ContentsQuery) WithAggregatedIn(contentID uint) *ContentsQuery {
	q.DB = q.DB.Where("aggregated_in = ?", contentID)
	q.filtered = true
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
	return q.DB.Create(contents).Error
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
	if !q.filtered {
		return ErrNotFiltered
	}

	return q.DB.Delete(&Content{}).Error
}

// OBJECTS

type ObjectsQuery struct{ DB *gorm.DB }
type ObjectID uint
type Object struct {
	ID         ObjectID   `gorm:"primarykey"`
	Cid        util.DbCID `gorm:"index"`
	Size       int
	Reads      int
	LastAccess time.Time
}

func NewObjectsQuery(db *gorm.DB) *ObjectsQuery {
	return &ObjectsQuery{DB: db.Model(Object{})}
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
	var exists bool
	if err := q.DB.Raw("SELECT EXISTS(?)", q.DB).Take(&exists).Error; err != nil {
		return false, err
	}
	return exists, nil
}

// TODO: simplify by using other abstracted functions instead
func (q *ObjectsQuery) DeleteUnreferenced(ids []uint) error {
	return q.DB.Where(
		"(?) = 0 AND id in ?",
		q.DB.Model(&ObjRef{}).Where("object = objects.id").Select("count(1)"), ids,
	).Delete(Object{}).Error
}

// OBJ REFS

type ObjRefsQuery struct {
	DB       *gorm.DB
	filtered bool
}
type ObjRefID uint
type ObjRef struct {
	ID        ObjRefID `gorm:"primarykey"`
	Content   ContentID
	Object    ObjectID
	Offloaded uint
}

func NewObjRefsQuery(db *gorm.DB) *ObjRefsQuery {
	return &ObjRefsQuery{DB: db.Model(ObjRef{})}
}

func (q *ObjRefsQuery) WithPinID(pinID uint) *ObjRefsQuery {
	q.DB = q.DB.Where("pin = ?", pinID)
	q.filtered = true
	return q
}

func (q *ObjRefsQuery) WithContentID(contentID uint) *ObjRefsQuery {
	q.DB = q.DB.Where("content = ?", contentID)
	q.filtered = true
	return q
}

type ObjRefStats struct {
	Bandwidth  int64
	TotalReads int64
}

func (q *ObjRefsQuery) GetStats() (ObjRefStats, error) {
	var stats ObjRefStats
	if err := q.DB.Model(ObjRef{}).
		Select("SUM(size * reads) as bandwidth, SUM(reads) as total_reads").
		Joins("left join objects on obj_refs.object = objects.id").
		Scan(&stats).Error; err != nil {
		return ObjRefStats{}, err
	}
	return stats, nil
}

func (q *ObjRefsQuery) Delete() error {
	if !q.filtered {
		return ErrNotFiltered
	}
	return q.DB.Delete(&ObjRef{}).Error
}

// DEALS

type DealsQuery struct{ DB *gorm.DB }
type DealID uint
type Deal struct {
	gorm.Model

	Content          uint
	PropCid          util.DbCID
	Miner            string
	DealID           int64
	Failed           bool
	Verified         bool
	FailedAt         time.Time
	DTChan           string
	TransferStarted  time.Time
	TransferFinished time.Time

	OnChainAt time.Time
	SealedAt  time.Time
}

func NewDealsQuery(db *gorm.DB) *DealsQuery {
	return &DealsQuery{DB: db.Model(Deal{})}
}

func (q *DealsQuery) WithID(id uint) *DealsQuery {
	q.DB = q.DB.Where("id = ?", id)
	return q
}

func (q *DealsQuery) WithDealOnChain(valid bool) *DealsQuery {
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

func (q *DealsQuery) Get() (Deal, error) {
	var deal Deal
	if err := q.DB.Take(&deal).Error; err != nil {
		return Deal{}, err
	}
	return deal, nil
}

func (q *DealsQuery) GetAll() ([]Deal, error) {
	var deals []Deal
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
type CollectionID uint
type Collection struct {
	ID        CollectionID `gorm:"primarykey"`
	CreatedAt time.Time

	UUID string `gorm:"index"`

	Name        string `gorm:"unique"`
	Description string
	UserID      UserID
}

func NewCollectionsQuery(db *gorm.DB) *CollectionsQuery {
	return &CollectionsQuery{DB: db.Model(Collection{})}
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
type CollectionRefID uint
type CollectionRef struct {
	ID         CollectionRefID `gorm:"primarykey"`
	CreatedAt  time.Time
	Collection CollectionID
	Content    ContentID
}

func NewCollectionRefsQuery(db *gorm.DB) *CollectionRefsQuery {
	return &CollectionRefsQuery{DB: db.Model(CollectionRef{})}
}

func (q *CollectionRefsQuery) Create(collectionRef CollectionRef) error {
	return q.DB.Create(&collectionRef).Error
}

// DFE RECORDS

type DFERecordsQuery struct{ DB *gorm.DB }
type DFERecord struct {
	gorm.Model

	Miner        string
	Phase        string
	Message      string
	Content      uint `gorm:"index"`
	MinerVersion string
}

func NewDFERecordsQuery(db *gorm.DB) *DFERecordsQuery {
	return &DFERecordsQuery{DB: db.Model(DFERecord{})}
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
type ProposalRecord struct {
	PropCid util.DbCID
	Data    []byte
}

func NewProposalRecordsQuery(db *gorm.DB) *ProposalRecordsQuery {
	return &ProposalRecordsQuery{DB: db.Model(ProposalRecord{})}
}

func (q *ProposalRecordsQuery) WithPropCid(cid gocid.Cid) *ProposalRecordsQuery {
	q.DB = q.DB.Where("prop_cid = ?", cidToBytes(cid))
	return q
}

func (q *ProposalRecordsQuery) Get() (ProposalRecord, error) {
	var record ProposalRecord
	if err := q.DB.Take(&record).Error; err != nil {
		return ProposalRecord{}, err
	}
	return record, nil
}

// INVITE CODES

type InviteCodesQuery struct{ DB *gorm.DB }
type InviteCodeID uint
type InviteCode struct {
	gorm.Model

	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}

func NewInviteCodesQuery(db *gorm.DB) *InviteCodesQuery {
	return &InviteCodesQuery{DB: db.Model(InviteCode{})}
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

type StorageMinersQuery struct {
	DB       *gorm.DB
	filtered bool
}
type StorageMinerID uint
type StorageMiner struct {
	gorm.Model

	Address         util.DbAddr `gorm:"unique"`
	Suspended       bool
	SuspendedReason string
	Name            string
	Version         string
	Location        string
	Owner           UserID
}

func NewStorageMinersQuery(db *gorm.DB) *StorageMinersQuery {
	return &StorageMinersQuery{DB: db.Model(StorageMiner{})}
}

func (q *StorageMinersQuery) WithAddress(addr address.Address) *StorageMinersQuery {
	q.DB = q.DB.Where("address = ?", addr.String())
	q.filtered = true
	return q
}

func (q *StorageMinersQuery) SetName(name string) error {
	return q.DB.Update("name", name).Error
}

func (q *StorageMinersQuery) Get() (StorageMiner, error) {
	var miner StorageMiner
	if err := q.DB.Take(&miner).Error; err != nil {
		return StorageMiner{}, err
	}
	return miner, nil
}

func (q *StorageMinersQuery) GetAll() ([]StorageMiner, error) {
	var miners []StorageMiner
	if err := q.DB.Find(&miners).Error; err != nil {
		return nil, err
	}
	return miners, nil
}

func (q *StorageMinersQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (q *StorageMinersQuery) Delete() error {
	if !q.filtered {
		return ErrNotFiltered
	}
	return q.DB.Delete(&StorageMiner{}).Error
}

// RETRIEVAL SUCCESS RECORDS

type RetrievalSuccessRecordsQuery struct{ DB *gorm.DB }
type RetrievalSuccessRecordID uint
type RetrievalSuccessRecord struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time

	Cid   util.DbCID
	Miner string

	Peer         string
	Size         uint64
	DurationMs   int64
	AverageSpeed uint64
	TotalPayment string
	NumPayments  int
	AskPrice     string
}

func NewRetrievalSuccessRecordsQuery(db *gorm.DB) *RetrievalSuccessRecordsQuery {
	return &RetrievalSuccessRecordsQuery{DB: db.Model(RetrievalSuccessRecord{})}
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
type RetrievalFailureRecordID uint
type RetrievalFailureRecord struct {
	gorm.Model

	Miner   string
	Phase   string
	Message string
	Content ContentID
	Cid     util.DbCID
}

func NewRetrievalFailureRecordsQuery(db *gorm.DB) *RetrievalFailureRecordsQuery {
	return &RetrievalFailureRecordsQuery{DB: db.Model(RetrievalFailureRecord{})}
}

func (q *RetrievalFailureRecordsQuery) Count() (int64, error) {
	var count int64
	if err := q.DB.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// PIECE COMM RECORDS

type PieceCommRecordsQuery struct{ DB *gorm.DB }
type PieceCommRecord struct {
	Data  util.DbCID `gorm:"unique"`
	Piece util.DbCID
	Size  abi.UnpaddedPieceSize
}

func NewPieceCommRecordsQuery(db *gorm.DB) *PieceCommRecordsQuery {
	return &PieceCommRecordsQuery{DB: db.Model(PieceCommRecord{})}
}

// MINER STORAGE ASKS

type MinerStorageAsksQuery struct{ DB *gorm.DB }
type MinerStorageAskID uint
type MinerStorageAsk struct {
	gorm.Model

	Miner         string `gorm:"unique"`
	Price         string
	VerifiedPrice string
	MinPieceSize  abi.PaddedPieceSize
	MaxPieceSize  abi.PaddedPieceSize
}

func NewMinerStorageAsksQuery(db *gorm.DB) *MinerStorageAsksQuery {
	return &MinerStorageAsksQuery{DB: db.Model(MinerStorageAsk{})}
}

// SHUTTLES

type ShuttlesQuery struct{ DB *gorm.DB }
type ShuttleID uint
type Shuttle struct {
	gorm.Model

	Handle string `gorm:"unique"`
	Host   string
	PeerID string

	Private bool

	Open bool

	Priority int
}

func NewShuttlesQuery(db *gorm.DB) *ShuttlesQuery {
	return &ShuttlesQuery{DB: db.Model(Shuttle{})}
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
