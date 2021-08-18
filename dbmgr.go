package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/application-research/estuary/util"
	"github.com/ipfs/go-cid"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type DBMgr struct {
	db *gorm.DB
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

func (mgr *DBMgr) CreateUser(user *User) error {
	if err := mgr.db.Create(user).Error; err != nil {
		return fmt.Errorf("error creating user: %w", err)
	}

	return nil
}

func (mgr *DBMgr) GetUserByUsername(username string) (*User, error) {
	var user *User
	if err := mgr.db.First(user, "username = ?", username).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("user does not exist with username '%s'", username)
		}

		return nil, err
	}

	return user, nil
}

func (mgr *DBMgr) UserExistsWithUsername(username string) (bool, error) {
	var count int64
	if err := mgr.db.Model(&User{}).Where("username = ?", username).Count(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}

// AUTH TOKENS

func (mgr *DBMgr) CreateAuthToken(authToken *AuthToken) error {
	return mgr.db.Create(authToken).Error
}

// CONTENTS

func (mgr *DBMgr) GetContentByID(id uint) (Content, error) {
	var content Content
	if err := mgr.db.First(&content, "id = ?", id).Error; err != nil {
		return Content{}, err
	}

	return content, nil
}

func (mgr *DBMgr) GetActiveContents() ([]Content, error) {
	var contents []Content
	if err := mgr.db.Find(&contents, "active").Error; err != nil {
		return nil, err
	}

	return contents, nil
}

func (mgr *DBMgr) GetContentsByUserID(userID uint) ([]Content, error) {
	var contents []Content
	if err := mgr.db.Find(&contents, "user_id = ?", userID).Error; err != nil {
		return nil, err
	}

	return contents, nil
}

func (mgr *DBMgr) DeleteContentByID(id uint) error {
	return mgr.db.Delete(&Content{}, id).Error
}

// OBJECTS

func (mgr *DBMgr) ObjectExistsWithCid(cid cid.Cid) (bool, error) {
	var count int64
	if err := mgr.db.Model(&Object{}).Where("cid = ?", cid.Bytes()).Count(&count).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, fmt.Errorf("object does not exist with cid '%s'", cid)
		}

		return false, err
	}

	return count > 0, nil
}

func (mgr *DBMgr) DeleteUnreferencedObjects(ids []uint) error {
	return mgr.db.Where(
		"(?) = 0 and id in ?",
		mgr.db.Model(ObjRef{}).Where("object = objects.id").Select("count(1)"), ids,
	).Delete(Object{}).Error
}

// OBJ REFS

func (mgr *DBMgr) DeleteObjRefByPinID(pinID uint) error {
	return mgr.db.Where("pin = ?", pinID).Delete(ObjRef{}).Error
}

// DEALS

func (mgr *DBMgr) GetDealsByContentIDs(contentIDs []uint) ([]contentDeal, error) {
	var deals []contentDeal
	if err := mgr.db.Find(&deals, "content in ?", contentIDs).Error; err != nil {
		return nil, err
	}

	return deals, nil
}
