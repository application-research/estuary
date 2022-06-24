package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/application-research/estuary/pinner/types"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	cli "github.com/urfave/cli/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Repo struct {
	DB        *gorm.DB
	Filestore *filestore.Filestore
	Dir       string

	leveldb *leveldb.Datastore

	Cfg *viper.Viper
}

func (r *Repo) Close() error {

	if err := r.leveldb.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to close leveldb: ", err)
	}

	return nil
}

type File struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	Path      string `gorm:"index"`
	Cid       string
	Mtime     time.Time
}

type Pin struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time
	File      uint `gorm:"index"`
	Cid       string
	RequestID string `gorm:"index"`
	Status    types.PinningStatus
}

func findRepo(cctx *cli.Context) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	wd = filepath.Clean(wd)

	home, err := homedir.Dir()
	if err != nil {
		return "", err
	}

	for wd != "/" {
		if wd == home {
			return "", fmt.Errorf("barge directory not found, have you run `barge init`?")
		}

		dir := filepath.Join(wd, ".barge")
		st, err := os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				return "", err
			}

			wd = filepath.Dir(wd)
			continue
		}

		if !st.IsDir() {
			return "", fmt.Errorf("found .barge, it wasnt a file")
		}

		return dir, nil
	}

	return "", fmt.Errorf("barge directory not found, have you run `barge init`?")
}

func openRepo(cctx *cli.Context) (*Repo, error) {
	dir, err := findRepo(cctx)
	if err != nil {
		return nil, err
	}

	reporoot := filepath.Dir(dir)

	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("json")
	v.AddConfigPath(dir)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			if err := v.WriteConfigAs("config"); err != nil {
				return nil, err
			}
		} else {
			fmt.Printf("read err: %#v\n", err)
			return nil, err
		}
	}

	dbdir := dir
	cfgdbdir, ok := v.Get("database.directory").(string)
	if ok && cfgdbdir != "" {
		dbdir = cfgdbdir
	}

	db, err := gorm.Open(sqlite.Open(filepath.Join(dbdir, "barge.db")), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&File{})
	db.AutoMigrate(&Pin{})

	lds, err := leveldb.NewDatastore(filepath.Join(dbdir, "leveldb"), &leveldb.Options{
		NoSync: true,
	})
	if err != nil {
		return nil, err
	}

	fsmgr := filestore.NewFileManager(lds, reporoot)
	fsmgr.AllowFiles = true

	ffs, err := flatfs.CreateOrOpen(filepath.Join(dbdir, "flatfs"), flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		return nil, err
	}

	fbs := blockstore.NewBlockstoreNoPrefix(ffs)
	fstore := filestore.NewFilestore(fbs, fsmgr)

	return &Repo{
		DB:        db,
		Filestore: fstore,
		Dir:       reporoot,
		Cfg:       v,
	}, nil
}
