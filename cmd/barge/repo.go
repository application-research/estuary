package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/mitchellh/go-homedir"
	cli "github.com/urfave/cli/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Repo struct {
	DB        *gorm.DB
	Filestore *filestore.Filestore
	Dir       string
}

type File struct {
	gorm.Model
	Path  string
	Cid   string
	Mtime time.Time
}

type Pin struct {
	gorm.Model
	File      uint
	Cid       string
	RequestID string
	Status    string
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

	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "barge.db")), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&File{})
	db.AutoMigrate(&Pin{})

	lds, err := leveldb.NewDatastore(filepath.Join(dir, "leveldb"), nil)
	if err != nil {
		return nil, err
	}

	fsmgr := filestore.NewFileManager(lds, reporoot)
	fsmgr.AllowFiles = true

	ffs, err := flatfs.CreateOrOpen(filepath.Join(dir, "flatfs"), flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		return nil, err
	}

	fbs := blockstore.NewBlockstoreNoPrefix(ffs)
	fstore := filestore.NewFilestore(fbs, fsmgr)

	return &Repo{
		DB:        db,
		Filestore: fstore,
		Dir:       reporoot,
	}, nil
}
