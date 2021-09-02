package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-filestore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/mitchellh/go-homedir"
	wnfspriv "github.com/qri-io/wnfs-go/private"
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

func (r *Repo) isPrivate() bool {
	return r.Cfg.IsSet("private.key")
}

func (r *Repo) initPrivateRoot(ctx context.Context, name string) (*wnfspriv.Root, error) {
	store, err := wnfspriv.NewStore(ctx, r.Filestore.MainBlockstore())
	if err != nil {
		return nil, err
	}

	key := wnfspriv.NewKey()
	root, err := wnfspriv.NewEmptyRoot(ctx, store, name, key)
	if err != nil {
		return nil, err
	}

	if _, err := root.Put(); err != nil {
		return nil, err
	}

	return root, nil
}

func (r *Repo) setPrivateConfig(root *wnfspriv.Root) error {
	r.Cfg.Set("private.key", root.Key())
	pn, err := root.PrivateName()
	if err != nil {
		return err
	}
	r.Cfg.Set("private.privateName", string(pn))
	r.Cfg.Set("private.cid", root.Cid().String())
	return nil
}

func (r *Repo) loadPrivate(ctx context.Context) (*wnfspriv.Root, error) {
	store, err := wnfspriv.NewStore(ctx, r.Filestore.MainBlockstore())
	if err != nil {
		return nil, err
	}

	rootKey := &wnfspriv.Key{}
	if keystr := r.Cfg.GetString("private.key"); keystr == "" {
		return nil, fmt.Errorf("private key is missing")
	} else {
		if err := rootKey.Decode(keystr); err != nil {
			return nil, fmt.Errorf("invalid private key: %w", err)
		}
	}

	var privateName wnfspriv.Name
	if namestr := r.Cfg.GetString("private.privateName"); namestr == "" {
		return nil, fmt.Errorf("private name is missing")
	} else {
		privateName = wnfspriv.Name(namestr)
	}

	var hamtCID cid.Cid
	if id := r.Cfg.GetString("private.cid"); id == "" {
		return nil, fmt.Errorf("private cid is missing")
	} else {
		hamtCID, err = cid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("invalid private CID: %w", err)
		}
	}

	name := r.Cfg.GetString("collection.name")
	return wnfspriv.LoadRoot(ctx, store, name, hamtCID, *rootKey, privateName)
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
