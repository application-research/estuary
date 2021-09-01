package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	cli "github.com/urfave/cli/v2"
)

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "initialize a barge repo in the current directory",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "collection",
			Usage: "specify an alternative name for this collection of data",
		},
		&cli.StringFlag{
			Name:  "description",
			Usage: "optionally set a description for this collection of data",
		},
		&cli.StringFlag{
			Name:  "dbdir",
			Usage: "set the location of the barge repo database",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context

		inited, err := repoIsInitialized()
		if err != nil {
			return err
		}

		if inited {
			fmt.Println("repo already initialized")
			return nil
		}

		c, err := loadClient(cctx)
		if err != nil {
			return err
		}

		if err := os.Mkdir(".barge", 0775); err != nil {
			return err
		}

		cwd, err := os.Getwd()
		if err != nil {
			return err
		}

		v := viper.New()
		v.SetConfigName("config")
		v.SetConfigType("json")
		v.AddConfigPath(filepath.Join(cwd, ".barge"))

		if dbdir := cctx.String("dbdir"); dbdir != "" {
			v.Set("database.directory", dbdir)
		}

		if err := v.WriteConfigAs(filepath.Join(filepath.Join(cwd, ".barge", "config.json"))); err != nil {
			return err
		}

		r, err := openRepo(cctx)
		if err != nil {
			return err
		}

		colname := cctx.String("collection")
		desc := cctx.String("description")

		wd, err := os.Getwd()
		if err != nil {
			return err
		}

		if colname == "" {
			buf := make([]byte, 3)
			rand.Read(buf)

			colname = fmt.Sprintf("%s-%x", filepath.Base(wd), buf)
		}
		if desc == "" {
			desc = wd
		}

		col, err := c.CollectionsCreate(ctx, colname, desc)
		if err != nil {
			return err
		}

		r.Cfg.Set("collection.uuid", col.UUID)
		r.Cfg.Set("collection.name", col.Name)

		return r.Cfg.WriteConfig()
	},
}

func repoIsInitialized() (bool, error) {
	st, err := os.Stat(".barge")
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	if st.IsDir() {
		return true, nil
	}

	return false, fmt.Errorf(".barge is not a directory")
}
