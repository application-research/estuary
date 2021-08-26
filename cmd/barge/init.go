package main

import (
	"fmt"
	"os"

	cli "github.com/urfave/cli/v2"
)

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "initialize a barge repo in the current directory",
	Action: func(cctx *cli.Context) error {
		inited, err := repoIsInitialized()
		if err != nil {
			return err
		}

		if inited {
			fmt.Println("repo already initialized")
			return nil
		}

		if err := os.Mkdir(".barge", 0775); err != nil {
			return err
		}

		// other stuff?
		return nil
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
