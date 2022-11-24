package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/facebookgo/atomicfile"
	"io"
	"os"
	"path/filepath"
)

var ErrNotInitialized = errors.New("node not initialized, please run configure")

// encode configuration with JSON
func encode(cfg interface{}, w io.Writer) error {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	buf, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func load(cfg interface{}, filename string) (err error) {
	f, err := os.Open(filepath.Clean(filename))
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return err
	}

	defer func() {
		if errC := f.Close(); errC != nil {
			err = errC
		}
	}()

	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return fmt.Errorf("failure to decode config: %s", err)
	}
	return err
}

// save writes the config from `cfg` into `filename`.
func save(cfg interface{}, filename string) error {
	err := os.MkdirAll(filepath.Dir(filename), 0750)
	if err != nil {
		return err
	}

	f, err := atomicfile.New(filename, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	return encode(cfg, f)
}

var ErrEmptyPath = errors.New("node not initialized, please run configure")
