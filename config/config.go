package config

/*
import (
	"encoding/json"
	"fmt"
	"os"
)
*/

type Config struct {
	ListenAddrs   []string
	AnnounceAddrs []string

	Blockstore string

	WriteLog          string
	HardFlushWriteLog bool
	WriteLogTruncate  bool
	NoBlockstoreCache bool
	NoLimiter         bool

	Libp2pKeyFile string

	Datastore string

	WalletDir string

	BitswapConfig BitswapConfig
}

type BitswapConfig struct {
	MaxOutstandingBytesPerPeer int64
	TargetMessageSize          int
}

func (cfg *Config) load(filename string) error {
	/*
		f, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				err = ErrNotInitialized
			}
			return err
		}
		defer f.Close()
		if err := json.NewDecoder(f).Decode(cfg); err != nil {
			return fmt.Errorf("failure to decode config: %s", err)
		}
	*/
	return nil
}

// WriteConfigFile writes the config from `cfg` into `filename`.
func WriteConfigFile(filename string, cfg interface{}) error {
	/*
		err := os.MkdirAll(filepath.Dir(filename), 0755)
		if err != nil {
			return err
		}

		f, err := atomicfile.New(filename, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		return encode(f, cfg)
	*/
	return nil
}
