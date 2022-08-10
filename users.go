package main

import (
	"github.com/application-research/estuary/util"
	"time"

	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	UUID          string `gorm:"unique"`
	WalletAddress string // The address of the user's ERC20 wallet as a hex string
	// TODO: Implement DiD resolution, store ENS
	DID string // A DID for the users On-Chain identity

	// note (al) - Deprecating these fields in favor of just using SIWE and PubKey
	Username string // The viewer expects this to be present
	//Salt     string
	//PassHash string

	// TODO: Implement some sort of ENS resolution tied to an Email address
	UserEmail string

	// TODO: figure this out
	// note (al) - Deprecating their definition of a wallet as a filecoin address for an ERC20 address
	// Parts of the library expoect the wallet to be a filecoin address, so I am keeping this field for backwards compatibility
	Address   util.DbAddr
	authToken AuthToken
	Perm      int
	Flags     int

	StorageDisabled bool
}

func (u *User) FlagSplitContent() bool {
	return u.Flags&8 != 0
}

type AuthToken struct {
	gorm.Model
	Token      string `gorm:"unique"`
	User       uint
	UploadOnly bool
	Expiry     time.Time
}

type InviteCode struct {
	gorm.Model
	Code      string `gorm:"unique"`
	CreatedBy uint
	ClaimedBy uint
}
