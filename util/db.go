package util

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func SetupDatabase(dbval string) (*gorm.DB, error) {
	parts := strings.SplitN(dbval, "=", 2)
	// if len(parts) == 1 {
	// 	return nil, fmt.Errorf("format for database string is 'DBTYPE=PARAMS'")
	// }

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

	return db, nil
}
