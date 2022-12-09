package deal

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Manager struct {
	db  *gorm.DB
	log *zap.SugaredLogger
}

func NewManager(db *gorm.DB, log *zap.SugaredLogger) *Manager {
	return &Manager{
		db:  db,
		log: log,
	}
}
