package db

import (
	"errors"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type DB struct {
	SQLLite *gorm.DB
}

var sqlliteinstance *DB = nil
var sqliteDBName string 

func GetSqlLiteInstance() (*gorm.DB, error) {

	if sqlliteinstance == nil {

		if len(sqliteDBName) < 1 {
			return nil, errors.New("set sqllitedbname first")
		}

		instance, err := NewSqlLite(sqliteDBName, &gorm.Config{})
		if err != nil {
			return nil, err
		}
		sqlliteinstance := &DB{}
		sqlliteinstance.SQLLite = instance

	}
	return sqlliteinstance.SQLLite, nil
}
func NewSqlLite(db string, config *gorm.Config) (*gorm.DB, error) {

	return gorm.Open(sqlite.Open(db), config)
}

func SetSqlLiteDB(dbPath string) {

	sqliteDBName = dbPath
}
