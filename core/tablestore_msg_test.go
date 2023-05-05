package core

import (
	"go-tablestore/config"
	"testing"
)

func getTestIMDB() *TableStore {
	db, err := NewTableStore(config.DBConfig{DataDir: "./data"})
	if err != nil {
		panic(err)
	}
	return db
}

func TestIMDB_PrintAll(t *testing.T) {
	imdb := getTestIMDB()
	defer imdb.CloseGracefully()

	err := imdb.PrintAll(_msgKeyPrefix)
	if err != nil {
		panic(err)
	}
}

func TestIMDB_ClearAllData(t *testing.T) {
	imdb := getTestIMDB()
	err := imdb.ClearAllData()
	if err != nil {
		panic(err)
	}
}
