package core

import (
	"fmt"
	"go-tablestore/config"
	"sync"
	"testing"
	"time"
)

func getTestIMDB() *IMDB {
	db, err := NewBadgerDb(config.DBConfig{DataDir: "./data"})
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

func TestIMDB_SaveMessage(t *testing.T) {
	imdb := getTestIMDB()
	defer imdb.CloseGracefully()

	err := imdb.SaveMessage(Message{
		MsgID: 1,
		Columns: map[string]interface{}{
			"sender":      123,
			"receiver":    456,
			"create_time": "2023-05-02 17:56:00",
		},
	})
	if err != nil {
		panic(err)
	}

	if err = imdb.PrintAll(""); err != nil {
		panic(err)
	}
}

// test parallels insert
func TestIMDB_SaveMessage2(t *testing.T) {
	imdb := getTestIMDB()
	defer imdb.CloseGracefully()

	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(index int) {
			for i := 0; i < 20; i++ {
				err := imdb.SaveMessage(Message{
					MsgID: uint64(i + index*20),
					Columns: map[string]interface{}{
						"sender":      i,
						"receiver":    i*10 + index*20,
						"create_time": time.Now().Format("2006-01-02 15:04:05"),
					},
				})
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestIMDB_CreateIndex(t *testing.T) {
	imdb := getTestIMDB()
	defer imdb.CloseGracefully()

	err := imdb.CreateIndex(IndexSetting{
		Fields: []string{"sender", "receiver", "create_time"},
		Enable: true,
	})

	if err != nil {
		panic(err)
	}
}

func TestIMDB_GetIndex(t *testing.T) {
	imdb := getTestIMDB()
	defer imdb.CloseGracefully()

	res, exist, err := imdb.GetIndex([]string{"sender", "receiver", "create_time"})
	if err != nil {
		panic(err)
	}
	fmt.Println(exist)
	fmt.Println(res)
}

func TestIMDB_ClearAllData(t *testing.T) {
	imdb := getTestIMDB()
	err := imdb.ClearAllData()
	if err != nil {
		panic(err)
	}
}
