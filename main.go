package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"log"
)

type Message struct {
	Sender   string
	Receiver string
	Content  string
}

func main() {
	// 打开数据库
	db, err := badger.Open(badger.DefaultOptions("./data"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 写入数据
	message1 := &Message{Sender: "user1", Receiver: "user2", Content: "hello"}
	message2 := &Message{Sender: "user1", Receiver: "user2", Content: "world"}
	message3 := &Message{Sender: "user2", Receiver: "user1", Content: "hi"}

	for _, message := range []*Message{message1, message2, message3} {
		key := []byte(fmt.Sprintf("%s:%s", message.Sender, message.Receiver))
		value, err := json.Marshal(message)
		if err != nil {
			log.Fatal(err)
		}
		err = db.Update(func(txn *badger.Txn) error {
			err := txn.Set(key, value)
			return err
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// 查询数据
	pageSize := 10
	page := 1
	sender := "user1"
	receiver := "user2"
	prefix := []byte(fmt.Sprintf("%s:%s", sender, receiver))
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = true
	options.Reverse = true

	count := 0
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			message := &Message{}
			err = json.Unmarshal(value, message)
			if err != nil {
				return err
			}
			count++
			if count > (page-1)*pageSize && count <= page*pageSize {
				fmt.Printf("Sender: %s, Receiver: %s, Content: %s\n", message.Sender, message.Receiver, message.Content)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
