package main

import "github.com/ysoding/bitcask"

func main() {

	db, err := bitcask.Open(bitcask.WithDBIndexerType(bitcask.BTree))
	if err != nil {
		panic(err)
	}
	defer db.Close()
}
