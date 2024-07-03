package main

import (
	"ms-go/app/consumers"
	_ "ms-go/db"
	"ms-go/router"
)

func main() {
	go consumers.ConsumerRails()

	router.Run()
}
