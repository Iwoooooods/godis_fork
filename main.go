package main

import (
	"godis/tcp/server"
	"log"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	handler := server.NewEchoHandler()

	if err := server.Serve(":8080", handler); err != nil {
		log.Fatal(err)
	}
}
