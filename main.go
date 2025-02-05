package main

import (
	"godis/tcp/server"
	"log"
)

func main() {
	handler := server.NewEchoHandler()

	if err := server.Serve(":8080", handler); err != nil {
		log.Fatal(err)
	}
}
