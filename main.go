package main

import (
	"fmt"
	"log"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	// 	handler := server.NewEchoHandler()

	// 	if err := server.Serve(":8080", handler); err != nil {
	// 		log.Fatal(err)
	// 	}
	fmt.Printf("%v", uint(1)<<uint(16))
}
