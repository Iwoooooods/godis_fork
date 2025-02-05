package server

import (
	"context"
	"log"
	"net"
	"sync"

	gsync "godis/lib/sync"
	"godis/redis/parser"
	"godis/tcp/client"
)

type EchoHandler struct {
	activeConn sync.Map
	closed     gsync.Boolean
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (h *EchoHandler) HandleF(ctx context.Context, conn net.Conn) {

	if h.closed.Get() {
		conn.Close()
		return
	}

	client := &client.Client{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})

	resultCh := parser.ParseStream(conn)

	for payload := range resultCh {
		if payload.Err != nil {
			log.Printf("got error in payload: %v", payload.Err)
			continue
		}
		log.Print(payload.Data)
		_, err := conn.Write(payload.Data.ToBytes())
		if err != nil {
			log.Printf("got error writing into reply: %v", err)
			continue
		}
	}
	// for {
	// 	msg, err := reader.ReadString('\n')
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			log.Printf("EOF: closing connection")
	// 		} else {
	// 			log.Printf("%s", err.Error())
	// 		}
	// 	}
	// 	client.Wg.Add(1)
	// 	// handle goroutine
	// 	go func(msg string) {
	// 		defer client.Wg.Done()
	// 		// TODO: some deserialization here
	// 		log.Print(msg)
	// 		b := []byte(fmt.Sprint(msg + "?"))
	// 		conn.Write(b)
	// 	}(msg)
	// }
}

func (h *EchoHandler) Close() error {
	log.Printf("handler closing...")

	h.closed.Set(true)
	h.activeConn.Range(func(key any, value any) bool {
		client := key.(*client.Client)
		if err := client.Close(); err != nil {
			return false
		}
		return true
	})
	return nil
}
