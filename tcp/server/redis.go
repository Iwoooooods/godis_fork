package server

import (
	"context"
	"godis/db"
	"godis/interfaces"
	gsync "godis/lib/sync"
	"godis/redis/parser"
	"godis/redis/protocol"
	"godis/tcp/client"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// implements the handler that listens and serves the connection
// closed is set to True when closing the connection
// stops the handler from handling new connections
// db is the key part for Redis
// activeConn is a Map for connections alive
type RedisHandler struct {
	closed     gsync.Boolean
	db         interfaces.DB
	activeConn sync.Map
}

func NewRedisHandler() *RedisHandler {
	return &RedisHandler{
		db: db.NewStandAloneDb(),
	}
}

func (r *RedisHandler) Close() error {
	log.Printf("handler closing...")

	r.closed.Set(true)
	r.activeConn.Range(func(key any, value any) bool {
		client := key.(*client.Client)
		_ = client.Close()
		return true
	})
	return nil
}

func (r *RedisHandler) closeClient(client *client.Connection) {
	client.Close()
	r.activeConn.Delete(client)
}

func (r *RedisHandler) HandleF(ctx context.Context, conn net.Conn) {
	if r.closed.Get() {
		_ = conn.Close()
		return
	}

	client := client.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	payloadCh := parser.ParseStream(conn)

	for payload := range payloadCh {

		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// should close the connection
				log.Printf("end of the connection")
				return
			}
			// protocol error
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				log.Printf("connection closed: %s", client.RemoteAddr())
				return
			}
			continue
		}

		if payload.Data == nil {
			log.Printf("empty payload")
			continue
		}

		reply, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			log.Printf("requires multi bulk protocol")
			continue
		}
		resp := r.db.Exec(client, reply.Args)
		if resp != nil {
			_, _ = client.Write(resp.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}

	}

}
