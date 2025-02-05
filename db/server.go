package db

// import (
// 	"godis/interfaces"
// 	"godis/redis/protocol"
// 	"log"
// 	"strings"
// )

// type Server struct {
// }

// func (server *Server) Exec(conn interfaces.Connection, cmdL [][]byte) (result protocol.Reply) {
// 	// cmdN refers to the name of cmd line
// 	cmdN := strings.ToLower(string(cmdL[0]))
// 	cmd, ok := CommandMap[cmdN]
// 	if !ok {
// 		log.Printf("undefined command %s", cmdN)
// 		return nil
// 	}
// 	if cmdN == "ping" {
// 		return Ping(conn, cmdL[1:])
// 	} else {
// 		return cmd.executor(r, cmdL)
// 	}
// }
