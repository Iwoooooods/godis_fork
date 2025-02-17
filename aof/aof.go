package aof

import "godis/redis/protocol"

type Persist interface {
	Persist(args [][]byte)
}

type AOFPersistor struct {
	cmdCh chan *protocol.MultiBulkReply
}

func (ap *AOFPersistor) Persist(args [][]byte) {
}
