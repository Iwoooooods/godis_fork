package tcp_test

import (
	"godis/tcp/server"
	"log"
	"net"
	"testing"
	"time"
)

func TestServerRunning(t *testing.T) {
	closeCh := make(chan struct{})
	errCh := make(chan error)
	var listener net.Listener
	go func() {
		var err error
		listener, err = server.ListenAndServe(":8080", closeCh, errCh)
		if err != nil {
			log.Fatalf("got error: %v", err)
		}
	}()

	time.Sleep(time.Second)

	log.Printf("%v", listener)
}
