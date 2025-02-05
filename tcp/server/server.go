package server

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// this handler should be implemented by
// a redis handler that uses the RESP
// takes cmds and calls db.Exec()
type Handler interface {
	Close() error
	HandleF(ctx context.Context, conn net.Conn)
}

func ListenAndServe(addr string, closeCh chan struct{},
	errCh chan error) (net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	log.Printf("bind: %s, start listening...", addr)

	return listener, nil
}

func Serve(addr string, handler Handler) error {
	closeCh := make(chan struct{})
	errCh := make(chan error)
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeCh <- struct{}{}
			return
		}
	}()

	listener, err := ListenAndServe(addr, closeCh, errCh)
	if err != nil {
		log.Fatal(err)
		return err
	}

	go func() {
		select {
		case <-closeCh:
			log.Printf("got exit signal.")
		case err := <-errCh:
			log.Printf("got error: %v", err)
		}
		log.Printf("shutting down...")

		listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		waitDone.Add(1)
		go func() {
			defer waitDone.Done()
			handler.HandleF(ctx, conn)
		}()
	}
	waitDone.Wait()
	return nil
}
