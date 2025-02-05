package client

import (
	"net"
	"sync"
)

type Client struct {
	Conn net.Conn
	// ensures all goroutine complete
	// before the client closes
	Wg sync.WaitGroup
}

func (c *Client) Close() error {
	c.Wg.Wait()
	if err := c.Conn.Close(); err != nil {
		return err
	}
	return nil
}
