package client

import (
	gsync "godis/lib/sync"
	"log"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn net.Conn
	// wait until finishing sending data for graceful shutdown
	sending gsync.Wait
	// lock while server sending response
	mu   sync.Mutex
	flag uint
}

// connPool is initialized when the package is loaded
// which is a package-level variable
// before main function starts
var connPool = sync.Pool{
	// New() will be called when:
	// the pool is empty or,
	// Get() is called
	New: func() any {
		return &Connection{}
	},
}

func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *Connection) Close() error {
	// wait a few seconds for finishing sending data
	if c.sending.WaitWithTimeout(10 * time.Second) {
		log.Printf("closing connection timed out")
	}
	c.conn.Close()
	// put the connection instance back to pool
	connPool.Put(c)
	return nil
}

func NewConn(conn net.Conn) *Connection {
	// get an instant connection from pool
	c, ok := connPool.Get().(*Connection)
	if !ok {
		log.Printf("connPool gives a wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sending.Add(1)
	defer func() {
		// clean up
		c.sending.Done()
	}()

	return c.conn.Write(b)
}
