package pool

import (
	"net"
	"time"
)

// Conn wraps a net.Conn, implementing all it's methods.
// This allows us to intercept some of them.
type Conn struct {
	underlying    net.Conn
	closeHandlers []func()
}

func (c *Conn) Read(b []byte) (n int, err error) {
	return c.underlying.Read(b)
}

func (c *Conn) Write(b []byte) (n int, err error) {
	return c.underlying.Write(b)
}

func (c *Conn) Close() error {
	for _, h := range c.closeHandlers {
		h()
	}
	return c.underlying.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.underlying.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.underlying.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.underlying.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.underlying.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.underlying.SetWriteDeadline(t)
}

func (c *Conn) RegisterCloseHandler(f func()) {
	c.closeHandlers = append(c.closeHandlers, f)
}
