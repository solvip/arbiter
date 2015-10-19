package pool

import (
	"time"
)

type State int

const (
	UNAVAILABLE State = iota
	READ_ONLY
	READ_WRITE
)

//go:generate stringer -type=State

type Backend interface {
	// Ping will be periodically called by pool in order to assess the health and state
	// of a backend.
	// Any error will cause this backend to be temporarily removed from the pool.
	Ping() (State, error)

	// Address returns the address of this backend
	Addr() string

	// Connect returns a Conn to the specified backend within time.Duration.
	// Clients must Close() the conn when they're done with it.
	Connect(time.Duration) (*Conn, error)

	// Fail closes all connections to a specified backend.  It should be called by a client
	// that cannot write to or read from a connection previously returned by Connect().
	Fail()
}
