package pool

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net"
	"time"
)

// pg is the Postgres implementation of a Backend
type pg struct {
	db         *sql.DB
	address    string
	connstring string
	inflight   map[*Conn]bool
}

func NewPostgresBackend(address, user, pass, database string) *pg {
	connstring := fmt.Sprintf("postgres://%s:%s@%s/%s?connect_timeout=5&sslmode=disable",
		user, pass, address, database)
	return &pg{
		inflight:   make(map[*Conn]bool),
		address:    address,
		connstring: connstring,
	}
}

func (p *pg) Addr() string {
	return p.address
}

func (p *pg) Ping() (s State, err error) {
	// Ensure that the monitoring connection is alive
	if p.db == nil {
		p.db, err = sql.Open("postgres", p.connstring)
		if err != nil {
			return s, err
		}
	}

	p.db.SetMaxOpenConns(1)

	if err = p.db.Ping(); err != nil {
		return s, err
	}

	// Check if we're a primary or a follower
	var inRecovery bool
	row := p.db.QueryRow("select pg_is_in_recovery();")
	if err = row.Scan(&inRecovery); err != nil {
		return s, err
	}

	if inRecovery {
		return READ_ONLY, nil
	} else {
		return READ_WRITE, nil
	}
}

func (p *pg) Connect(t time.Duration) (conn *Conn, err error) {
	conn = new(Conn)
	conn.underlying, err = net.DialTimeout("tcp", p.Addr(), t)
	if err != nil {
		p.Fail()
		return conn, err
	}

	p.inflight[conn] = true
	closeHandler := func() {
		delete(p.inflight, conn)
	}
	conn.RegisterCloseHandler(closeHandler)

	return conn, nil
}

func (p *pg) Fail() {
	for k := range p.inflight {
		k.Close()
	}
}
