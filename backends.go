package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"math"
	"net"
	"sort"
	"sync"
	"time"
)

var ErrNoInstance = errors.New("no available instance")

func NewBackendsMonitor(username, password, database string) (m *BackendsMonitor) {
	m = &BackendsMonitor{
		user: username,
		pass: password,
		db:   database,
	}

	return m
}

type BackendsMonitor struct {
	sync.RWMutex

	// Database credentials used for health checks
	user string
	pass string
	db   string

	// A slice of backends; we enforce the invariant
	// that backends is always sorted by latency.
	backends []*backend
}

type State int

const (
	UNAVAILABLE State = iota
	PRIMARY
	FOLLOWER
)

type backend struct {
	latency time.Duration
	state   State
	address string
}

func (m *BackendsMonitor) Add(addr string) {
	m.Lock()
	defer m.Unlock()

	b := &backend{
		address: addr,
		state:   UNAVAILABLE,
		latency: math.MaxInt64,
	}

	m.backends = append(m.backends, b)
	go m.monitor(b)

	return
}

func (m *BackendsMonitor) DialTimeout(s State, timeout time.Duration) (net.Conn, error) {
	m.RLock()
	defer m.RUnlock()
	for _, backend := range m.backends {
		if backend.state == s {
			// Connect to the first backend we find.
			// If the connection fails; mark the backend as unavailable before
			// returning to the caller.
			conn, err := net.DialTimeout("tcp", backend.address, timeout)
			if err != nil {
				backend.state = UNAVAILABLE
				backend.latency = math.MaxInt64
			}

			return conn, err
		}
	}

	return nil, ErrNoInstance
}

func (m *BackendsMonitor) setBackendState(b *backend, newstate State) {
	m.Lock()
	defer m.Unlock()

	// If we're going to unavailable, max the latency so that this
	// backend is always put at the end of m.backends.
	if newstate == UNAVAILABLE {
		b.latency = math.MaxInt64
	}
	b.state = newstate
}

func (m *BackendsMonitor) setBackendLatency(b *backend, latency time.Duration) {
	m.Lock()
	defer m.Unlock()

	b.latency = latency
	sort.Sort(ByLatency(m.backends))
}

type ByLatency []*backend

func (coll ByLatency) Len() int           { return len(coll) }
func (coll ByLatency) Swap(i, j int)      { coll[i], coll[j] = coll[j], coll[i] }
func (coll ByLatency) Less(i, j int) bool { return coll[i].latency < coll[j].latency }

func (m *BackendsMonitor) monitor(b *backend) {
	var conn *sql.DB
	var err error

	connstring := fmt.Sprintf("postgres://%s:%s@%s/%s?connect_timeout=5&sslmode=disable",
		m.user, m.pass, b.address, m.db)

	log.Printf("Starting monitoring of %s", b.address)

	// Ping the database every second
	ticker := time.Tick(time.Second)
	for _ = range ticker {
		// Ensure that the monitoring connection is alive
		if conn == nil {
			conn, err = sql.Open("postgres", connstring)
			if err != nil {
				log.Printf("[Backend %s]: error establishing connection to database: %s",
					b.address, err)
				conn = nil
				m.setBackendState(b, UNAVAILABLE)
				continue
			}
		}

		if err = conn.Ping(); err != nil {
			log.Printf("[Backend %s]: ping error: %s", b.address, err)
			m.setBackendState(b, UNAVAILABLE)
			continue
		}

		// Check if we're a primary or a follower
		var inRecovery bool
		queryStart := time.Now()
		row := conn.QueryRow("select pg_is_in_recovery();")
		if err = row.Scan(&inRecovery); err != nil {
			log.Printf("[Backend %s]: could not execute query: %s", b.address, err)
			m.setBackendState(b, UNAVAILABLE)
			continue
		}
		m.setBackendLatency(b, time.Since(queryStart))

		if inRecovery {
			m.setBackendState(b, FOLLOWER)
		} else {
			m.setBackendState(b, PRIMARY)
		}
	}
}
