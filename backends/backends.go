package backends

// Keep count of and monitor available backends

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net"
	"sync"
	"time"
)

var PrimaryNotFound = errors.New("primary database instance not found")
var InstanceNotFound = errors.New("no available database instance found")

var (
	mu       sync.RWMutex
	backends []*backend
)

type backend struct {
	// The address of the backend, i.e., ip:port or host
	address string

	// True if the last health check was able to connect
	available bool

	// True if the backend is a primary
	primary bool
}

// Return the number of backends defined
func Count() int {
	mu.RLock()
	defer mu.RUnlock()

	return len(backends)
}

// Add a backend and start monitoring it
func Add(address, database, user, pass string) {
	mu.Lock()
	defer mu.Unlock()

	b := &backend{
		address:   address,
		available: false,
		primary:   false,
	}

	backends = append(backends, b)

	go b.monitor(database, user, pass)
}

// Return a connection to the primary backend
func DialPrimary() (c net.Conn, err error) {
	retry := 2 * time.Millisecond

	timeout := time.Now().Add(30 * time.Second)
	for time.Now().Before(timeout) {
		var addr string
		addr, err = getPrimary()
		if err == nil {
			c, err = net.DialTimeout("tcp", addr, 5*time.Second)
			if err == nil {
				return
			}

		}

		time.Sleep(retry)
		retry = retry * 2
	}

	return c, err
}

// Return a connection to a random backend
func Dial() (c net.Conn, err error) {
	retry := 2 * time.Millisecond

	timeout := time.Now().Add(30 * time.Second)
	for time.Now().Before(timeout) {
		var addr string
		addr, err = getBackend()
		if err == nil {
			c, err = net.DialTimeout("tcp", addr, 5*time.Second)
			if err == nil {
				return
			}

		}

		time.Sleep(retry)
		retry = retry * 2
	}

	return c, err
}

func getPrimary() (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	for _, b := range backends {
		if b.available && b.primary {
			return b.address, nil
		}
	}

	return "", PrimaryNotFound
}

func getBackend() (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	for _, b := range backends {
		if b.available {
			return b.address, nil
		}
	}

	return "", InstanceNotFound
}

// Monitor a backend
func (b *backend) monitor(database, user, pass string) {
	var err error
	var conn *sql.DB

	// Ping the database every second
	ping := time.Tick(time.Second)

	log.Printf("Starting monitoring of %s", b.address)

	connstring := fmt.Sprintf("postgres://%s:%s@%s/%s?connect_timeout=5&sslmode=disable",
		user, pass, b.address, database)

	for _ = range ping {
		// Ensure that the monitoring connection is alive
		if conn == nil {
			conn, err = sql.Open("postgres", connstring)
			if err != nil {
				log.Printf("[Backend %s]: error establishing connection to database: %s",
					b.address, err)
				conn = nil
				b.setAvailable(false)
				continue
			}
		}

		if err = conn.Ping(); err != nil {
			log.Printf("[Backend %s]: ping error: %s", b.address, err)
			b.setAvailable(false)
			continue
		}

		// Check if we're a primary or a follower
		var inRecovery bool
		row := conn.QueryRow("select pg_is_in_recovery();")
		if err = row.Scan(&inRecovery); err != nil {
			log.Printf("[Backend %s]: could not execute query: %s", b.address, err)
			b.setAvailable(false)
			continue
		}

		b.setAvailable(true)
		if inRecovery {
			b.setPrimary(false)
		} else {
			b.setPrimary(true)
		}
	}

	return
}

func (b *backend) setAvailable(v bool) {
	mu.Lock()
	defer mu.Unlock()
	b.available = v
	if v == false {
		b.primary = false
	}
}

func (b *backend) setPrimary(v bool) {
	mu.Lock()
	b.primary = v
	mu.Unlock()
}
