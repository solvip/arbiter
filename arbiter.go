package main

import (
	"bufio"
	"code.google.com/p/gcfg"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/solvip/arbiter/backends"
)

var ErrUnsupportedProtocol = errors.New("unsupported protocol version")

func start(listenAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting client: %s", err)
			continue
		}

		go handleClientConnection(conn)
	}
}

func main() {
	cfgPath := flag.String("f", "/etc/arbiter/config.ini", "The path to the arbiter configuration file")
	flag.Parse()

	var config struct {
		Main struct {
			Listen   string
			Backends string
		}

		Health struct {
			Username string
			Password string
			Database string
		}
	}

	if err := gcfg.ReadFileInto(&config, *cfgPath); err != nil {
		log.Fatalf("Could not load configuration file: %s", err)
	}

	for _, v := range strings.Split(config.Main.Backends, ",") {
		backend := strings.TrimSpace(v)
		if _, _, err := net.SplitHostPort(backend); err != nil {
			log.Fatalf("Invalid backend %s", backend)
		}

		backends.Add(backend,
			config.Health.Database,
			config.Health.Username,
			config.Health.Password)
	}

	if backends.Count() == 0 {
		log.Fatalf("No backends defined in configuration")
	}

	// Start profiling server
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	/* Config looks good.  Let's go. */
	log.Printf("Starting up...")
	if err := start(config.Main.Listen); err != nil {
		log.Fatalf("Could not start Arbiter: %s", err)
	}

	return
}

type connection struct {
	wg   sync.WaitGroup
	conn net.Conn

	// Messages received on conn will be published on R.
	R chan Message

	// Messages received on W will be written to conn.
	W chan Message

	// semaphore-channel to close W.
	cw chan struct{}

	// emu guards err
	emu sync.RWMutex
	err error

	builder MessageBuilder
}

func (c *connection) Error() error {
	c.emu.RLock()
	defer c.emu.RUnlock()
	return c.err
}

func (c *connection) setError(err error) {
	c.emu.Lock()
	defer c.emu.Unlock()
	if c.err == nil {
		c.err = err
	}
}

func (c *connection) Close() (err error) {
	c.cw <- struct{}{} // close W
	err = c.conn.Close()
	c.wg.Wait()
	return err
}

func newConnection(conn net.Conn, builder MessageBuilder) *connection {
	c := new(connection)
	c.conn = conn
	c.builder = builder

	// 32 is just some arbitary number somewhat larger than 0.
	c.W = make(chan Message, 32)
	c.R = make(chan Message, 32)

	// Buffered so that sending to cw, in order to close, doesn't block.
	// If the goroutine is gone by that point - the gc will eventually
	// remove cw.
	c.cw = make(chan struct{}, 1)

	// Start the reading side
	go func() {
		c.wg.Add(1)
		defer close(c.R)
		defer c.wg.Done()

		r := bufio.NewReader(conn)
		for {
			b, err := r.ReadByte()
			if err != nil {
				c.setError(err)
				return
			}

			msg, err := c.builder(b)
			if err != nil {
				c.setError(err)
				return
			}

			if err := msg.DecodeFrom(r); err != nil {
				c.setError(err)
				return
			}

			c.R <- msg
		}
	}()

	// Start the writing side
	go func() {
		c.wg.Add(1)
		defer c.wg.Done()

		w := bufio.NewWriter(conn)

		var err error

		for {
			select {
			case <-c.cw:
				close(c.W)
				return
			case msg := <-c.W:
				if err = msg.EncodeTo(w); err != nil {
					c.setError(err)
					return
				}

				if len(c.W) == 0 {
					if err = w.Flush(); err != nil {
						c.setError(err)
						return
					}
				}
			}
		}
	}()

	return c
}

// startup - Handle the startup of a connection.
func handleStartup(conn net.Conn) (msg StartMessage, err error) {
	const (
		INIT = iota
		INIT_SSL
	)

	state := INIT

	// The only two possible state transitions are:
	// INIT -> INIT_SSL -> INIT -> return
	// INIT -> return
	for i := 0; i < 3; i++ {
		switch state {

		case INIT:
			if msg, err = readStartMessage(conn); err != nil {
				return
			}

			switch msg.(type) {
			case *Startup:
				if msg.MajorVersion() == 3 && msg.MinorVersion() == 0 {
					return
				} else {
					return nil, ErrUnsupportedProtocol
				}

			case *SSLRequest:
				state = INIT_SSL

			case *CancelRequest:
				return
			}

		case INIT_SSL:
			if _, err = conn.Write([]byte{'N'}); err != nil {
				return
			} else {
				state = INIT
			}
		}
	}

	// Too many state transitions
	return msg, ErrProtocolViolation
}

// handle the authentication phase.
// As we cannot possibly pool sessions unless knowing the credentials, i.e., postgres
// always sends us a unique salt for each md5 auth request, we need to MITM the
// authentication phase.
// If the server requests MD5 - we lie to the client and tell it we want password.
// If the server trusts us - we extend that trust to the client.
func handleAuthentication(frontend, backend *connection, startMsg StartMessage) (err error) {
	checkErr := func() (err error) {
		if err = frontend.Error(); err != nil {
			return
		}

		if err = backend.Error(); err != nil {
			return
		}

		return nil
	}

	const (
		INIT = iota
		AUTHREQ
		PASSWORD
		MD5PASSWORD
		AUTHENTICATED
	)

	var authreq *AuthenticationRequest

	for state := INIT; ; {
		switch state {
		case INIT:
			backend.W <- startMsg
			state = AUTHREQ

		case AUTHREQ:
			msg, ok := <-backend.R
			if !ok {
				return checkErr()
			}

			if authreq, ok = msg.(*AuthenticationRequest); !ok {
				return ErrProtocolViolation
			}

			switch authreq.Type {
			case OK:
				frontend.W <- msg
				state = AUTHENTICATED
			case CleartextPassword:
				frontend.W <- msg
				state = PASSWORD
			case MD5Password:
				authreq.Type = CleartextPassword
				frontend.W <- msg
				state = MD5PASSWORD
			default:
				return ErrUnsupportedAuthenticationRequest
			}

		case PASSWORD:
			msg, ok := <-frontend.R
			if !ok {
				return checkErr()
			}

			if password, ok := msg.(*PasswordMessage); !ok {
				return ErrProtocolViolation
			} else {
				backend.W <- password
			}

			state = AUTHENTICATED

		case MD5PASSWORD:
			msg, ok := <-frontend.R
			if !ok {
				return checkErr()
			}

			passMsg, ok := msg.(*PasswordMessage)
			if !ok {
				return ErrProtocolViolation
			}

			// At this point, password is clear, but the server is expecting MD5.
			// concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
			passMsg.SetPassword(saltPassword([]byte(startMsg.(*Startup).User()),
				passMsg.Password(), authreq.Salt()))

			backend.W <- passMsg
			state = AUTHENTICATED

		case AUTHENTICATED:
			msg, ok := <-backend.R
			if !ok {
				return checkErr()
			}

			switch msg.(type) {
			case *AuthenticationRequest:
				if msg.(*AuthenticationRequest).Type == OK {
					frontend.W <- msg
				} else {
					return ErrProtocolViolation
				}
			case *ErrorResponse:
				if msg.(*ErrorResponse).Code() == "28P01" {
					err = errors.New("invalid password")
				} else {
					err = errors.New("authentication failure")
				}
				frontend.W <- msg
			default:
				return ErrProtocolViolation
			}

			return
		}
	}

	return
}

// md5hex - MD5 sum a byte slice, return it as a byte slice containing a hex-encoded string.
func md5hex(s []byte) []byte {
	var sum [16]byte = md5.Sum(s)

	return []byte(hex.EncodeToString(sum[:]))
}

// saltPassword - Prepare a password for postgres
// http://www.postgresql.org/docs/9.2/static/protocol-flow.html#AEN95360
func saltPassword(username, password, salt []byte) []byte {
	salted := md5hex(append(password, username...))
	salted = md5hex(append(salted, salt...))

	return append([]byte("md5"), salted...)
}

func handleClientConnection(frontendConn net.Conn) {
	// Until client is authenticated - timeout in 60 seconds.
	frontendConn.SetDeadline(time.Now().Add(60 * time.Second))

	// First, we read the startup message.  it's handled different from the others,
	// as it has no tag.
	startMsg, err := handleStartup(frontendConn)
	if err != nil {
		log.Printf("Error handling StartupMessage from a client: %s", err)
		frontendConn.Close()
		return
	}
	frontend := newConnection(frontendConn, frontendMessageBuilder)
	defer frontend.Close()

	backendConn, err := backends.Dial()
	if err != nil {
		log.Printf("Couldn't retrieve a backend: %s", err)
		return
	}

	backend := newConnection(backendConn, backendMessageBuilder)
	defer backend.Close()

	// Fast-path if we were handling a CancelRequest.
	if _, ok := startMsg.(*CancelRequest); ok {
		backend.W <- startMsg
		<-backend.R
		return
	}

	err = handleAuthentication(frontend, backend, startMsg)
	if err == io.EOF {
		// Client closed connection
		return
	} else if err != nil {
		log.Printf("Error in authentication phase: %s", err)
		return
	}

	// Successfuly authenticated.  No more deadline.
	frontendConn.SetDeadline(time.Time{})

	for {
		if err := frontend.Error(); err == io.EOF {
			// Client closed connection.  No retries.
			return
		} else if err != nil {
			log.Printf("frontend error in proxy phase: %s", err)
			return
		}

		if err = backend.Error(); err == io.EOF {
			log.Printf("backend closed connection")
			return
		} else if err != nil {
			log.Printf("backend error in proxy phase: %s", err)
			return
		}

		select {
		case msg, ok := <-frontend.R:
			if ok {
				backend.W <- msg
			}

		case msg, ok := <-backend.R:
			if ok {
				if errorMsg, ok := msg.(*ErrorResponse); ok {
					log.Printf("Error message from backend: %v", errorMsg)
				}
				frontend.W <- msg
			}
		}
	}
}
