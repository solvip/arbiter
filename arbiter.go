package main

import (
	"bufio"
	"code.google.com/p/gcfg"
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
	wg      sync.WaitGroup
	conn    net.Conn
	R       chan Message // Only read from this channel
	W       chan Message // Only write to this channel
	cw      chan bool    // to close W
	err     error
	emu     sync.RWMutex
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
	err = c.conn.Close()
	c.cw <- true // close W
	c.wg.Wait()
	return err
}

func NewConnection(conn net.Conn, builder MessageBuilder) *connection {
	c := new(connection)
	c.conn = conn
	c.builder = builder

	// 32 is just some arbitary number somewhat larger than 0.
	c.W = make(chan Message, 32)
	c.R = make(chan Message, 32)
	c.cw = make(chan bool)

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
				close(c.cw)
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

func handleClientConnection(frontendConn net.Conn) {
	// Until client is authenticated - timeout in 60 seconds.
	frontendConn.SetDeadline(time.Now().Add(60 * time.Second))

	// First, we read the startup message.  it's handled different from the others,
	// as it has no tag.
	// XXX:  We also need to be able to handle CancelRequest at this point
	startMsg := new(StartupMessage)
	var err error
	for i := 0; i < 2; i++ {
		if err = startMsg.DecodeFrom(frontendConn); err != nil {
			log.Printf("Couldn't read the startup message from frontend: %s", err)
			return
		}

		switch {
		case startMsg.MajorVersion() == 1234 && startMsg.MinorVersion() == 5679:
			/* SSL handshaking would go here */
			if _, err = frontendConn.Write([]byte{byte('N')}); err != nil {
				log.Printf("Error writing to socket: %s", err)
				return
			}

			continue

		case startMsg.MajorVersion() == 3 && startMsg.MinorVersion() == 0:
			// Supported protocol
			break

		default:
			/* XXX:  Should return an error to client */
			log.Printf("Unsupported protocol version %d %d",
				startMsg.MajorVersion(), startMsg.MinorVersion())
			return
		}

		break
	}

	backendConn, err := backends.DialPrimary()
	if err != nil {
		log.Printf("Couldn't retrieve a backend: %s", err)
		frontendConn.Close()
		return
	}

	frontend := NewConnection(frontendConn, frontendMessageBuilder)
	defer frontend.Close()

	backend := NewConnection(backendConn, backendMessageBuilder)
	defer backend.Close()

	// Send the startmsg to the backend
	backend.W <- startMsg

	// Start the authentication step.
	authenticated := false
	for !authenticated {
		if err := frontend.Error(); err == io.EOF {
			log.Printf("unauthenticated client closed connection")
			return
		} else if err != nil {
			log.Printf("frontend error in authentication step: %s", err)
			return
		}

		if err = backend.Error(); err == io.EOF {
			log.Printf("backend closed connection")
			return
		} else if err != nil {
			log.Printf("backend error in authentication step: %s", err)
			return
		}

		select {
		case msg, ok := <-backend.R:
			if !ok {
				continue
			}

			switch msg.(type) {
			case *ErrorResponse:
				frontend.W <- msg
			case *AuthenticationRequest:
				frontend.W <- msg
				if msg.(*AuthenticationRequest).Type == OK {
					authenticated = true
					break
				}
			}

		case msg, ok := <-frontend.R:
			if !ok {
				continue
			}

			switch msg.(type) {
			case *PasswordMessage:
				backend.W <- msg
			}
		}
	}

	// Auth done.  The client can stick around for as long as it wants.
	var t time.Time // XXX:  No other way to find zero value of time?
	frontendConn.SetDeadline(t)

	// Proxy!
	for {
		if err := frontend.Error(); err == io.EOF {
			log.Printf("authenticated client closed connection")
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
				frontend.W <- msg
			}
		}
	}
}
