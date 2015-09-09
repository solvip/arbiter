package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"
)

type connectionHandler func(net.Conn)

type server struct {
	monitor *BackendsMonitor

	// Bytes transferred
	transferred AtomicInt

	// Current number of connections
	nconns AtomicInt
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func main() {
	httpAddr := flag.String("p", "127.0.0.1:6060", "Enable the HTTP status interface")
	cfgPath := flag.String("f", "/etc/arbiter/config.ini",
		"The path to the arbiter configuration file")
	flag.Parse()

	c, err := ConfigFromFile(*cfgPath)
	if err != nil {
		log.Fatalf("Could not load configuration file: %s", err)
	}

	s := &server{
		monitor: NewBackendsMonitor(c.Health.Username, c.Health.Password, c.Health.Database),
	}

	for _, addr := range c.Main.Backends {
		s.monitor.Add(addr)
	}

	go func() {
		log.Printf("Starting HTTP server; listening on %s", *httpAddr)
		http.HandleFunc("/stats", s.handleStats)
		log.Fatal(http.ListenAndServe(*httpAddr, nil))
	}()

	go func() {
		log.Printf("Starting follower listener; listening on %s", c.Main.Follower)
		if err := s.startListener(c.Main.Follower, FOLLOWER); err != nil {
			log.Fatalf("Could not start Arbiter: %s", err)
		}
	}()

	log.Printf("Starting primary listener; listening on %s", c.Main.Primary)
	if err := s.startListener(c.Main.Primary, PRIMARY); err != nil {
		log.Fatalf("Could not start Arbiter: %s", err)
	}

	return
}

func (s *server) handleStats(w http.ResponseWriter, req *http.Request) {
	curStats := struct {
		TransferredBytes    int64 `json:"transferred_bytes"`
		NumberOfConnections int64 `json:"connections"`
	}{
		s.transferred.Get(),
		s.nconns.Get(),
	}

	b, err := json.MarshalIndent(curStats, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), 500)
	} else {
		w.Write(b)
	}
}

func (s *server) startListener(addr string, state State) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting client: %s", err)
			continue
		}

		go func() {
			s.nconns.Add(1)
			backendConn, err := s.monitor.DialTimeout(state, 5*time.Second)
			if err != nil {
				log.Printf("Couldn't retrieve a backend: %s", err)
				clientConn.Close()
				return
			}

			backendErr := s.proxy(clientConn, backendConn)
			if backendErr != io.EOF {
				log.Printf("Error wrting to backend: %v", backendErr)
			}
			backendConn.Close()
			clientConn.Close()
			s.nconns.Add(-1)
		}()
	}
}

// Proxy frontend <-> backend.
// err will be the first error encountered reading from- or writing to backend.
func (s *server) proxy(frontend, backend io.ReadWriter) (err error) {
	errch := make(chan error)

	// Proxy frontend -> backend
	go func() {
		var n int
		var rerr, werr error

		buf := make([]byte, 4096)
		for {
			n, rerr = frontend.Read(buf)
			s.transferred.Add(int64(n))
			if n > 0 {
				n, werr = backend.Write(buf[0:n])
				if werr != nil {
					errch <- werr
					break
				}
			}

			if rerr != nil {
				break
			}
		}
	}()

	// Proxy backend -> frontend
	go func() {
		var n int
		var rerr, werr error

		buf := make([]byte, 4096)
		for {
			n, rerr = backend.Read(buf)
			s.transferred.Add(int64(n))
			if n > 0 {
				n, werr = frontend.Write(buf[0:n])
				if werr != nil {
					break
				}
			}

			if rerr != nil {
				errch <- rerr
				break
			}
		}
	}()

	err = <-errch

	return err
}
