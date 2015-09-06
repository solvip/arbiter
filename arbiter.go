package main

import (
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

type connectionHandler func(net.Conn)

type server struct {
	monitor *BackendsMonitor
}

func main() {
	pprof := flag.Bool("p", false, "Enable pprof, listening on localhost:6060")
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

	if *pprof {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	go func() {
		log.Printf("Starting follower handler; listening on %s", c.Main.Follower)
		if err := startHandler(c.Main.Follower, s.followerConnectionHandler); err != nil {
			log.Fatalf("Could not start Arbiter: %s", err)
		}
	}()

	log.Printf("Starting primary handler; listening on %s", c.Main.Primary)
	if err := startHandler(c.Main.Primary, s.primaryConnectionHandler); err != nil {
		log.Fatalf("Could not start Arbiter: %s", err)
	}

	return
}

func startHandler(addr string, handler connectionHandler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting client: %s", err)
			continue
		}

		go handler(conn)
	}
}

func (s *server) primaryConnectionHandler(clientConn net.Conn) {
	backendConn, err := s.monitor.DialTimeout(PRIMARY, 5*time.Second)
	if err != nil {
		log.Printf("Couldn't retrieve a backend: %s", err)
		clientConn.Close()
		return
	}

	proxy(clientConn, backendConn)
	clientConn.Close()
	backendConn.Close()
}

func (s *server) followerConnectionHandler(clientConn net.Conn) {
	backendConn, err := s.monitor.DialTimeout(PRIMARY, 5*time.Second)
	if err != nil {
		log.Printf("Couldn't retrieve a backend: %s", err)
		clientConn.Close()
		return
	}

	proxy(clientConn, backendConn)
	clientConn.Close()
	backendConn.Close()
}

// Proxy a <-> b until EOF.
func proxy(a net.Conn, b net.Conn) {
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		io.Copy(a, b)
		wg.Done()
	}()

	go func() {
		io.Copy(b, a)
		wg.Done()
	}()

	wg.Wait()
}
