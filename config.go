package main

import (
	"fmt"
	"gopkg.in/gcfg.v1"
	"net"
	"strings"
)

type ConfigError interface {
	error
}

func newConfigError(format string, args ...interface{}) ConfigError {
	return fmt.Errorf(format, args...)
}

type Config struct {
	Main struct {
		Primary  string
		Follower string
		Backends []string
	}

	Health struct {
		Username string
		Password string
		Database string
	}
}

func ConfigFromFile(filename string) (c *Config, err error) {
	c = &Config{}

	if err := gcfg.ReadFileInto(c, filename); err != nil {
		return nil, err
	}

	_, _, err = net.SplitHostPort(c.Main.Primary)
	if err != nil {
		return nil, newConfigError("Main.Primary: %s", c.Main.Primary, err)
	}

	_, _, err = net.SplitHostPort(c.Main.Follower)
	if err != nil {
		return nil, newConfigError("Main.Follower: %s", c.Main.Follower, err)
	}

	c.Main.Backends = strings.Split(c.Main.Backends[0], ",")
	if len(c.Main.Backends) < 1 {
		return nil, newConfigError("Main.Backends contains no backend definitions")
	}

	for i := range c.Main.Backends {
		c.Main.Backends[i] = strings.TrimSpace(c.Main.Backends[i])
		_, _, err = net.SplitHostPort(c.Main.Backends[i])
		if err != nil {
			return nil, newConfigError("Invalid backend '%s': %s", c.Main.Backends[i], err)
		}
	}

	if c.Health.Username == "" {
		return nil, newConfigError("No health-check username defined")
	}

	if c.Health.Database == "" {
		return nil, newConfigError("No health-check database defined")
	}

	return c, nil
}
