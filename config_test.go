package main

import (
	"os"
	"testing"
)

func TestConfigFromFile(t *testing.T) {
	c, err := ConfigFromFile("non_existing_file")
	if _, ok := err.(*os.PathError); !ok {
		t.Errorf("Expected ConfigFromFile to return os.PathError in the case of a non-existing configuration file; instead got %T", err)
	}

	if c != nil {
		t.Errorf("Expected ConfigFromFile to return nil config in error cases; instead got %v", c)
	}

	_, err = ConfigFromFile("./config.ini")
	if err != nil {
		t.Errorf("Expected config.ini to be successfully parsed; instead got %v", err)
	}
}
