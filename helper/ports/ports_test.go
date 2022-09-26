package ports

import (
	"log"
	"testing"
)

func TestGetFreePort(t *testing.T) {
	port, err := GetFreePort()
	if err != nil {
		log.Println(err)
	}
	log.Println(port)
}

func TestGetFreePorts(t *testing.T) {
	port, err := GetFreePorts(3)
	if err != nil {
		log.Println(err)
	}
	log.Println(port)
}