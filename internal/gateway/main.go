package main

import (
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	} else {
		log.Println("gateway network online on port :50051")
	}

}
