package main

import (
	"forgequeue/internal/gateway"
)

func main() {
	// Call the execution function inside your internal package
	gateway.Start()
}
