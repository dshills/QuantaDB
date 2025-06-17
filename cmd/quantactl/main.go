package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: quantactl [command] [options]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  status    Check cluster status\n")
		fmt.Fprintf(os.Stderr, "  get       Get a value by key\n")
		fmt.Fprintf(os.Stderr, "  put       Store a key-value pair\n")
		fmt.Fprintf(os.Stderr, "  delete    Delete a key\n")
		fmt.Fprintf(os.Stderr, "\n")
		flag.PrintDefaults()
	}
	
	flag.Parse()
	
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	
	command := flag.Arg(0)
	
	switch command {
	case "status":
		fmt.Println("Cluster status: Not implemented")
	case "get", "put", "delete":
		fmt.Printf("%s command: Not implemented\n", command)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		flag.Usage()
		os.Exit(1)
	}
}