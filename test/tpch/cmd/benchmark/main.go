package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dshills/QuantaDB/test/tpch"
)

func main() {
	var (
		host        = flag.String("host", "localhost", "Database host")
		port        = flag.Int("port", 5432, "Database port")
		user        = flag.String("user", "postgres", "Database user")
		password    = flag.String("password", "", "Database password")
		dbname      = flag.String("dbname", "quantadb", "Database name")
		scaleFactor = flag.Float64("sf", 0.01, "Scale factor for data")
		warmup      = flag.Bool("warmup", true, "Run warmup queries")
		reportFile  = flag.String("report", "", "Output report file (default: stdout)")
	)
	flag.Parse()

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
		*host, *port, *user, *dbname)
	if *password != "" {
		connStr += fmt.Sprintf(" password=%s", *password)
	}

	// Create benchmark suite
	fmt.Printf("Connecting to %s:%d/%s...\n", *host, *port, *dbname)
	suite, err := tpch.NewBenchmarkSuite(connStr, *scaleFactor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create benchmark suite: %v\n", err)
		os.Exit(1)
	}
	defer suite.Close()

	fmt.Println("Connected successfully!")

	// Run warmup if requested
	if *warmup {
		if err := suite.WarmUp(); err != nil {
			fmt.Fprintf(os.Stderr, "Warmup failed: %v\n", err)
		}
	}

	// Run benchmarks
	fmt.Println("\nRunning TPC-H benchmarks...")
	results := suite.RunAll()

	// Generate report
	report := tpch.GenerateReport(results, *scaleFactor)

	// Output report
	if *reportFile != "" {
		if err := os.WriteFile(*reportFile, []byte(report), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nReport written to %s\n", *reportFile)
	} else {
		fmt.Printf("\n%s\n", report)
	}

	// Exit with error if any queries failed
	for _, r := range results {
		if r.Error != nil {
			os.Exit(1)
		}
	}
}
