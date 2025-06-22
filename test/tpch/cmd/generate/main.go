package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dshills/QuantaDB/test/tpch"
)

func main() {
	var (
		scaleFactor = flag.Float64("sf", 0.01, "Scale factor (0.01 = 1%, 0.1 = 10%, 1.0 = 100%)")
		outputDir   = flag.String("output", "./data", "Output directory for SQL files")
		combined    = flag.Bool("combined", false, "Generate single combined file")
	)
	flag.Parse()

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generating TPC-H data at scale factor %.2f...\n", *scaleFactor)
	generator := tpch.NewGenerator(*scaleFactor)

	// Generate data for each table
	tables := []struct {
		name     string
		generate func() []string
	}{
		{"region", generator.GenerateRegions},
		{"nation", generator.GenerateNations},
		{"supplier", generator.GenerateSuppliers},
		{"customer", generator.GenerateCustomers},
		{"part", generator.GenerateParts},
		{"orders", generator.GenerateOrders},
	}

	var allInserts []string

	for _, table := range tables {
		fmt.Printf("Generating %s data...\n", table.name)
		inserts := table.generate()
		allInserts = append(allInserts, inserts...)

		if !*combined {
			// Write individual file
			filename := filepath.Join(*outputDir, fmt.Sprintf("%s.sql", table.name))
			if err := writeFile(filename, inserts); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write %s: %v\n", filename, err)
				os.Exit(1)
			}
			fmt.Printf("  Wrote %d rows to %s\n", len(inserts), filename)
		}
	}

	// Generate lineitem data (largest table, done separately for memory efficiency)
	fmt.Printf("Generating lineitem data...\n")
	lineitemFile := filepath.Join(*outputDir, "lineitem.sql")
	if err := generateLineitemData(generator, *scaleFactor, lineitemFile, *combined); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate lineitem data: %v\n", err)
		os.Exit(1)
	}

	// Generate partsupp data
	fmt.Printf("Generating partsupp data...\n")
	partsuppFile := filepath.Join(*outputDir, "partsupp.sql")
	if err := generatePartsuppData(generator, *scaleFactor, partsuppFile, *combined); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate partsupp data: %v\n", err)
		os.Exit(1)
	}

	if *combined {
		// Write combined file
		filename := filepath.Join(*outputDir, "tpch_data.sql")
		if err := writeFile(filename, allInserts); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write combined file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nWrote all data to %s\n", filename)
	}

	fmt.Println("\nData generation complete!")
}

func writeFile(filename string, lines []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := fmt.Fprintln(file, line); err != nil {
			return err
		}
	}
	return nil
}

func generateLineitemData(g *tpch.Generator, scaleFactor float64, filename string, skipFile bool) error {
	if skipFile {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	orderCount := int(1500000 * scaleFactor)
	totalRows := 0

	// Generate 1-7 line items per order
	for orderKey := 1; orderKey <= orderCount; orderKey++ {
		numLines := 1 + (orderKey % 7)
		for lineNum := 1; lineNum <= numLines; lineNum++ {
			insert := g.GenerateLineitem(orderKey, lineNum, int(200000*scaleFactor), int(10000*scaleFactor))
			if _, err := fmt.Fprintln(file, insert); err != nil {
				return err
			}
			totalRows++
		}
	}

	fmt.Printf("  Wrote %d rows to %s\n", totalRows, filename)
	return nil
}

func generatePartsuppData(g *tpch.Generator, scaleFactor float64, filename string, skipFile bool) error {
	if skipFile {
		return nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	partCount := int(200000 * scaleFactor)
	suppCount := int(10000 * scaleFactor)
	totalRows := 0

	// Each part has 4 suppliers
	for partKey := 1; partKey <= partCount; partKey++ {
		for i := 0; i < 4; i++ {
			suppKey := ((partKey + i*2) % suppCount) + 1
			insert := g.GeneratePartsupp(partKey, suppKey)
			if _, err := fmt.Fprintln(file, insert); err != nil {
				return err
			}
			totalRows++
		}
	}

	fmt.Printf("  Wrote %d rows to %s\n", totalRows, filename)
	return nil
}
