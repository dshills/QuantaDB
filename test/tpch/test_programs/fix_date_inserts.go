//go:build ignore

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: fix_date_inserts <input_file> <output_file>")
		os.Exit(1)
	}

	inputFile := os.Args[1]
	outputFile := os.Args[2]

	// Open input file
	in, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	// Create output file
	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	scanner := bufio.NewScanner(in)
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	// Pattern to match date values in INSERT statements
	datePattern := regexp.MustCompile(`'(\d{4}-\d{2}-\d{2})'`)

	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()

		// Replace date strings with DATE 'yyyy-mm-dd' format
		fixed := datePattern.ReplaceAllString(line, "DATE '$1'")

		fmt.Fprintln(writer, fixed)
		lineCount++

		if lineCount%1000 == 0 {
			fmt.Printf("Processed %d lines\n", lineCount)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Fixed %d lines\n", lineCount)
}
