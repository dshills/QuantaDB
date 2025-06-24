package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	outputDir := "./data"
	scaleFactor := 0.01

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Regenerating TPC-H data at scale factor %.2f with DATE literals...\n", scaleFactor)
	generator := NewGenerator(scaleFactor)

	// Generate orders with DATE literals
	fmt.Println("Generating orders data...")
	orders := generator.GenerateOrders()
	orderFile := filepath.Join(outputDir, "orders.sql")
	if err := writeFile(orderFile, orders); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write orders: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  Wrote %d rows to %s\n", len(orders), orderFile)

	// Generate lineitem with DATE literals
	fmt.Println("Generating lineitem data...")
	lineitemFile := filepath.Join(outputDir, "lineitem.sql")
	if err := generateLineitemData(generator, scaleFactor, lineitemFile); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate lineitem data: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nData regeneration complete!")
}

type Generator struct {
	scaleFactor float64
	rng         *rand.Rand
}

func NewGenerator(scaleFactor float64) *Generator {
	return &Generator{
		scaleFactor: scaleFactor,
		rng:         rand.New(rand.NewSource(42)),
	}
}

func (g *Generator) GenerateOrders() []string {
	orderStatuses := []string{"F", "O", "P"}
	orderPriorities := []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}

	orderCount := int(1500000 * g.scaleFactor)
	inserts := make([]string, 0, orderCount)

	for i := 1; i <= orderCount; i++ {
		custKey := g.rng.Intn(int(150000*g.scaleFactor)) + 1
		orderStatus := orderStatuses[g.rng.Intn(len(orderStatuses))]
		orderDate := time.Date(1992+g.rng.Intn(7), time.Month(1+g.rng.Intn(12)), 1+g.rng.Intn(28), 0, 0, 0, 0, time.UTC)
		totalPrice := g.randomDecimal(1000.0, 500000.0)
		orderPriority := orderPriorities[g.rng.Intn(len(orderPriorities))]
		clerk := fmt.Sprintf("Clerk#%09d", g.rng.Intn(1000)+1)
		shipPriority := 0
		comment := g.randomText(19, 78)

		insert := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, '%s', %.2f, DATE '%s', '%s', '%s', %d, '%s');",
			i, custKey, orderStatus, totalPrice, orderDate.Format("2006-01-02"),
			orderPriority, clerk, shipPriority, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

func (g *Generator) GenerateLineitem(orderKey, lineNumber, maxPartKey, maxSuppKey int) string {
	partKey := g.rng.Intn(maxPartKey) + 1
	suppKey := g.rng.Intn(maxSuppKey) + 1
	quantity := g.randomDecimal(1, 50)
	extendedPrice := quantity * g.randomDecimal(900, 1100)
	discount := g.randomDecimal(0, 0.10)
	tax := g.randomDecimal(0, 0.08)

	returnFlag := "N"
	lineStatus := "O"
	if g.rng.Float64() < 0.1 {
		returnFlag = "R"
	}
	if g.rng.Float64() < 0.5 {
		lineStatus = "F"
	}

	baseDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
	shipOffset := g.rng.Intn(2500)
	shipDate := baseDate.AddDate(0, 0, shipOffset)
	commitDate := shipDate.AddDate(0, 0, g.rng.Intn(30))
	receiptDate := shipDate.AddDate(0, 0, g.rng.Intn(7)+1)

	shipInstruct := []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}[g.rng.Intn(4)]
	shipMode := []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}[g.rng.Intn(7)]
	comment := g.randomText(10, 43)

	return fmt.Sprintf("INSERT INTO lineitem VALUES (%d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, '%s', '%s', DATE '%s', DATE '%s', DATE '%s', '%s', '%s', '%s');",
		orderKey, partKey, suppKey, lineNumber, quantity, extendedPrice, discount, tax,
		returnFlag, lineStatus, shipDate.Format("2006-01-02"), commitDate.Format("2006-01-02"),
		receiptDate.Format("2006-01-02"), shipInstruct, shipMode, comment)
}

func (g *Generator) randomText(minLen, maxLen int) string {
	length := minLen + g.rng.Intn(maxLen-minLen+1)
	words := []string{"the", "of", "and", "to", "a", "in", "that", "is", "was", "for", "it", "with", "as", "his", "on", "be", "at", "by", "have", "from"}

	var result []string
	totalLen := 0
	for totalLen < length {
		word := words[g.rng.Intn(len(words))]
		result = append(result, word)
		totalLen += len(word) + 1
	}

	text := strings.Join(result, " ")
	if len(text) > length {
		text = text[:length]
	}
	return text
}

func (g *Generator) randomDecimal(min, max float64) float64 {
	return min + g.rng.Float64()*(max-min)
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

func generateLineitemData(g *Generator, scaleFactor float64, filename string) error {
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
