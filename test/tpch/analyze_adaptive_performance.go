package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/lib/pq"
)

// QueryBenchmark represents a single query benchmark
type QueryBenchmark struct {
	Name        string
	Query       string
	Description string
}

// BenchmarkResult holds the results of running a query
type BenchmarkResult struct {
	QueryName     string
	Configuration string
	ExecutionTime time.Duration
	RowsReturned  int
	Success       bool
	Error         string
}

// PerformanceComparison holds comparison data
type PerformanceComparison struct {
	QueryName     string
	BaselineTime  time.Duration
	AdaptiveTime  time.Duration
	Improvement   float64
	SpeedupFactor float64
}

var tpchQueries = []QueryBenchmark{
	{
		Name:        "Q1",
		Description: "Pricing Summary Report - Heavy aggregation",
		Query: `SELECT
			l_returnflag,
			l_linestatus,
			SUM(l_quantity) as sum_qty,
			SUM(l_extendedprice) as sum_base_price,
			SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
			SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
			AVG(l_quantity) as avg_qty,
			AVG(l_extendedprice) as avg_price,
			AVG(l_discount) as avg_disc,
			COUNT(*) as count_order
		FROM
			lineitem
		WHERE
			l_shipdate <= date '1998-12-01' - interval '90' day
		GROUP BY
			l_returnflag,
			l_linestatus
		ORDER BY
			l_returnflag,
			l_linestatus`,
	},
	{
		Name:        "Q3",
		Description: "Shipping Priority - Join with aggregation",
		Query: `SELECT
			l_orderkey,
			SUM(l_extendedprice * (1 - l_discount)) as revenue,
			o_orderdate,
			o_shippriority
		FROM
			customer,
			orders,
			lineitem
		WHERE
			c_mktsegment = 'BUILDING'
			AND c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate < date '1995-03-15'
			AND l_shipdate > date '1995-03-15'
		GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
		ORDER BY
			revenue DESC,
			o_orderdate
		LIMIT 10`,
	},
	{
		Name:        "Q6",
		Description: "Forecasting Revenue Change - Simple filter and aggregation",
		Query: `SELECT
			SUM(l_extendedprice * l_discount) as revenue
		FROM
			lineitem
		WHERE
			l_shipdate >= date '1994-01-01'
			AND l_shipdate < date '1994-01-01' + interval '1' year
			AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
			AND l_quantity < 24`,
	},
	{
		Name:        "Q10",
		Description: "Returned Item Reporting - Complex join with filter",
		Query: `SELECT
			c_custkey,
			c_name,
			SUM(l_extendedprice * (1 - l_discount)) as revenue,
			c_acctbal,
			n_name,
			c_address,
			c_phone,
			c_comment
		FROM
			customer,
			orders,
			lineitem,
			nation
		WHERE
			c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate >= date '1993-10-01'
			AND o_orderdate < date '1993-10-01' + interval '3' month
			AND l_returnflag = 'R'
			AND c_nationkey = n_nationkey
		GROUP BY
			c_custkey,
			c_name,
			c_acctbal,
			c_phone,
			n_name,
			c_address,
			c_comment
		ORDER BY
			revenue DESC
		LIMIT 20`,
	},
	{
		Name:        "Q12",
		Description: "Shipping Modes - CASE expressions with joins",
		Query: `SELECT
			l_shipmode,
			SUM(CASE
				WHEN o_orderpriority = '1-URGENT'
					OR o_orderpriority = '2-HIGH'
					THEN 1
				ELSE 0
			END) as high_line_count,
			SUM(CASE
				WHEN o_orderpriority <> '1-URGENT'
					AND o_orderpriority <> '2-HIGH'
					THEN 1
				ELSE 0
			END) as low_line_count
		FROM
			orders,
			lineitem
		WHERE
			o_orderkey = l_orderkey
			AND l_shipmode IN ('MAIL', 'SHIP')
			AND l_commitdate < l_receiptdate
			AND l_shipdate < l_commitdate
			AND l_receiptdate >= date '1994-01-01'
			AND l_receiptdate < date '1994-01-01' + interval '1' year
		GROUP BY
			l_shipmode
		ORDER BY
			l_shipmode`,
	},
	{
		Name:        "Q14",
		Description: "Promotion Effect - LIKE with aggregation",
		Query: `SELECT
			100.00 * SUM(CASE
				WHEN p_type LIKE 'PROMO%'
					THEN l_extendedprice * (1 - l_discount)
				ELSE 0
			END) / SUM(l_extendedprice * (1 - l_discount)) as promo_revenue
		FROM
			lineitem,
			part
		WHERE
			l_partkey = p_partkey
			AND l_shipdate >= date '1995-09-01'
			AND l_shipdate < date '1995-09-01' + interval '1' month`,
	},
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run analyze_adaptive_performance.go <baseline_dsn> [adaptive_dsn]")
		fmt.Println("Example: go run analyze_adaptive_performance.go 'host=localhost port=5432 ...' 'host=localhost port=5433 ...'")
		os.Exit(1)
	}

	baselineDSN := os.Args[1]
	adaptiveDSN := baselineDSN
	if len(os.Args) > 2 {
		adaptiveDSN = os.Args[2]
	}

	fmt.Println("=== QuantaDB Adaptive Execution Performance Analysis ===")
	fmt.Println()
	fmt.Printf("Baseline DSN: %s\n", maskPassword(baselineDSN))
	fmt.Printf("Adaptive DSN: %s\n", maskPassword(adaptiveDSN))
	fmt.Println()

	// Run benchmarks
	baselineResults := runBenchmarks("Baseline", baselineDSN)
	adaptiveResults := runBenchmarks("Adaptive", adaptiveDSN)

	// Compare results
	comparisons := compareResults(baselineResults, adaptiveResults)

	// Print results
	printResults(baselineResults, adaptiveResults, comparisons)

	// Print summary
	printSummary(comparisons)
}

func runBenchmarks(name string, dsn string) map[string]*BenchmarkResult {
	fmt.Printf("Running %s benchmarks...\n", name)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to %s database: %v", name, err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping %s database: %v", name, err)
	}

	results := make(map[string]*BenchmarkResult)

	for _, query := range tpchQueries {
		fmt.Printf("  Running %s (%s)...", query.Name, query.Description)

		result := &BenchmarkResult{
			QueryName:     query.Name,
			Configuration: name,
		}

		// Warm up run
		_, _ = executeQuery(db, query.Query)

		// Measure 3 runs and take average
		var totalTime time.Duration
		var rowCount int
		runs := 3

		for i := 0; i < runs; i++ {
			start := time.Now()
			rows, err := executeQuery(db, query.Query)
			elapsed := time.Since(start)

			if err != nil {
				result.Success = false
				result.Error = err.Error()
				break
			}

			totalTime += elapsed
			rowCount = rows
			result.Success = true
		}

		if result.Success {
			result.ExecutionTime = totalTime / time.Duration(runs)
			result.RowsReturned = rowCount
			fmt.Printf(" %v (%d rows)\n", result.ExecutionTime, rowCount)
		} else {
			fmt.Printf(" FAILED: %s\n", result.Error)
		}

		results[query.Name] = result
	}

	fmt.Println()
	return results
}

func executeQuery(db *sql.DB, query string) (int, error) {
	rows, err := db.Query(query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		// Just consume the row without processing
	}

	return count, rows.Err()
}

func compareResults(baseline, adaptive map[string]*BenchmarkResult) []PerformanceComparison {
	var comparisons []PerformanceComparison

	for queryName, baseResult := range baseline {
		adaptResult, exists := adaptive[queryName]
		if !exists || !baseResult.Success || !adaptResult.Success {
			continue
		}

		improvement := float64(baseResult.ExecutionTime-adaptResult.ExecutionTime) / float64(baseResult.ExecutionTime) * 100
		speedup := float64(baseResult.ExecutionTime) / float64(adaptResult.ExecutionTime)

		comparisons = append(comparisons, PerformanceComparison{
			QueryName:     queryName,
			BaselineTime:  baseResult.ExecutionTime,
			AdaptiveTime:  adaptResult.ExecutionTime,
			Improvement:   improvement,
			SpeedupFactor: speedup,
		})
	}

	return comparisons
}

func printResults(baseline, adaptive map[string]*BenchmarkResult, comparisons []PerformanceComparison) {
	fmt.Println("=== Performance Results ===")
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query\tBaseline\tAdaptive\tImprovement\tSpeedup\tStatus")
	fmt.Fprintln(w, "-----\t--------\t--------\t-----------\t-------\t------")

	for _, comp := range comparisons {
		status := ""
		if comp.Improvement > 20 {
			status = "✓ Excellent"
		} else if comp.Improvement > 10 {
			status = "✓ Good"
		} else if comp.Improvement > 0 {
			status = "✓ Better"
		} else if comp.Improvement > -5 {
			status = "~ Same"
		} else {
			status = "✗ Slower"
		}

		fmt.Fprintf(w, "%s\t%v\t%v\t%.1f%%\t%.2fx\t%s\n",
			comp.QueryName,
			comp.BaselineTime,
			comp.AdaptiveTime,
			comp.Improvement,
			comp.SpeedupFactor,
			status)
	}

	w.Flush()
	fmt.Println()
}

func printSummary(comparisons []PerformanceComparison) {
	if len(comparisons) == 0 {
		fmt.Println("No successful comparisons available")
		return
	}

	fmt.Println("=== Performance Summary ===")
	fmt.Println()

	var totalImprovement float64
	var bestImprovement float64
	var worstImprovement float64 = 100
	var bestQuery, worstQuery string

	for _, comp := range comparisons {
		totalImprovement += comp.Improvement

		if comp.Improvement > bestImprovement {
			bestImprovement = comp.Improvement
			bestQuery = comp.QueryName
		}

		if comp.Improvement < worstImprovement {
			worstImprovement = comp.Improvement
			worstQuery = comp.QueryName
		}
	}

	avgImprovement := totalImprovement / float64(len(comparisons))

	fmt.Printf("Average Improvement: %.1f%%\n", avgImprovement)
	fmt.Printf("Best Improvement:    %.1f%% (%s)\n", bestImprovement, bestQuery)
	fmt.Printf("Worst Improvement:   %.1f%% (%s)\n", worstImprovement, worstQuery)
	fmt.Println()

	// Categorize results
	excellent := 0
	good := 0
	better := 0
	same := 0
	slower := 0

	for _, comp := range comparisons {
		switch {
		case comp.Improvement > 20:
			excellent++
		case comp.Improvement > 10:
			good++
		case comp.Improvement > 0:
			better++
		case comp.Improvement > -5:
			same++
		default:
			slower++
		}
	}

	fmt.Println("Performance Distribution:")
	fmt.Printf("  Excellent (>20%%):  %d queries\n", excellent)
	fmt.Printf("  Good (10-20%%):     %d queries\n", good)
	fmt.Printf("  Better (0-10%%):    %d queries\n", better)
	fmt.Printf("  Same (-5-0%%):      %d queries\n", same)
	fmt.Printf("  Slower (<-5%%):     %d queries\n", slower)
	fmt.Println()

	// Overall assessment
	if avgImprovement > 15 {
		fmt.Println("✓ EXCELLENT: Adaptive execution provides significant performance gains!")
	} else if avgImprovement > 10 {
		fmt.Println("✓ GOOD: Adaptive execution shows meaningful performance improvements.")
	} else if avgImprovement > 5 {
		fmt.Println("✓ POSITIVE: Adaptive execution provides modest performance benefits.")
	} else if avgImprovement > 0 {
		fmt.Println("~ NEUTRAL: Adaptive execution shows marginal improvements.")
	} else {
		fmt.Println("✗ NEGATIVE: Adaptive execution may need tuning for this workload.")
	}
}

func maskPassword(dsn string) string {
	parts := strings.Split(dsn, " ")
	for i, part := range parts {
		if strings.HasPrefix(part, "password=") {
			parts[i] = "password=****"
		}
	}
	return strings.Join(parts, " ")
}
