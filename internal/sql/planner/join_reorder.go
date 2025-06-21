package planner

import (
	"math"
	"sort"

	"github.com/dshills/QuantaDB/internal/catalog"
)

// JoinGraph represents the join relationships between tables.
type JoinGraph struct {
	Tables   []*TableNode
	Joins    []*JoinEdge
	tableMap map[string]*TableNode
}

// TableNode represents a table in the join graph.
type TableNode struct {
	TableName   string
	Alias       string
	Table       *catalog.Table
	BaseRows    float64
	Selectivity float64
}

// JoinEdge represents a join condition between two tables.
type JoinEdge struct {
	Left        *TableNode
	Right       *TableNode
	Condition   Expression
	JoinType    JoinType
	Selectivity float64
}

// Note: JoinType is defined in plan.go

// JoinEnumerator provides different algorithms for join ordering.
type JoinEnumerator interface {
	// FindBestJoinOrder returns the optimal join order for the given tables and conditions.
	FindBestJoinOrder(graph *JoinGraph, costEstimator *CostEstimator) (Plan, Cost, error)
}

// DynamicProgrammingEnumerator uses dynamic programming for optimal join ordering.
type DynamicProgrammingEnumerator struct {
	MaxTables int // Maximum tables for DP (typically 8-10)
}

// NewDynamicProgrammingEnumerator creates a new DP-based join enumerator.
func NewDynamicProgrammingEnumerator() *DynamicProgrammingEnumerator {
	return &DynamicProgrammingEnumerator{
		MaxTables: 8, // PostgreSQL default
	}
}

// FindBestJoinOrder finds the optimal join order using dynamic programming.
func (dp *DynamicProgrammingEnumerator) FindBestJoinOrder(graph *JoinGraph, costEstimator *CostEstimator) (Plan, Cost, error) {
	if len(graph.Tables) > dp.MaxTables {
		// Fall back to greedy algorithm for large numbers of tables
		greedy := NewGreedyEnumerator()
		return greedy.FindBestJoinOrder(graph, costEstimator)
	}

	// Dynamic programming approach
	// memo[tableSet] = (bestPlan, bestCost)
	memo := make(map[uint64]*DPEntry)

	// Base case: single tables
	for i, table := range graph.Tables {
		tableSet := uint64(1) << i
		scan := NewLogicalScan(table.TableName, table.Alias, nil) // Simplified schema
		cost := costEstimator.EstimateTableScanCost(table.Table, 1.0)

		memo[tableSet] = &DPEntry{
			Plan: scan,
			Cost: cost,
		}
	}

	// Build up larger join sets
	for size := 2; size <= len(graph.Tables); size++ {
		dp.enumerateSubsets(graph, costEstimator, memo, size)
	}

	// Return the best plan for all tables
	allTables := (uint64(1) << len(graph.Tables)) - 1
	if entry, exists := memo[allTables]; exists {
		return entry.Plan, entry.Cost, nil
	}

	// Fallback: return a simple nested loop join
	return dp.createFallbackPlan(graph), Cost{}, nil
}

// DPEntry represents an entry in the dynamic programming memo table.
type DPEntry struct {
	Plan Plan
	Cost Cost
}

// enumerateSubsets enumerates all subsets of the given size and finds the best join plan.
func (dp *DynamicProgrammingEnumerator) enumerateSubsets(graph *JoinGraph, costEstimator *CostEstimator, memo map[uint64]*DPEntry, size int) {
	// Generate all subsets of the given size
	subsets := dp.generateSubsets(len(graph.Tables), size)

	for _, subset := range subsets {
		bestCost := Cost{TotalCost: math.Inf(1)}
		var bestPlan Plan

		// Try all possible ways to split this subset into two parts
		for leftSubset := subset; leftSubset > 0; leftSubset = (leftSubset - 1) & subset {
			if leftSubset == subset {
				continue // Skip the subset itself
			}

			rightSubset := subset ^ leftSubset
			if rightSubset == 0 {
				continue
			}

			// Check if we have plans for both subsets
			leftEntry, leftExists := memo[leftSubset]
			rightEntry, rightExists := memo[rightSubset]

			if !leftExists || !rightExists {
				continue
			}

			// Check if these subsets can be joined
			if joinCondition := dp.findJoinCondition(graph, leftSubset, rightSubset); joinCondition != nil {
				// Calculate the cost of joining these two subsets
				joinCost := dp.calculateJoinCost(costEstimator, leftEntry, rightEntry, joinCondition)

				if joinCost.TotalCost < bestCost.TotalCost {
					bestCost = joinCost
					bestPlan = dp.createJoinPlan(leftEntry.Plan, rightEntry.Plan, joinCondition)
				}
			}
		}

		if bestPlan != nil {
			memo[subset] = &DPEntry{
				Plan: bestPlan,
				Cost: bestCost,
			}
		}
	}
}

// generateSubsets generates all subsets of the given size.
func (dp *DynamicProgrammingEnumerator) generateSubsets(n, size int) []uint64 {
	var subsets []uint64

	var generate func(start, remaining int, current uint64)
	generate = func(start, remaining int, current uint64) {
		if remaining == 0 {
			subsets = append(subsets, current)
			return
		}

		for i := start; i <= n-remaining; i++ {
			generate(i+1, remaining-1, current|(1<<i))
		}
	}

	generate(0, size, 0)
	return subsets
}

// findJoinCondition finds a join condition between two table subsets.
func (dp *DynamicProgrammingEnumerator) findJoinCondition(graph *JoinGraph, leftSubset, rightSubset uint64) *JoinEdge {
	for _, edge := range graph.Joins {
		leftIndex := dp.findTableIndex(graph, edge.Left)
		rightIndex := dp.findTableIndex(graph, edge.Right)

		if leftIndex >= 0 && rightIndex >= 0 {
			leftBit := uint64(1) << leftIndex
			rightBit := uint64(1) << rightIndex

			// Check if one table is in left subset and other is in right subset
			if (leftSubset&leftBit != 0 && rightSubset&rightBit != 0) ||
				(leftSubset&rightBit != 0 && rightSubset&leftBit != 0) {
				return edge
			}
		}
	}
	return nil
}

// findTableIndex finds the index of a table in the graph.
func (dp *DynamicProgrammingEnumerator) findTableIndex(graph *JoinGraph, table *TableNode) int {
	for i, t := range graph.Tables {
		if t == table {
			return i
		}
	}
	return -1
}

// calculateJoinCost calculates the cost of joining two plans.
func (dp *DynamicProgrammingEnumerator) calculateJoinCost(_ *CostEstimator, left, right *DPEntry, joinCondition *JoinEdge) Cost {
	// Simplified cost calculation
	// In practice, this would consider different join algorithms

	combinedRows := left.Cost.Rows * right.Cost.Rows * float64(joinCondition.Selectivity)
	if combinedRows == 0 {
		combinedRows = left.Cost.Rows * right.Cost.Rows * 0.1 // Default selectivity
	}

	// Hash join cost estimation
	buildCost := right.Cost.TotalCost // Build phase
	probeCost := left.Cost.TotalCost  // Probe phase

	return Cost{
		StartupCost: left.Cost.StartupCost + right.Cost.StartupCost + buildCost,
		TotalCost:   left.Cost.TotalCost + right.Cost.TotalCost + buildCost + probeCost,
		Rows:        combinedRows,
		Width:       left.Cost.Width + right.Cost.Width,
	}
}

// createJoinPlan creates a join plan from two child plans.
func (dp *DynamicProgrammingEnumerator) createJoinPlan(left, right Plan, joinCondition *JoinEdge) Plan {
	// Convert to LogicalPlan interface
	leftLogical, leftOk := left.(LogicalPlan)
	rightLogical, rightOk := right.(LogicalPlan)

	if !leftOk || !rightOk {
		// Fallback: create a simple join structure
		return &LogicalJoin{
			basePlan: basePlan{
				children: []Plan{left, right},
			},
			JoinType:  joinCondition.JoinType,
			Condition: joinCondition.Condition,
		}
	}

	// Create a logical join node using the constructor
	return NewLogicalJoin(leftLogical, rightLogical, joinCondition.JoinType, joinCondition.Condition, nil)
}

// createFallbackPlan creates a simple nested loop join plan as fallback.
func (dp *DynamicProgrammingEnumerator) createFallbackPlan(graph *JoinGraph) Plan {
	if len(graph.Tables) == 0 {
		return nil
	}

	// Start with the first table
	var plan Plan = NewLogicalScan(graph.Tables[0].TableName, graph.Tables[0].Alias, nil)

	// Add joins for remaining tables
	for i := 1; i < len(graph.Tables); i++ {
		rightScan := NewLogicalScan(graph.Tables[i].TableName, graph.Tables[i].Alias, nil)

		// Find a join condition
		var condition Expression
		for _, edge := range graph.Joins {
			if (edge.Left == graph.Tables[i-1] && edge.Right == graph.Tables[i]) ||
				(edge.Left == graph.Tables[i] && edge.Right == graph.Tables[i-1]) {
				condition = edge.Condition
				break
			}
		}

		// Create join using the constructor
		if planLogical, ok := plan.(LogicalPlan); ok {
			join := NewLogicalJoin(planLogical, rightScan, InnerJoin, condition, nil)
			plan = join
		}
	}

	return plan
}

// GreedyEnumerator uses a greedy algorithm for join ordering.
type GreedyEnumerator struct{}

// NewGreedyEnumerator creates a new greedy join enumerator.
func NewGreedyEnumerator() *GreedyEnumerator {
	return &GreedyEnumerator{}
}

// FindBestJoinOrder finds a good join order using a greedy algorithm.
func (g *GreedyEnumerator) FindBestJoinOrder(graph *JoinGraph, costEstimator *CostEstimator) (Plan, Cost, error) {
	if len(graph.Tables) <= 1 {
		if len(graph.Tables) == 1 {
			scan := NewLogicalScan(graph.Tables[0].TableName, graph.Tables[0].Alias, nil)
			cost := costEstimator.EstimateTableScanCost(graph.Tables[0].Table, 1.0)
			return scan, cost, nil
		}
		return nil, Cost{}, nil
	}

	// Start with the table with the highest selectivity (smallest after filters)
	remaining := make([]*TableNode, len(graph.Tables))
	copy(remaining, graph.Tables)

	// Sort by estimated size after local predicates
	sort.Slice(remaining, func(i, j int) bool {
		sizeI := remaining[i].BaseRows * remaining[i].Selectivity
		sizeJ := remaining[j].BaseRows * remaining[j].Selectivity
		return sizeI < sizeJ
	})

	// Start with the smallest table
	var plan Plan = NewLogicalScan(remaining[0].TableName, remaining[0].Alias, nil)
	totalCost := costEstimator.EstimateTableScanCost(remaining[0].Table, remaining[0].Selectivity)

	used := map[*TableNode]bool{remaining[0]: true}

	// Greedily add tables
	for len(used) < len(graph.Tables) {
		bestTable, bestJoin, bestCost := g.findBestNextTable(graph, costEstimator, used, totalCost)

		if bestTable == nil {
			break // No more joinable tables
		}

		// Create join
		rightScan := NewLogicalScan(bestTable.TableName, bestTable.Alias, nil)
		if planLogical, ok := plan.(LogicalPlan); ok {
			join := NewLogicalJoin(planLogical, rightScan, InnerJoin, bestJoin.Condition, nil)
			plan = join
		}
		totalCost = bestCost
		used[bestTable] = true
	}

	return plan, totalCost, nil
}

// findBestNextTable finds the best table to join next in the greedy algorithm.
func (g *GreedyEnumerator) findBestNextTable(graph *JoinGraph, costEstimator *CostEstimator, used map[*TableNode]bool, currentCost Cost) (*TableNode, *JoinEdge, Cost) {
	var bestTable *TableNode
	var bestJoin *JoinEdge
	bestCost := Cost{TotalCost: math.Inf(1)}

	for _, table := range graph.Tables {
		if used[table] {
			continue
		}

		// Find a join condition with any used table
		for _, edge := range graph.Joins {
			var joinable bool
			if used[edge.Left] && edge.Right == table {
				joinable = true
			} else if used[edge.Right] && edge.Left == table {
				joinable = true
			}

			if joinable {
				// Estimate the cost of adding this table
				tableCost := costEstimator.EstimateTableScanCost(table.Table, table.Selectivity)

				// Simplified join cost
				joinCost := Cost{
					StartupCost: currentCost.StartupCost + tableCost.StartupCost,
					TotalCost:   currentCost.TotalCost + tableCost.TotalCost + (currentCost.Rows * tableCost.Rows * float64(edge.Selectivity)),
					Rows:        currentCost.Rows * tableCost.Rows * float64(edge.Selectivity),
					Width:       currentCost.Width + tableCost.Width,
				}

				if joinCost.TotalCost < bestCost.TotalCost {
					bestTable = table
					bestJoin = edge
					bestCost = joinCost
				}
			}
		}
	}

	return bestTable, bestJoin, bestCost
}

// BuildJoinGraph builds a join graph from a query's FROM and WHERE clauses.
func BuildJoinGraph(tables []string, joins []Expression, cat catalog.Catalog, costEstimator *CostEstimator) (*JoinGraph, error) {
	graph := &JoinGraph{
		Tables:   make([]*TableNode, 0, len(tables)),
		Joins:    make([]*JoinEdge, 0),
		tableMap: make(map[string]*TableNode),
	}

	// Add tables to the graph
	for _, tableName := range tables {
		// Try to get table metadata
		var table *catalog.Table
		var err error

		// Try different schemas
		for _, schema := range []string{"public", "test", ""} {
			table, err = cat.GetTable(schema, tableName)
			if err == nil {
				break
			}
		}

		if table == nil {
			continue // Skip tables we can't find
		}

		// Get table statistics for cardinality estimation
		baseRows := float64(1000) // Default
		if stats, err := cat.GetTableStats(table.SchemaName, table.TableName); err == nil && stats != nil {
			baseRows = float64(stats.RowCount)
		}

		node := &TableNode{
			TableName:   tableName,
			Table:       table,
			BaseRows:    baseRows,
			Selectivity: 1.0, // Will be updated with local predicates
		}

		graph.Tables = append(graph.Tables, node)
		graph.tableMap[tableName] = node
	}

	// Add join conditions
	for _, joinExpr := range joins {
		if edge := extractJoinEdge(joinExpr, graph, costEstimator); edge != nil {
			graph.Joins = append(graph.Joins, edge)
		}
	}

	return graph, nil
}

// extractJoinEdge extracts a join edge from a join expression.
func extractJoinEdge(expr Expression, graph *JoinGraph, _ *CostEstimator) *JoinEdge {
	// Simplified join edge extraction
	// In practice, this would be more sophisticated

	if binOp, ok := expr.(*BinaryOp); ok && binOp.Operator == OpEqual {
		// Look for column references from different tables
		leftCol, leftOk := binOp.Left.(*ColumnRef)
		rightCol, rightOk := binOp.Right.(*ColumnRef)

		if leftOk && rightOk {
			// Find the tables these columns belong to
			var leftTable, rightTable *TableNode

			for _, table := range graph.Tables {
				for _, col := range table.Table.Columns {
					if col.Name == leftCol.ColumnName {
						leftTable = table
					}
					if col.Name == rightCol.ColumnName {
						rightTable = table
					}
				}
			}

			if leftTable != nil && rightTable != nil && leftTable != rightTable {
				return &JoinEdge{
					Left:        leftTable,
					Right:       rightTable,
					Condition:   expr,
					JoinType:    InnerJoin,
					Selectivity: 0.1, // Default join selectivity
				}
			}
		}
	}

	return nil
}
