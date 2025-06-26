package executor

import (
	"fmt"
	"io"

	"github.com/dshills/QuantaDB/internal/catalog"
	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

// VacuumOperator implements the VACUUM SQL command
type VacuumOperator struct {
	baseOperator
	table          *catalog.Table // Optional: specific table to vacuum
	storage        *MVCCStorageBackend
	vacuumExecutor *VacuumExecutor
	stats          *VacuumStats
	executed       bool
}

// NewVacuumOperator creates a new VACUUM operator
func NewVacuumOperator(storage *MVCCStorageBackend, table *catalog.Table) *VacuumOperator {
	// Create schema for vacuum results
	schema := &Schema{
		Columns: []Column{
			{Name: "operation", Type: types.Text, Nullable: false},
			{Name: "tables_processed", Type: types.Integer, Nullable: false},
			{Name: "versions_scanned", Type: types.Integer, Nullable: false},
			{Name: "versions_removed", Type: types.Integer, Nullable: false},
			{Name: "space_reclaimed", Type: types.Integer, Nullable: false},
			{Name: "duration_ms", Type: types.Integer, Nullable: false},
		},
	}

	return &VacuumOperator{
		baseOperator: baseOperator{schema: schema},
		table:        table,
		storage:      storage,
		executed:     false,
	}
}

// Open initializes the vacuum operator
func (op *VacuumOperator) Open(ctx *ExecContext) error {
	if op.executed {
		return fmt.Errorf("vacuum operator already executed")
	}

	op.ctx = ctx

	// Get the horizon tracker from the transaction manager
	if ctx.TxnManager == nil {
		return fmt.Errorf("transaction manager not available")
	}

	horizonTracker := ctx.TxnManager.GetHorizonTracker()
	if horizonTracker == nil {
		return fmt.Errorf("horizon tracker not available")
	}

	// Create vacuum executor
	op.vacuumExecutor = NewVacuumExecutor(op.storage, horizonTracker)

	// Execute vacuum operation
	var err error
	if op.table != nil {
		// Vacuum specific table
		err = op.vacuumExecutor.VacuumTable(op.table.ID)
	} else {
		// Vacuum entire database
		err = op.vacuumExecutor.VacuumDatabase()
	}

	if err != nil {
		return fmt.Errorf("vacuum operation failed: %w", err)
	}

	// Get statistics
	stats := op.vacuumExecutor.GetStats()
	op.stats = &stats
	op.executed = true

	return nil
}

// Next returns the vacuum operation results
func (op *VacuumOperator) Next() (*Row, error) {
	if !op.executed {
		return nil, fmt.Errorf("vacuum not executed")
	}

	// Return results only once
	if op.stats == nil {
		return nil, io.EOF
	}

	// Create result row
	operation := "VACUUM"
	if op.table != nil {
		operation = fmt.Sprintf("VACUUM %s.%s", op.table.SchemaName, op.table.TableName)
	}

	// Safe conversion to int32 with bounds checking
	const maxInt32 = int32(^uint32(0) >> 1) // 2147483647

	tablesProcessed := maxInt32
	if op.stats.TablesProcessed <= int(maxInt32) {
		tablesProcessed = int32(op.stats.TablesProcessed) //nolint:gosec // Safe conversion after bounds check
	}

	versionsScanned := maxInt32
	if op.stats.VersionsScanned <= int(maxInt32) {
		versionsScanned = int32(op.stats.VersionsScanned) //nolint:gosec // Safe conversion after bounds check
	}

	versionsRemoved := maxInt32
	if op.stats.VersionsRemoved <= int(maxInt32) {
		versionsRemoved = int32(op.stats.VersionsRemoved) //nolint:gosec // Safe conversion after bounds check
	}

	spaceReclaimed := maxInt32
	if op.stats.SpaceReclaimed <= int64(maxInt32) {
		spaceReclaimed = int32(op.stats.SpaceReclaimed) //nolint:gosec // Safe conversion after bounds check
	}

	durationMs := maxInt32
	if op.stats.Duration.Milliseconds() <= int64(maxInt32) {
		durationMs = int32(op.stats.Duration.Milliseconds()) //nolint:gosec // Safe conversion after bounds check
	}

	row := &Row{
		Values: []types.Value{
			types.NewValue(operation),
			types.NewValue(tablesProcessed),
			types.NewValue(versionsScanned),
			types.NewValue(versionsRemoved),
			types.NewValue(spaceReclaimed),
			types.NewValue(durationMs),
		},
	}

	// Clear stats to return EOF on next call
	op.stats = nil

	return row, nil
}

// Close cleans up the vacuum operator
func (op *VacuumOperator) Close() error {
	op.executed = false
	op.stats = nil
	op.vacuumExecutor = nil
	return nil
}

// EstimateCost estimates the cost of vacuum operation
func (op *VacuumOperator) EstimateCost() planner.Cost {
	// Vacuum is expensive - it scans all pages
	if op.table != nil {
		// Single table vacuum
		return planner.Cost{
			SetupCost: 1000.0,
			TotalCost: 10000.0,
			CPUCost:   8000.0,
			IOCost:    1000.0,
		}
	}

	// Full database vacuum
	return planner.Cost{
		SetupCost: 10000.0,
		TotalCost: 100000.0,
		CPUCost:   80000.0,
		IOCost:    10000.0,
	}
}

// Explain returns a string representation for EXPLAIN
func (op *VacuumOperator) Explain() string {
	if op.table != nil {
		return fmt.Sprintf("Vacuum on %s.%s", op.table.SchemaName, op.table.TableName)
	}
	return "Vacuum on all tables"
}

// VacuumAnalyzeOperator combines VACUUM with statistics update (future enhancement)
type VacuumAnalyzeOperator struct {
	VacuumOperator
	analyzeOp *AnalyzeOperator
}

// NewVacuumAnalyzeOperator creates a VACUUM ANALYZE operator
func NewVacuumAnalyzeOperator(storage *MVCCStorageBackend, catalog catalog.Catalog, table *catalog.Table) *VacuumAnalyzeOperator {
	// Create a LogicalAnalyze plan for the analyze operator
	var plan *planner.LogicalAnalyze
	if table != nil {
		plan = planner.NewLogicalAnalyze(table.SchemaName, table.TableName, nil)
	} else {
		// For all tables, we'll need to handle this differently
		plan = planner.NewLogicalAnalyze("", "", nil)
	}

	return &VacuumAnalyzeOperator{
		VacuumOperator: *NewVacuumOperator(storage, table),
		analyzeOp:      NewAnalyzeOperator(plan, catalog, storage),
	}
}

// Open executes both vacuum and analyze
func (op *VacuumAnalyzeOperator) Open(ctx *ExecContext) error {
	// First vacuum
	if err := op.VacuumOperator.Open(ctx); err != nil {
		return err
	}

	// Then analyze to update statistics
	if err := op.analyzeOp.Open(ctx); err != nil {
		return fmt.Errorf("analyze after vacuum failed: %w", err)
	}

	return nil
}

// Next returns combined results
func (op *VacuumAnalyzeOperator) Next() (*Row, error) {
	// First return vacuum results
	row, err := op.VacuumOperator.Next()
	if err != nil || row != nil {
		return row, err
	}

	// Then return analyze results
	return op.analyzeOp.Next()
}

// Close cleans up both operators
func (op *VacuumAnalyzeOperator) Close() error {
	vacuumErr := op.VacuumOperator.Close()
	analyzeErr := op.analyzeOp.Close()

	if vacuumErr != nil {
		return vacuumErr
	}
	return analyzeErr
}
