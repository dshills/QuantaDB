package executor

import (
	"testing"

	"github.com/dshills/QuantaDB/internal/sql/planner"
	"github.com/dshills/QuantaDB/internal/sql/types"
)

func TestNestedLoopJoin(t *testing.T) {
	// Create test data for employees table
	employeeRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice"), types.NewValue(int64(10))}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob"), types.NewValue(int64(20))}},
		{Values: []types.Value{types.NewValue(int64(3)), types.NewValue("Charlie"), types.NewValue(int64(10))}},
	}
	employeeSchema := &Schema{
		Columns: []Column{
			{Name: "emp_id", Type: types.Integer},
			{Name: "emp_name", Type: types.Text},
			{Name: "dept_id", Type: types.Integer},
		},
	}

	// Create test data for departments table
	departmentRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(10)), types.NewValue("Engineering")}},
		{Values: []types.Value{types.NewValue(int64(20)), types.NewValue("Sales")}},
		{Values: []types.Value{types.NewValue(int64(30)), types.NewValue("HR")}},
	}
	departmentSchema := &Schema{
		Columns: []Column{
			{Name: "dept_id", Type: types.Integer},
			{Name: "dept_name", Type: types.Text},
		},
	}

	t.Run("Inner join with equality predicate", func(t *testing.T) {
		// Create mock operators
		leftMock := newMockOperator(employeeRows, employeeSchema)
		rightMock := newMockOperator(departmentRows, departmentSchema)

		// Create join predicate: emp.dept_id = dept.dept_id
		// Column indices in joined row: emp_id(0), emp_name(1), emp_dept_id(2), dept_id(3), dept_name(4)
		predicate := &binaryOpEvaluator{
			left:     &columnRefEvaluator{columnIdx: 2, resolved: true}, // emp.dept_id
			right:    &columnRefEvaluator{columnIdx: 3, resolved: true}, // dept.dept_id
			operator: planner.OpEqual,
			dataType: types.Boolean,
		}

		joinOp := NewNestedLoopJoinOperator(leftMock, rightMock, predicate, InnerJoin)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := joinOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open join operator: %v", err)
		}
		defer joinOp.Close()

		// Expected results:
		// Alice (dept_id=10) joins with Engineering (dept_id=10)
		// Bob (dept_id=20) joins with Sales (dept_id=20)
		// Charlie (dept_id=10) joins with Engineering (dept_id=10)
		expected := []struct {
			empName  string
			deptName string
		}{
			{"Alice", "Engineering"},
			{"Bob", "Sales"},
			{"Charlie", "Engineering"},
		}

		for i, exp := range expected {
			row, err := joinOp.Next()
			if err != nil {
				t.Fatalf("Error getting row %d: %v", i, err)
			}
			if row == nil {
				t.Fatalf("Expected row %d, got nil", i)
			}

			empName := row.Values[1].Data.(string)
			deptName := row.Values[4].Data.(string)

			if empName != exp.empName || deptName != exp.deptName {
				t.Errorf("Row %d: expected (%s, %s), got (%s, %s)",
					i, exp.empName, exp.deptName, empName, deptName)
			}
		}

		// Check EOF
		row, err := joinOp.Next()
		if err != nil {
			t.Fatalf("Error checking EOF: %v", err)
		}
		if row != nil {
			t.Error("Expected EOF, got row")
		}
	})

	t.Run("Cross join (no predicate)", func(t *testing.T) {
		// Use smaller test sets for cross join
		leftRows := []*Row{
			{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("A")}},
			{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("B")}},
		}
		rightRows := []*Row{
			{Values: []types.Value{types.NewValue(int64(10)), types.NewValue("X")}},
			{Values: []types.Value{types.NewValue(int64(20)), types.NewValue("Y")}},
		}

		leftSchema := &Schema{
			Columns: []Column{
				{Name: "id1", Type: types.Integer},
				{Name: "val1", Type: types.Text},
			},
		}
		rightSchema := &Schema{
			Columns: []Column{
				{Name: "id2", Type: types.Integer},
				{Name: "val2", Type: types.Text},
			},
		}

		leftMock := newMockOperator(leftRows, leftSchema)
		rightMock := newMockOperator(rightRows, rightSchema)

		// No predicate means all combinations
		joinOp := NewNestedLoopJoinOperator(leftMock, rightMock, nil, InnerJoin)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := joinOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open join operator: %v", err)
		}
		defer joinOp.Close()

		// Expected: 2 x 2 = 4 rows
		rowCount := 0
		for {
			row, err := joinOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++
		}

		if rowCount != 4 {
			t.Errorf("Expected 4 rows from cross join, got %d", rowCount)
		}
	})
}

func TestHashJoin(t *testing.T) {
	// Create test data for employees table
	employeeRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(1)), types.NewValue("Alice"), types.NewValue(int64(10))}},
		{Values: []types.Value{types.NewValue(int64(2)), types.NewValue("Bob"), types.NewValue(int64(20))}},
		{Values: []types.Value{types.NewValue(int64(3)), types.NewValue("Charlie"), types.NewValue(int64(10))}},
		{Values: []types.Value{types.NewValue(int64(4)), types.NewValue("David"), types.NewValue(int64(40))}}, // No matching dept
	}
	employeeSchema := &Schema{
		Columns: []Column{
			{Name: "emp_id", Type: types.Integer},
			{Name: "emp_name", Type: types.Text},
			{Name: "dept_id", Type: types.Integer},
		},
	}

	// Create test data for departments table
	departmentRows := []*Row{
		{Values: []types.Value{types.NewValue(int64(10)), types.NewValue("Engineering")}},
		{Values: []types.Value{types.NewValue(int64(20)), types.NewValue("Sales")}},
		{Values: []types.Value{types.NewValue(int64(30)), types.NewValue("HR")}}, // No employees
	}
	departmentSchema := &Schema{
		Columns: []Column{
			{Name: "dept_id", Type: types.Integer},
			{Name: "dept_name", Type: types.Text},
		},
	}

	t.Run("Hash join with single key", func(t *testing.T) {
		// Create mock operators
		leftMock := newMockOperator(employeeRows, employeeSchema)
		rightMock := newMockOperator(departmentRows, departmentSchema)

		// Hash join on dept_id
		leftKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 2, resolved: true}, // emp.dept_id
		}
		rightKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // dept.dept_id
		}

		joinOp := NewHashJoinOperator(leftMock, rightMock, leftKeys, rightKeys, nil, InnerJoin)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := joinOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open join operator: %v", err)
		}
		defer joinOp.Close()

		// Expected results (same as nested loop join)
		expected := []struct {
			empName  string
			deptName string
		}{
			{"Alice", "Engineering"},
			{"Bob", "Sales"},
			{"Charlie", "Engineering"},
		}

		results := make(map[string]string)
		for {
			row, err := joinOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			empName := row.Values[1].Data.(string)
			deptName := row.Values[4].Data.(string)
			results[empName] = deptName
		}

		if len(results) != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), len(results))
		}

		for _, exp := range expected {
			if dept, ok := results[exp.empName]; !ok || dept != exp.deptName {
				t.Errorf("Expected %s to be in %s, got %s", exp.empName, exp.deptName, dept)
			}
		}
	})

	t.Run("Left join", func(t *testing.T) {
		// Create mock operators
		leftMock := newMockOperator(employeeRows, employeeSchema)
		rightMock := newMockOperator(departmentRows, departmentSchema)

		// Hash join on dept_id
		leftKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 2, resolved: true}, // emp.dept_id
		}
		rightKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // dept.dept_id
		}

		joinOp := NewHashJoinOperator(leftMock, rightMock, leftKeys, rightKeys, nil, LeftJoin)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := joinOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open join operator: %v", err)
		}
		defer joinOp.Close()

		// Expected: All employees, including David with NULL department
		rowCount := 0
		davidFound := false
		for {
			row, err := joinOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}
			rowCount++

			empName := row.Values[1].Data.(string)
			if empName == "David" {
				davidFound = true
				// Check that department columns are NULL
				if !row.Values[3].IsNull() || !row.Values[4].IsNull() {
					t.Error("Expected NULL department for David in left join")
				}
			}
		}

		if rowCount != 4 {
			t.Errorf("Expected 4 rows from left join, got %d", rowCount)
		}

		if !davidFound {
			t.Error("David not found in left join results")
		}
	})

	t.Run("Hash join with additional predicate", func(t *testing.T) {
		// Create mock operators
		leftMock := newMockOperator(employeeRows, employeeSchema)
		rightMock := newMockOperator(departmentRows, departmentSchema)

		// Hash join on dept_id
		leftKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 2, resolved: true}, // emp.dept_id
		}
		rightKeys := []ExprEvaluator{
			&columnRefEvaluator{columnIdx: 0, resolved: true}, // dept.dept_id
		}

		// Additional predicate: dept_name = 'Engineering'
		additionalPredicate := &binaryOpEvaluator{
			left:     &columnRefEvaluator{columnIdx: 4, resolved: true}, // dept_name
			right:    &literalEvaluator{value: types.NewValue("Engineering")},
			operator: planner.OpEqual,
			dataType: types.Boolean,
		}

		joinOp := NewHashJoinOperator(leftMock, rightMock, leftKeys, rightKeys, additionalPredicate, InnerJoin)
		ctx := &ExecContext{Stats: &ExecStats{}}

		if err := joinOp.Open(ctx); err != nil {
			t.Fatalf("Failed to open join operator: %v", err)
		}
		defer joinOp.Close()

		// Expected: Only employees in Engineering department
		engineeringCount := 0
		for {
			row, err := joinOp.Next()
			if err != nil {
				t.Fatalf("Error getting row: %v", err)
			}
			if row == nil {
				break
			}

			deptName := row.Values[4].Data.(string)
			if deptName != "Engineering" {
				t.Errorf("Expected only Engineering department, got %s", deptName)
			}
			engineeringCount++
		}

		if engineeringCount != 2 {
			t.Errorf("Expected 2 employees in Engineering, got %d", engineeringCount)
		}
	})
}
