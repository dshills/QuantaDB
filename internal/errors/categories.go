package errors

// Category-specific error constructors for common database operations

// Parser errors
func ParseError(msg string, line, col int) *Error {
	return Newf(SyntaxError, "syntax error at line %d, column %d: %s", line, col, msg).
		WithPosition(col)
}

func UnexpectedTokenError(expected, actual string, line, col int) *Error {
	return Newf(SyntaxError, "syntax error at line %d, column %d: expected %s, got %s", line, col, expected, actual).
		WithPosition(col)
}

func InvalidIdentifierError(identifier string) *Error {
	return Newf(InvalidName, "invalid identifier: \"%s\"", identifier)
}

// Planner errors
func AmbiguousColumnError(columnName string) *Error {
	return Newf(AmbiguousColumn, "column reference \"%s\" is ambiguous", columnName).
		WithColumn(columnName)
}

func ColumnNotFoundError(columnName, tableName string) *Error {
	if tableName != "" {
		return Newf(UndefinedColumn, "column %s.%s does not exist", tableName, columnName).
			WithTable("", tableName).
			WithColumn(columnName)
	}
	return Newf(UndefinedColumn, "column \"%s\" does not exist", columnName).
		WithColumn(columnName)
}

func TableNotFoundError(tableName string) *Error {
	return UndefinedTableError(tableName)
}

func InvalidGroupByError(columnName string) *Error {
	return Newf(GroupingError, "column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", columnName).
		WithColumn(columnName)
}

func SubqueryMultipleRowsError() *Error {
	return New(CardinalityViolation, "more than one row returned by a subquery used as an expression")
}

// Executor errors
func TupleNotFoundError() *Error {
	return New(NoData, "no rows returned")
}

func MultipleRowsError(expected int) *Error {
	return Newf(CardinalityViolation, "query returned %d rows when at most one was expected", expected)
}

func InvalidCastError(fromType, toType string) *Error {
	return Newf(CannotCoerce, "cannot cast type %s to %s", fromType, toType).
		WithDataType(toType)
}

func FunctionNotFoundError(funcName string, argTypes []string) *Error {
	if len(argTypes) > 0 {
		return Newf(UndefinedFunction, "function %s(%v) does not exist", funcName, argTypes)
	}
	return Newf(UndefinedFunction, "function %s() does not exist", funcName)
}

func AggregateInWhereError(funcName string) *Error {
	return Newf(GroupingError, "aggregate functions are not allowed in WHERE").
		WithDetailf("Function %s cannot be used in WHERE clause.", funcName)
}

// Storage errors
func StorageCorruptionError(details string) *Error {
	return Newf(DataCorrupted, "data corruption detected").
		WithDetail(details).
		WithHint("You may need to restore from backup.")
}

func PageNotFoundError(pageID uint32) *Error {
	return Newf(DataCorrupted, "page %d not found", pageID)
}

func BufferPoolFullError() *Error {
	return New(OutOfMemory, "buffer pool is full").
		WithHint("Consider increasing buffer pool size.")
}

func DiskSpaceError(operation string) *Error {
	return Newf(DiskFull, "could not %s: no space left on device", operation)
}

func FileIOError(operation, filename string, err error) *Error {
	return IOErrorf("could not %s file \"%s\": %v", operation, filename, err)
}

// Transaction errors
func TransactionAbortedError(reason string) *Error {
	return Newf(InFailedSQLTransaction, "current transaction is aborted, commands ignored until end of transaction block").
		WithDetail(reason)
}

func SerializationError(details string) *Error {
	return New(SerializationFailure, "could not serialize access due to concurrent update").
		WithDetail(details).
		WithHint("The transaction might succeed if retried.")
}

func LockTimeoutError(objectName string) *Error {
	return Newf(LockNotAvailable, "could not obtain lock on %s", objectName)
}

func NoActiveTransactionError() *Error {
	return New(NoActiveSQLTransaction, "there is no transaction in progress")
}

func TransactionAlreadyActiveError() *Error {
	return New(ActiveSQLTransaction, "there is already a transaction in progress")
}

// Index errors
func IndexCorruptedError(indexName string, details string) *Error {
	return Newf(IndexCorrupted, "index \"%s\" contains corrupted data", indexName).
		WithDetail(details)
}

func DuplicateKeyError(indexName string, key string) *Error {
	return Newf(UniqueViolation, "duplicate key value violates unique constraint \"%s\"", indexName).
		WithDetailf("Key (%s) already exists.", key).
		WithConstraint(indexName)
}

// Catalog errors
func SchemaNotFoundError(schemaName string) *Error {
	return Newf(InvalidSchemaName, "schema \"%s\" does not exist", schemaName)
}

func SchemaAlreadyExistsError(schemaName string) *Error {
	return Newf(DuplicateSchema, "schema \"%s\" already exists", schemaName)
}

func DependentObjectsError(objectType, objectName string, dependentCount int) *Error {
	return Newf(DependentObjectsStillExist, "cannot drop %s %s because other objects depend on it", objectType, objectName).
		WithDetailf("%d dependent objects found.", dependentCount).
		WithHint("Use DROP ... CASCADE to drop the dependent objects too.")
}

// Type errors
func InvalidTypeError(typeName string) *Error {
	return Newf(UndefinedObject, "type \"%s\" does not exist", typeName).
		WithDataType(typeName)
}

func TypeMismatchError(expected, actual string, context string) *Error {
	return Newf(DatatypeMismatch, "%s is of type %s but expression is of type %s", context, expected, actual).
		WithHintf("You will need to rewrite or cast the expression.")
}

// Constraint errors
func CheckConstraintViolationError(constraintName, tableName string) *Error {
	return Newf(CheckViolation, "new row for relation \"%s\" violates check constraint \"%s\"", tableName, constraintName).
		WithTable("", tableName).
		WithConstraint(constraintName)
}

func ExclusionConstraintViolationError(constraintName, tableName string) *Error {
	return Newf(ExclusionViolation, "conflicting key value violates exclusion constraint \"%s\"", constraintName).
		WithTable("", tableName).
		WithConstraint(constraintName)
}

// Protocol errors
func ProtocolError(msg string) *Error {
	return Newf(ProtocolViolation, "protocol error: %s", msg)
}

func InvalidStatementNameError(name string) *Error {
	return Newf(InvalidSQLStatementName, "prepared statement \"%s\" does not exist", name)
}

func InvalidPortalNameError(name string) *Error {
	return Newf(InvalidCursorName, "portal \"%s\" does not exist", name)
}

// WAL errors
func WALCorruptionError(details string) *Error {
	return Newf(DataCorrupted, "WAL corruption detected").
		WithDetail(details).
		WithHint("Database recovery may be required.")
}

func CheckpointFailedError(reason string) *Error {
	return Newf(IOError, "checkpoint failed").
		WithDetail(reason)
}

// Connection errors
func TooManyConnectionsError(current, max int) *Error {
	return Newf(TooManyConnections, "too many connections").
		WithDetailf("Current connections: %d, Maximum allowed: %d", current, max)
}

func AuthenticationFailedError(username string) *Error {
	return Newf(InvalidPassword, "password authentication failed for user \"%s\"", username)
}

func ConnectionTimeoutError() *Error {
	return New(ConnectionFailure, "connection timed out")
}

// Permission errors
func PermissionDeniedError(operation, objectType, objectName string) *Error {
	return InsufficientPrivilegeError(operation, objectType, objectName)
}

func PermissionDeniedErrorf(format string, args ...interface{}) *Error {
	return Newf(InsufficientPrivilege, format, args...)
}

func ReadOnlyTransactionError() *Error {
	return New(ReadOnlySQLTransaction, "cannot execute operation in a read-only transaction")
}

// Configuration errors
func InvalidConfigurationError(parameter, value string) *Error {
	return Newf(ConfigFileError, "invalid value for parameter \"%s\": \"%s\"", parameter, value)
}

func InvalidParameterValueError(parameter, value, reason string) *Error {
	return Newf(InvalidParameterValue, "invalid value for parameter \"%s\": \"%s\"", parameter, value).
		WithDetail(reason)
}
