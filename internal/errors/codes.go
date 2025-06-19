package errors

// PostgreSQL Error Codes (SQLSTATE)
// Based on PostgreSQL error codes: https://www.postgresql.org/docs/current/errcodes-appendix.html

// Class 00 - Successful Completion
const (
	SuccessfulCompletion = "00000"
)

// Class 01 - Warning
const (
	Warning                          = "01000"
	DynamicResultSetsReturned        = "0100C"
	ImplicitZeroBitPadding           = "01008"
	NullValueEliminatedInSetFunction = "01003"
	PrivilegeNotGranted              = "01007"
	PrivilegeNotRevoked              = "01006"
	StringDataRightTruncationWarning = "01004"
	DeprecatedFeature                = "01P01"
)

// Class 02 - No Data
const (
	NoData                                = "02000"
	NoAdditionalDynamicResultSetsReturned = "02001"
)

// Class 03 - SQL Statement Not Yet Complete
const (
	SQLStatementNotYetComplete = "03000"
)

// Class 08 - Connection Exception
const (
	ConnectionException                        = "08000"
	ConnectionDoesNotExist                     = "08003"
	ConnectionFailure                          = "08006"
	SQLClientUnableToEstablishConnection       = "08001"
	SQLServerRejectedEstablishmentOfConnection = "08004"
	TransactionResolutionUnknown               = "08007"
	ProtocolViolation                          = "08P01"
)

// Class 09 - Triggered Action Exception
const (
	TriggeredActionException = "09000"
)

// Class 0A - Feature Not Supported
const (
	FeatureNotSupported = "0A000"
)

// Class 0B - Invalid Transaction Initiation
const (
	InvalidTransactionInitiation = "0B000"
)

// Class 0F - Locator Exception
const (
	LocatorException            = "0F000"
	InvalidLocatorSpecification = "0F001"
)

// Class 0L - Invalid Grantor
const (
	InvalidGrantor        = "0L000"
	InvalidGrantOperation = "0LP01"
)

// Class 0P - Invalid Role Specification
const (
	InvalidRoleSpecification = "0P000"
)

// Class 0Z - Diagnostics Exception
const (
	DiagnosticsException                           = "0Z000"
	StackedDiagnosticsAccessedWithoutActiveHandler = "0Z002"
)

// Class 20 - Case Not Found
const (
	CaseNotFound = "20000"
)

// Class 21 - Cardinality Violation
const (
	CardinalityViolation = "21000"
)

// Class 22 - Data Exception
const (
	DataException                             = "22000"
	ArraySubscriptError                       = "2202E"
	CharacterNotInRepertoire                  = "22021"
	DatetimeFieldOverflow                     = "22008"
	DivisionByZero                            = "22012"
	ErrorInAssignment                         = "22005"
	EscapeCharacterConflict                   = "2200B"
	IndicatorOverflow                         = "22022"
	IntervalFieldOverflow                     = "22015"
	InvalidArgumentForLogarithm               = "2201E"
	InvalidArgumentForNtileFunction           = "22014"
	InvalidArgumentForNthValueFunction        = "22016"
	InvalidArgumentForPowerFunction           = "2201F"
	InvalidArgumentForWidthBucketFunction     = "2201G"
	InvalidCharacterValueForCast              = "22018"
	InvalidDatetimeFormat                     = "22007"
	InvalidEscapeCharacter                    = "22019"
	InvalidEscapeOctet                        = "2200D"
	InvalidEscapeSequence                     = "22025"
	NonstandardUseOfEscapeCharacter           = "22P06"
	InvalidIndicatorParameterValue            = "22010"
	InvalidParameterValue                     = "22023"
	InvalidPrecedingOrFollowingSize           = "22013"
	InvalidRegularExpression                  = "2201B"
	InvalidRowCountInLimitClause              = "2201W"
	InvalidRowCountInResultOffsetClause       = "2201X"
	InvalidTablesampleArgument                = "2202H"
	InvalidTablesampleRepeat                  = "2202G"
	InvalidTimeZoneDisplacementValue          = "22009"
	InvalidUseOfEscapeCharacter               = "2200C"
	MostSpecificTypeMismatch                  = "2200G"
	NullValueNotAllowed                       = "22004"
	NullValueNoIndicatorParameter             = "22002"
	NumericValueOutOfRange                    = "22003"
	SequenceGeneratorLimitExceeded            = "2200H"
	StringDataLengthMismatch                  = "22026"
	StringDataRightTruncation                 = "22001"
	SubstringError                            = "22011"
	TrimError                                 = "22027"
	UnterminatedCString                       = "22024"
	ZeroLengthCharacterString                 = "2200F"
	FloatingPointException                    = "22P01"
	InvalidTextRepresentation                 = "22P02"
	InvalidBinaryRepresentation               = "22P03"
	BadCopyFileFormat                         = "22P04"
	UntranslatableCharacter                   = "22P05"
	NotAnXMLDocument                          = "2200L"
	InvalidXMLDocument                        = "2200M"
	InvalidXMLContent                         = "2200N"
	InvalidXMLComment                         = "2200S"
	InvalidXMLProcessingInstruction           = "2200T"
	DuplicateJSON                             = "22030"
	InvalidArgumentForSQLJSONDatetimeFunction = "22031"
	InvalidJSONText                           = "22032"
	InvalidSQLJSONSubscript                   = "22033"
	MoreThanOneSQLJSONItem                    = "22034"
	NoSQLJSONItem                             = "22035"
	NonNumericSQLJSONItem                     = "22036"
	NonUniqueKeysInAJSONObject                = "22037"
	SingletonSQLJSONItemRequired              = "22038"
	SQLJSONArrayNotFound                      = "22039"
	SQLJSONMemberNotFound                     = "2203A"
	SQLJSONNumberNotFound                     = "2203B"
	SQLJSONObjectNotFound                     = "2203C"
	TooManyJSONArrayElements                  = "2203D"
	TooManyJSONObjectMembers                  = "2203E"
	SQLJSONScalarRequired                     = "2203F"
	SQLJSONItemCannotBeCastToTargetType       = "2203G"
)

// Class 23 - Integrity Constraint Violation
const (
	IntegrityConstraintViolation = "23000"
	RestrictViolation            = "23001"
	NotNullViolation             = "23502"
	ForeignKeyViolation          = "23503"
	UniqueViolation              = "23505"
	CheckViolation               = "23514"
	ExclusionViolation           = "23P01"
)

// Class 24 - Invalid Cursor State
const (
	InvalidCursorState = "24000"
)

// Class 25 - Invalid Transaction State
const (
	InvalidTransactionState                         = "25000"
	ActiveSQLTransaction                            = "25001"
	BranchTransactionAlreadyActive                  = "25002"
	HeldCursorRequiresSameIsolationLevel            = "25008"
	InappropriateAccessModeForBranchTransaction     = "25003"
	InappropriateIsolationLevelForBranchTransaction = "25004"
	NoActiveSQLTransactionForBranchTransaction      = "25005"
	ReadOnlySQLTransaction                          = "25006"
	SchemaAndDataStatementMixingNotSupported        = "25007"
	NoActiveSQLTransaction                          = "25P01"
	InFailedSQLTransaction                          = "25P02"
	IdleInTransactionSessionTimeout                 = "25P03"
)

// Class 26 - Invalid SQL Statement Name
const (
	InvalidSQLStatementName = "26000"
)

// Class 27 - Triggered Data Change Violation
const (
	TriggeredDataChangeViolation = "27000"
)

// Class 28 - Invalid Authorization Specification
const (
	InvalidAuthorizationSpecification = "28000"
	InvalidPassword                   = "28P01"
)

// Class 2B - Dependent Privilege Descriptors Still Exist
const (
	DependentPrivilegeDescriptorsStillExist = "2B000"
	DependentObjectsStillExist              = "2BP01"
)

// Class 2D - Invalid Transaction Termination
const (
	InvalidTransactionTermination = "2D000"
)

// Class 2F - SQL Routine Exception
const (
	SQLRoutineException                       = "2F000"
	FunctionExecutedNoReturnStatement         = "2F005"
	ModifyingSQLDataNotPermittedInFunction    = "2F002"
	ProhibitedSQLStatementAttemptedInFunction = "2F003"
	ReadingSQLDataNotPermittedInFunction      = "2F004"
)

// Class 34 - Invalid Cursor Name
const (
	InvalidCursorName = "34000"
)

// Class 38 - External Routine Exception
const (
	ExternalRoutineException                         = "38000"
	ContainingSQLNotPermitted                        = "38001"
	ModifyingSQLDataNotPermittedInExternalRoutine    = "38002"
	ProhibitedSQLStatementAttemptedInExternalRoutine = "38003"
	ReadingSQLDataNotPermittedInExternalRoutine      = "38004"
)

// Class 39 - External Routine Invocation Exception
const (
	ExternalRoutineInvocationException   = "39000"
	InvalidSQLStateReturned              = "39001"
	NullValueNotAllowedInExternalRoutine = "39004"
	TriggerProtocolViolated              = "39P01"
	SRFProtocolViolated                  = "39P02"
	EventTriggerProtocolViolated         = "39P03"
)

// Class 3B - Savepoint Exception
const (
	SavepointException            = "3B000"
	InvalidSavepointSpecification = "3B001"
)

// Class 3D - Invalid Catalog Name
const (
	InvalidCatalogName = "3D000"
)

// Class 3F - Invalid Schema Name
const (
	InvalidSchemaName = "3F000"
)

// Class 40 - Transaction Rollback
const (
	TransactionRollback                     = "40000"
	TransactionIntegrityConstraintViolation = "40002"
	SerializationFailure                    = "40001"
	StatementCompletionUnknown              = "40003"
	DeadlockDetected                        = "40P01"
)

// Class 42 - Syntax Error or Access Rule Violation
const (
	SyntaxErrorOrAccessRuleViolation   = "42000"
	SyntaxError                        = "42601"
	InsufficientPrivilege              = "42501"
	CannotCoerce                       = "42846"
	GroupingError                      = "42803"
	WindowingError                     = "42P20"
	InvalidRecursion                   = "42P19"
	InvalidForeignKey                  = "42830"
	InvalidName                        = "42602"
	NameTooLong                        = "42622"
	ReservedName                       = "42939"
	DatatypeMismatch                   = "42804"
	IndeterminateDatatype              = "42P18"
	CollationMismatch                  = "42P21"
	IndeterminateCollation             = "42P22"
	WrongObjectType                    = "42809"
	GeneratedAlways                    = "428C9"
	UndefinedColumn                    = "42703"
	UndefinedFunction                  = "42883"
	UndefinedTable                     = "42P01"
	UndefinedParameter                 = "42P02"
	UndefinedObject                    = "42704"
	DuplicateColumn                    = "42701"
	DuplicateCursor                    = "42P03"
	DuplicateDatabase                  = "42P04"
	DuplicateFunction                  = "42723"
	DuplicatePreparedStatement         = "42P05"
	DuplicateSchema                    = "42P06"
	DuplicateTable                     = "42P07"
	DuplicateAlias                     = "42712"
	DuplicateObject                    = "42710"
	AmbiguousColumn                    = "42702"
	AmbiguousFunction                  = "42725"
	AmbiguousParameter                 = "42P08"
	AmbiguousAlias                     = "42P09"
	InvalidColumnReference             = "42P10"
	InvalidColumnDefinition            = "42611"
	InvalidCursorDefinition            = "42P11"
	InvalidDatabaseDefinition          = "42P12"
	InvalidFunctionDefinition          = "42P13"
	InvalidPreparedStatementDefinition = "42P14"
	InvalidSchemaDefinition            = "42P15"
	InvalidTableDefinition             = "42P16"
	InvalidObjectDefinition            = "42P17"
)

// Class 44 - WITH CHECK OPTION Violation
const (
	WithCheckOptionViolation = "44000"
)

// Class 53 - Insufficient Resources
const (
	InsufficientResources      = "53000"
	DiskFull                   = "53100"
	OutOfMemory                = "53200"
	TooManyConnections         = "53300"
	ConfigurationLimitExceeded = "53400"
)

// Class 54 - Program Limit Exceeded
const (
	ProgramLimitExceeded = "54000"
	StatementTooComplex  = "54001"
	TooManyColumns       = "54011"
	TooManyArguments     = "54023"
)

// Class 55 - Object Not In Prerequisite State
const (
	ObjectNotInPrerequisiteState = "55000"
	ObjectInUse                  = "55006"
	CantChangeRuntimeParam       = "55P02"
	LockNotAvailable             = "55P03"
	UnsafeNewEnumValueUsage      = "55P04"
)

// Class 57 - Operator Intervention
const (
	OperatorIntervention = "57000"
	QueryCanceled        = "57014"
	AdminShutdown        = "57P01"
	CrashShutdown        = "57P02"
	CannotConnectNow     = "57P03"
	DatabaseDropped      = "57P04"
	IdleSessionTimeout   = "57P05"
)

// Class 58 - System Error
const (
	SystemError   = "58000"
	IOError       = "58030"
	UndefinedFile = "58P01"
	DuplicateFile = "58P02"
)

// Class 72 - Snapshot Failure
const (
	SnapshotTooOld = "72000"
)

// Class F0 - Configuration File Error
const (
	ConfigFileError = "F0000"
	LockFileExists  = "F0001"
)

// Class HV - Foreign Data Wrapper Error
const (
	FDWError                             = "HV000"
	FDWColumnNameNotFound                = "HV005"
	FDWDynamicParameterValueNeeded       = "HV002"
	FDWFunctionSequenceError             = "HV010"
	FDWInconsistentDescriptorInformation = "HV021"
	FDWInvalidAttributeValue             = "HV024"
	FDWInvalidColumnName                 = "HV007"
	FDWInvalidColumnNumber               = "HV008"
	FDWInvalidDataType                   = "HV004"
	FDWInvalidDataTypeDescriptors        = "HV006"
	FDWInvalidDescriptorFieldIdentifier  = "HV091"
	FDWInvalidHandle                     = "HV00B"
	FDWInvalidOptionIndex                = "HV00C"
	FDWInvalidOptionName                 = "HV00D"
	FDWInvalidStringLengthOrBufferLength = "HV090"
	FDWInvalidStringFormat               = "HV00A"
	FDWInvalidUseOfNullPointer           = "HV009"
	FDWTooManyHandles                    = "HV014"
	FDWOutOfMemory                       = "HV001"
	FDWNoSchemas                         = "HV00P"
	FDWOptionNameNotFound                = "HV00J"
	FDWReplyHandle                       = "HV00K"
	FDWSchemaNotFound                    = "HV00Q"
	FDWTableNotFound                     = "HV00R"
	FDWUnableToCreateExecution           = "HV00L"
	FDWUnableToCreateReply               = "HV00M"
	FDWUnableToEstablishConnection       = "HV00N"
)

// Class P0 - PL/pgSQL Error
const (
	PLpgSQLError   = "P0000"
	RaiseException = "P0001"
	NoDataFound    = "P0002"
	TooManyRows    = "P0003"
	AssertFailure  = "P0004"
)

// Class XX - Internal Error
const (
	InternalError  = "XX000"
	DataCorrupted  = "XX001"
	IndexCorrupted = "XX002"
)
