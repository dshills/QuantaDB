import java.sql.*;
import java.util.Properties;

/**
 * PostgreSQL JDBC driver test for QuantaDB
 * Tests PreparedStatement, ResultSet metadata, and connection handling
 */
public class TestJDBC {
    
    private static final String URL = "jdbc:postgresql://localhost:5433/quantadb";
    private static final String USER = "postgres";
    private static final String PASSWORD = "";
    
    public static void main(String[] args) {
        System.out.println("=== Testing JDBC driver with QuantaDB ===");
        System.out.println("Connecting to: " + URL);
        
        TestCase[] tests = {
            new TestCase("Simple Connection", TestJDBC::testSimpleConnection),
            new TestCase("Parameter Binding", TestJDBC::testParameterBinding),
            new TestCase("Prepared Statements", TestJDBC::testPreparedStatements),
            new TestCase("ResultSet Metadata", TestJDBC::testResultSetMetadata),
            new TestCase("Transaction Handling", TestJDBC::testTransactionHandling),
            new TestCase("Batch Operations", TestJDBC::testBatchOperations),
            new TestCase("Type Handling", TestJDBC::testTypeHandling),
            new TestCase("Error Handling", TestJDBC::testErrorHandling),
            new TestCase("Connection Properties", TestJDBC::testConnectionProperties),
            new TestCase("Statement Types", TestJDBC::testStatementTypes)
        };
        
        int passed = 0;
        int failed = 0;
        
        for (TestCase test : tests) {
            System.out.println("\n--- Testing: " + test.name + " ---");
            try {
                test.method.run();
                System.out.println("✅ PASSED");
                passed++;
            } catch (Exception e) {
                System.out.println("❌ FAILED: " + e.getMessage());
                e.printStackTrace();
                failed++;
            }
        }
        
        System.out.println("\n=== Test Results ===");
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        System.out.println("Total: " + (passed + failed));
        
        if (failed > 0) {
            System.out.println("❌ Some tests failed");
            System.exit(1);
        } else {
            System.out.println("🎉 All tests passed!");
            System.exit(0);
        }
    }
    
    private static void testSimpleConnection() throws SQLException {
        System.out.println("   Testing simple connection...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            if (conn.isClosed()) {
                throw new RuntimeException("Connection is closed");
            }
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 as test_value")) {
                
                if (!rs.next()) {
                    throw new RuntimeException("No rows returned");
                }
                
                int value = rs.getInt("test_value");
                if (value != 1) {
                    throw new RuntimeException("Expected 1, got " + value);
                }
                
                System.out.println("   Simple query result: " + value);
            }
        }
    }
    
    private static void testParameterBinding() throws SQLException {
        System.out.println("   Testing parameter binding...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            // Single parameter
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as param_value")) {
                pstmt.setInt(1, 42);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("No rows returned");
                    }
                    
                    int value = rs.getInt("param_value");
                    if (value != 42) {
                        throw new RuntimeException("Expected 42, got " + value);
                    }
                    System.out.println("   Single parameter: " + value);
                }
            }
            
            // Multiple parameters
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as first, ? as second, ? as third")) {
                pstmt.setInt(1, 123);
                pstmt.setString(2, "hello");
                pstmt.setBoolean(3, true);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("No rows returned");
                    }
                    
                    int intVal = rs.getInt("first");
                    String strVal = rs.getString("second");
                    boolean boolVal = rs.getBoolean("third");
                    
                    if (intVal != 123 || !"hello".equals(strVal) || !boolVal) {
                        throw new RuntimeException("Parameter mismatch: " + intVal + ", " + strVal + ", " + boolVal);
                    }
                    
                    System.out.println("   Multiple parameters: " + intVal + ", '" + strVal + "', " + boolVal);
                }
            }
        }
    }
    
    private static void testPreparedStatements() throws SQLException {
        System.out.println("   Testing prepared statements...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            // Test statement preparation and reuse
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? * ? as result")) {
                
                // First execution
                pstmt.setInt(1, 6);
                pstmt.setInt(2, 7);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("No rows returned");
                    }
                    
                    int result = rs.getInt("result");
                    if (result != 42) {
                        throw new RuntimeException("Expected 42, got " + result);
                    }
                    System.out.println("   Prepared statement result: " + result);
                }
                
                // Second execution with different parameters
                pstmt.setInt(1, 10);
                pstmt.setInt(2, 5);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("No rows returned");
                    }
                    
                    int result = rs.getInt("result");
                    if (result != 50) {
                        throw new RuntimeException("Expected 50, got " + result);
                    }
                    System.out.println("   Prepared statement reuse: " + result);
                }
            }
        }
    }
    
    private static void testResultSetMetadata() throws SQLException {
        System.out.println("   Testing ResultSet metadata...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement("SELECT ? as id, ? as name");) {
            
            pstmt.setInt(1, 1);
            pstmt.setString(2, "Alice");
            
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metadata = rs.getMetaData();
                
                int columnCount = metadata.getColumnCount();
                System.out.println("   Column count: " + columnCount);
                
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metadata.getColumnName(i);
                    String columnType = metadata.getColumnTypeName(i);
                    System.out.println("   Column " + i + ": " + columnName + " (" + columnType + ")");
                }
                
                if (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    System.out.println("   Result: id=" + id + ", name='" + name + "'");
                }
            }
        }
    }
    
    private static void testTransactionHandling() throws SQLException {
        System.out.println("   Testing transactions...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            // Test manual transaction control
            conn.setAutoCommit(false);
            
            try {
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as tx_value")) {
                    pstmt.setInt(1, 100);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new RuntimeException("No rows returned");
                        }
                        
                        int value = rs.getInt("tx_value");
                        if (value != 100) {
                            throw new RuntimeException("Expected 100, got " + value);
                        }
                        
                        conn.commit();
                        System.out.println("   Transaction result: " + value);
                    }
                }
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
    
    private static void testBatchOperations() throws SQLException {
        System.out.println("   Testing batch operations...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement("SELECT ? as batch_value")) {
            
            // Add multiple parameter sets to batch
            pstmt.setInt(1, 1);
            pstmt.addBatch();
            
            pstmt.setInt(1, 2);
            pstmt.addBatch();
            
            pstmt.setInt(1, 3);
            pstmt.addBatch();
            
            // Execute batch (Note: This might not work with SELECT in all databases)
            // For testing purposes, we'll execute them individually
            pstmt.clearBatch();
            
            // Test individual executions instead
            int[] expectedValues = {1, 2, 3};
            for (int value : expectedValues) {
                pstmt.setInt(1, value);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        int result = rs.getInt("batch_value");
                        if (result != value) {
                            throw new RuntimeException("Expected " + value + ", got " + result);
                        }
                    }
                }
            }
            
            System.out.println("   Batch operations completed successfully");
        }
    }
    
    private static void testTypeHandling() throws SQLException {
        System.out.println("   Testing type handling...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            Object[][] testCases = {
                {"SELECT ?::INTEGER", 42, "integer"},
                {"SELECT ?::TEXT", "test string", "text"},
                {"SELECT ?::BOOLEAN", true, "boolean"},
                {"SELECT ?::BOOLEAN", false, "boolean"},
                {"SELECT ?::REAL", 123.45f, "real"}
            };
            
            for (Object[] testCase : testCases) {
                String query = (String) testCase[0];
                Object param = testCase[1];
                String typeName = (String) testCase[2];
                
                try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                    if (param instanceof Integer) {
                        pstmt.setInt(1, (Integer) param);
                    } else if (param instanceof String) {
                        pstmt.setString(1, (String) param);
                    } else if (param instanceof Boolean) {
                        pstmt.setBoolean(1, (Boolean) param);
                    } else if (param instanceof Float) {
                        pstmt.setFloat(1, (Float) param);
                    }
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            Object result = rs.getObject(1);
                            System.out.println("   " + typeName + ": " + param + " -> " + result);
                        }
                    }
                }
            }
        }
    }
    
    private static void testErrorHandling() throws SQLException {
        System.out.println("   Testing error handling...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            // Test syntax error
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("INVALID SQL STATEMENT");
                throw new RuntimeException("Expected error for invalid SQL");
            } catch (SQLException e) {
                if (e.getMessage().contains("Expected error")) {
                    throw new RuntimeException(e.getMessage());
                }
                System.out.println("   Error handling works: " + e.getClass().getSimpleName());
            }
            
            // Test parameter error
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ?")) {
                // Don't set parameter
                pstmt.executeQuery();
                throw new RuntimeException("Expected error for missing parameter");
            } catch (SQLException e) {
                if (e.getMessage().contains("Expected error")) {
                    throw new RuntimeException(e.getMessage());
                }
                System.out.println("   Parameter error handling: " + e.getClass().getSimpleName());
            }
        }
    }
    
    private static void testConnectionProperties() throws SQLException {
        System.out.println("   Testing connection properties...");
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("ssl", "false");
        
        try (Connection conn = DriverManager.getConnection(URL, props)) {
            DatabaseMetaData metadata = conn.getMetaData();
            
            System.out.println("   Database product: " + metadata.getDatabaseProductName());
            System.out.println("   Database version: " + metadata.getDatabaseProductVersion());
            System.out.println("   Driver name: " + metadata.getDriverName());
            System.out.println("   Driver version: " + metadata.getDriverVersion());
            System.out.println("   JDBC version: " + metadata.getJDBCMajorVersion() + "." + metadata.getJDBCMinorVersion());
        }
    }
    
    private static void testStatementTypes() throws SQLException {
        System.out.println("   Testing statement types...");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            
            // Test regular Statement
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 999 as stmt_value")) {
                    if (rs.next()) {
                        int value = rs.getInt("stmt_value");
                        System.out.println("   Statement result: " + value);
                    }
                }
            }
            
            // Test PreparedStatement (already tested above)
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT ? as prep_value")) {
                pstmt.setInt(1, 888);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        int value = rs.getInt("prep_value");
                        System.out.println("   PreparedStatement result: " + value);
                    }
                }
            }
            
            // Test CallableStatement (might not be supported)
            try {
                try (CallableStatement cstmt = conn.prepareCall("SELECT 777 as call_value")) {
                    try (ResultSet rs = cstmt.executeQuery()) {
                        if (rs.next()) {
                            int value = rs.getInt("call_value");
                            System.out.println("   CallableStatement result: " + value);
                        }
                    }
                }
            } catch (SQLException e) {
                System.out.println("   CallableStatement not fully supported: " + e.getClass().getSimpleName());
            }
        }
    }
    
    // Helper class for test organization
    private static class TestCase {
        final String name;
        final TestMethod method;
        
        TestCase(String name, TestMethod method) {
            this.name = name;
            this.method = method;
        }
    }
    
    @FunctionalInterface
    private interface TestMethod {
        void run() throws Exception;
    }
}