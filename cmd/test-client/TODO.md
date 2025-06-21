# test-client TODO

This test client needs implementation for:

1. **PostgreSQL Protocol Testing** - Full wire protocol test suite
2. **Performance Benchmarks** - Query performance testing
3. **Load Testing** - Concurrent connection testing
4. **SQL Compliance Testing** - Test various SQL queries

## Implementation Notes

- Should test both simple and extended query protocols
- Add SSL/TLS testing when server supports it
- Include transaction testing scenarios
- Test error handling and edge cases
- Consider moving to test/ directory instead of cmd/