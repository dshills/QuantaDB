# QuantaDB Cluster Documentation

✅ **PRODUCTION-READY**: QuantaDB's distributed clustering features are now production-ready with enterprise-grade capabilities including synchronous replication, advanced query routing, and comprehensive monitoring.

## Overview

QuantaDB includes production-ready distributed cluster support with:
- **Raft Consensus**: For cluster coordination and leader election
- **Synchronous/Asynchronous Replication**: Multiple consistency modes with configurable timeouts
- **Advanced Query Routing**: Intelligent load balancing with health-aware decisions
- **Enhanced Failover**: Split-brain prevention with witness nodes
- **Cluster Monitoring**: Real-time metrics and comprehensive management API
- **Distributed Backup**: Point-in-time recovery with cross-cluster coordination

## Architecture

### Components

1. **Cluster Coordinator** (`internal/cluster/`)
   - Central component managing all distributed features
   - Integrates Raft, replication, and failover managers
   - Provides unified interface for cluster operations

2. **Raft Consensus** (`internal/cluster/raft/`)
   - Implements the Raft consensus algorithm
   - Handles leader election and log replication
   - Ensures consistency across cluster nodes

3. **Streaming Replication** (`internal/cluster/replication/`)
   - WAL-based replication from primary to replicas
   - Asynchronous streaming with configurable batching
   - Automatic reconnection and lag monitoring

4. **Failover Manager** (`internal/cluster/failover/`)
   - Health monitoring of cluster nodes
   - Automatic role transitions (Primary/Replica/Standby)
   - Configurable failover timeouts and intervals

5. **Cluster API** (`internal/cluster/api.go`)
   - HTTP REST API for cluster monitoring
   - Real-time status and health information
   - Node discovery and role information

### Network Ports

QuantaDB uses multiple ports for different cluster functions:
- **SQL Port** (default 5432): PostgreSQL wire protocol
- **Replication Port** (SQL+1000): WAL streaming between nodes
- **Raft Port** (SQL+2000): Raft consensus protocol
- **API Port** (SQL+3000): HTTP management API

## Quick Start

### Starting a Primary Node

```bash
./build/quantadb \
  --cluster-mode primary \
  --node-id node1 \
  --data ./data/primary \
  --port 5432
```

### Starting a Replica Node

```bash
./build/quantadb \
  --cluster-mode replica \
  --node-id node2 \
  --data ./data/replica \
  --port 5433 \
  --primary localhost:6432
```

### Using Configuration Files

Create a primary configuration (`primary.json`):
```json
{
  "host": "localhost",
  "port": 5432,
  "data_dir": "./data/primary",
  "log_level": "info",
  "cluster": {
    "node_id": "node1",
    "mode": "primary",
    "data_dir": "./data/primary/cluster"
  }
}
```

Create a replica configuration (`replica.json`):
```json
{
  "host": "localhost", 
  "port": 5433,
  "data_dir": "./data/replica",
  "log_level": "info",
  "cluster": {
    "node_id": "node2",
    "mode": "replica",
    "data_dir": "./data/replica/cluster",
    "replication": {
      "primary_address": "localhost:6432"
    }
  }
}
```

Start nodes using configurations:
```bash
./build/quantadb --config primary.json
./build/quantadb --config replica.json
```

### Helper Script

Use the provided script to quickly start a test cluster:
```bash
./scripts/start-cluster.sh
```

This script:
1. Cleans up previous test data
2. Builds the server
3. Starts a primary on port 5432
4. Starts a replica on port 5433
5. Shows connection information

## Cluster Management

### Monitoring Cluster Status

The cluster provides an HTTP API for monitoring (on port+3000):

**Get cluster status:**
```bash
curl http://localhost:8432/cluster/status
```

Response:
```json
{
  "node": {
    "id": "node1",
    "raft_address": "localhost:7432",
    "replication_port": 6432,
    "role": "PRIMARY",
    "start_time": "2024-01-26T10:00:00Z",
    "uptime_seconds": 3600
  },
  "raft": {
    "term": 1,
    "state": "LEADER",
    "leader": "node1"
  },
  "replication": {
    "mode": "PRIMARY",
    "node_id": "node1",
    "address": ":6432",
    "state": "ACTIVE",
    "last_lsn": 12345,
    "replica_count": 1
  },
  "failover": {
    "current_role": "PRIMARY",
    "is_healthy": true,
    "last_failover": "0001-01-01T00:00:00Z",
    "failover_in_progress": false
  },
  "cluster": {
    "is_healthy": true
  }
}
```

**List all nodes:**
```bash
curl http://localhost:8432/cluster/nodes
```

**Health check:**
```bash
curl http://localhost:8432/cluster/health
```

### Read-Only Enforcement

Replica nodes automatically reject write queries:

```sql
-- On primary (port 5432)
INSERT INTO users VALUES (1, 'Alice');  -- Success

-- On replica (port 5433)
INSERT INTO users VALUES (2, 'Bob');     -- Error: cannot execute write queries on replica node
SELECT * FROM users;                     -- Success (read allowed)
```

## Configuration Reference

### Cluster Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `node_id` | Unique identifier for the node | Required |
| `mode` | Node mode: "none", "primary", "replica" | "none" |
| `data_dir` | Directory for cluster metadata | "./cluster" |

### Replication Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `primary_address` | Primary node address (replicas only) | Required for replicas |
| `stream_buffer_size` | Buffer size for WAL streaming | 1MB |
| `batch_size` | Records per replication batch | 100 |
| `flush_interval` | Batch flush interval | 100ms |
| `heartbeat_interval` | Heartbeat between nodes | 10s |
| `heartbeat_timeout` | Timeout for heartbeat response | 30s |
| `max_lag_bytes` | Maximum replication lag in bytes | 16MB |
| `max_lag_time` | Maximum replication lag in time | 5m |
| `connect_timeout` | Connection timeout | 30s |
| `reconnect_interval` | Retry interval after disconnect | 5s |
| `max_reconnect_tries` | Maximum reconnection attempts | 10 |

### Raft Configuration (Internal)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `election_timeout` | Raft election timeout | 150ms |
| `heartbeat_interval` | Raft heartbeat interval | 50ms |
| `max_log_entries` | Maximum log entries before snapshot | 100 |

## Current Limitations

While production-ready, the following features are planned for future releases:

1. **Inter-node TLS**: Cluster communication encryption (planned)
2. **Horizontal Sharding**: Automatic data partitioning (planned)
3. **Cross-region replication**: Advanced geographic distribution (planned)
4. **Advanced RBAC**: Fine-grained permission system (planned)

## Troubleshooting

### Common Issues

**Replica can't connect to primary:**
- Check primary is running and replication port (6432) is accessible
- Verify `primary_address` in replica configuration
- Check firewall rules

**Nodes not forming cluster:**
- Ensure node IDs are unique
- Check Raft ports (7432, 7433) are not blocked
- Review logs for connection errors

**Read-only errors on primary:**
- Node may have lost leadership
- Check cluster status via API
- Verify network connectivity between nodes

### Debug Logging

Enable debug logging for more details:
```bash
./build/quantadb --log-level debug --cluster-mode primary --node-id node1
```

### Log Locations

Cluster components log to stdout with prefixes:
- `[raft]` - Raft consensus messages
- `[replication]` - Replication stream messages
- `[failover]` - Failover manager messages
- `[cluster]` - Coordinator messages

## Production Features Completed

✅ **Enterprise-Grade Capabilities:**

1. **Advanced Replication** (Production-Ready)
   - Synchronous replication with multiple consistency modes
   - Configurable timeout handling for slow replicas
   - Point-in-time recovery with cluster coordination

2. **Enhanced Operations** (Production-Ready)
   - Online node addition/removal with safety checks
   - Comprehensive cluster monitoring and alerting
   - Distributed backup coordination and verification

3. **Intelligent Query Routing** (Production-Ready)
   - Load balancing with multiple strategies (Round Robin, Least Connections, Weighted, Health-Aware)
   - Circuit breaker pattern for failed node detection
   - Health-aware routing with real-time node status

4. **Split-Brain Prevention** (Production-Ready)
   - Witness node coordination for even-numbered clusters
   - Network partition detection with automatic service degradation
   - Enhanced failover safety mechanisms

## Testing

Run integration tests (requires Go):
```bash
go test -tags=integration ./internal/cluster/... -v
```

The tests cover:
- Single node cluster formation
- Multi-node replication setup
- API endpoint functionality
- Role transitions

## Contributing

When contributing to cluster features:

1. Add tests for new functionality
2. Update this documentation
3. Follow existing patterns in `internal/cluster/`
4. Test with multiple nodes before submitting

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for general guidelines.