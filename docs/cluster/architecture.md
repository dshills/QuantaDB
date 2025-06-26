# QuantaDB Cluster Architecture

## Overview

QuantaDB's distributed architecture is designed for high availability, scalability, and consistency. The system uses a combination of Raft consensus, WAL-based replication, and automatic failover to provide a robust distributed database platform.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Client Applications                         │
│                    (psql, pgAdmin, custom apps)                     │
└─────────────┬───────────────────────────────────────┬───────────────┘
              │                                       │
              ▼                                       ▼
┌─────────────────────────┐             ┌─────────────────────────┐
│     Primary Node        │             │     Replica Node        │
│  ┌─────────────────┐    │             │  ┌─────────────────┐    │
│  │ SQL Engine      │    │             │  │ SQL Engine      │    │
│  │ (Read/Write)    │    │             │  │ (Read Only)     │    │
│  └────────┬────────┘    │             │  └────────┬────────┘    │
│           │              │             │           │              │
│  ┌────────▼────────┐    │             │  ┌────────▼────────┐    │
│  │ Cluster         │◄───┼─────────────┼──┤ Cluster         │    │
│  │ Coordinator     │    │    Raft     │  │ Coordinator     │    │
│  └────────┬────────┘    │  Protocol   │  └────────┬────────┘    │
│           │              │             │           │              │
│  ┌────────▼────────┐    │             │  ┌────────▼────────┐    │
│  │ WAL Manager     │────┼─────────────┼─►│ WAL Receiver    │    │
│  │                 │    │ Replication │  │                 │    │
│  └────────┬────────┘    │   Stream    │  └────────┬────────┘    │
│           │              │             │           │              │
│  ┌────────▼────────┐    │             │  ┌────────▼────────┐    │
│  │ Storage Engine  │    │             │  │ Storage Engine  │    │
│  │ (Buffer Pool)   │    │             │  │ (Buffer Pool)   │    │
│  └─────────────────┘    │             │  └─────────────────┘    │
└─────────────────────────┘             └─────────────────────────┘
```

## Component Details

### 1. Cluster Coordinator

The central component that orchestrates all distributed operations.

```go
type Coordinator struct {
    raftNode           RaftNode
    replicationManager ReplicationManager  
    failoverManager    FailoverManager
    walManager         *wal.Manager
}
```

**Responsibilities:**
- Initialize and manage Raft consensus
- Coordinate replication streams
- Handle failover decisions
- Provide cluster status information

### 2. Raft Consensus Layer

Implements the Raft consensus algorithm for distributed coordination.

```
┌─────────────────────────────────────────┐
│            Raft State Machine           │
├─────────────────┬───────────────────────┤
│   Log Entries   │   Persistent State    │
├─────────────────┼───────────────────────┤
│ Entry 1: Op A   │ Current Term: 3       │
│ Entry 2: Op B   │ Voted For: Node2      │
│ Entry 3: Op C   │ Log Index: 125        │
└─────────────────┴───────────────────────┘
```

**Key Components:**
- **Leader Election**: Ensures single leader for consistency
- **Log Replication**: Distributes operations across nodes
- **Safety**: Guarantees no split-brain scenarios
- **Membership Changes**: Dynamic node addition/removal

### 3. WAL Streaming Replication

Asynchronous replication of Write-Ahead Log entries.

```
Primary Node                           Replica Node
┌─────────────┐                       ┌─────────────┐
│ WAL Writer  │                       │ WAL Receiver│
│             │                       │             │
│ ┌─────────┐ │    WAL Stream        │ ┌─────────┐ │
│ │Record 1 │ ├──────────────────────►│ │Record 1 │ │
│ │Record 2 │ │    (Batched)         │ │Record 2 │ │
│ │Record 3 │ │                       │ │Record 3 │ │
│ └─────────┘ │                       │ └─────────┘ │
└─────────────┘                       └─────────────┘
```

**Features:**
- Batched streaming for efficiency
- Automatic reconnection on failure
- Lag monitoring (bytes and time)
- Configurable buffer sizes

### 4. Failover Manager

Monitors health and manages role transitions.

```
State Transitions:
┌─────────┐     Leader      ┌─────────┐
│Follower ├────────────────►│ Primary │
└────┬────┘    Election     └────┬────┘
     │                            │
     │         Health            │
     │         Check             │
     │         Failed            │
     │                           │
     ▼                           ▼
┌─────────┐                 ┌─────────┐
│ Standby │◄────────────────┤ Replica │
└─────────┘   Demotion      └─────────┘
```

**Capabilities:**
- Health monitoring with configurable intervals
- Automatic promotion on primary failure
- Prevention of failover thrashing
- Integration with Raft for consistency

### 5. Network Architecture

Multiple network protocols for different functions:

```
Node A                                  Node B
┌────────────────────────┐     ┌────────────────────────┐
│ :5432 - SQL Protocol   │     │ :5432 - SQL Protocol   │
│ :6432 - Replication    │◄───►│ :6432 - Replication    │
│ :7432 - Raft RPC       │◄───►│ :7432 - Raft RPC       │
│ :8432 - HTTP API       │     │ :8432 - HTTP API       │
└────────────────────────┘     └────────────────────────┘
```

### 6. Query Routing

Read/write separation at the network layer:

```go
func (c *Connection) checkQueryPermissions(query string, stmt Statement) error {
    role := c.clusterCoordinator.GetRole()
    
    if role == RolePrimary {
        return nil // All queries allowed
    }
    
    if role == RoleReplica && isReadOnlyQuery(stmt) {
        return nil // Read queries allowed
    }
    
    return ErrReadOnlyNode
}
```

## Data Flow

### Write Path (Primary Only)

1. Client sends write query to primary
2. SQL engine processes query
3. Changes written to WAL
4. WAL streamed to replicas
5. Storage engine updates pages
6. Client receives confirmation

### Read Path (Any Node)

1. Client sends read query
2. Query permission check
3. SQL engine processes query
4. Data read from buffer pool/disk
5. Results returned to client

### Replication Flow

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│  Write  │───►│   WAL   │───►│ Stream  │───►│ Apply   │
│ Operation│    │ Record  │    │ to Replica│   │ at Replica│
└─────────┘    └─────────┘    └─────────┘    └─────────┘
     │              │               │              │
     ▼              ▼               ▼              ▼
  Primary      Primary WAL    Network Transfer  Replica Storage
```

## Consistency Model

### Strong Consistency (Within Primary)
- All writes go through single primary
- MVCC ensures read consistency
- No dirty reads or write conflicts

### Eventual Consistency (Replicas)
- Asynchronous replication lag
- Monotonic read consistency per connection
- Configurable maximum lag thresholds

### Consensus Consistency (Cluster State)
- Raft ensures single leader
- Majority agreement for state changes
- No split-brain scenarios

## Failure Scenarios

### Primary Failure

1. **Detection**: Replicas detect missing heartbeats
2. **Election**: Raft election selects new leader
3. **Promotion**: Winning replica becomes primary
4. **Notification**: Clients notified of new primary

### Replica Failure

1. **Detection**: Primary detects disconnection
2. **Removal**: Failed replica removed from stream
3. **Recovery**: Replica can rejoin when healthy
4. **Catch-up**: Resync from last known position

### Network Partition

```
Before Partition:          During Partition:
┌───┬───┬───┐             ┌───┬───┐ │ ┌───┐
│ A │ B │ C │             │ A │ B │ │ │ C │
└───┴───┴───┘             └───┴───┘ │ └───┘
    Primary                Primary   │  Follower
                          (Majority) │ (No Writes)
```

- Majority partition maintains service
- Minority partition becomes read-only
- Automatic reconciliation on heal

## Performance Considerations

### Replication Lag

Factors affecting lag:
- Network bandwidth and latency
- Write volume on primary
- Replica processing capacity
- Batch size and flush interval

### Resource Usage

Per-node overhead:
- Raft: ~10MB RAM + log storage
- Replication: Buffer size (default 1MB)
- HTTP API: Minimal (~1MB)
- Network: 4 ports per node

### Scalability Limits

Current implementation:
- Recommended: 3-5 nodes
- Maximum tested: 7 nodes
- Replicas: Unlimited (fan-out)
- Not suitable for: 100+ nodes

## Security Considerations

⚠️ **Current Limitations:**
- No encryption in transit
- No authentication between nodes
- HTTP API unsecured
- Trust-based cluster membership

**Future Enhancements:**
- TLS for all protocols
- Mutual TLS authentication
- API authentication tokens
- Encrypted storage

## Monitoring and Observability

### Cluster API Endpoints

```bash
GET /cluster/status      # Comprehensive status
GET /cluster/nodes       # Node listing
GET /cluster/health      # Health check
```

### Key Metrics

- **Raft**: Term, state, last log index
- **Replication**: Lag bytes/time, connection state
- **Failover**: Role, health status, last transition
- **Performance**: Queries/sec, replication throughput

### Log Analysis

Important log patterns:
```
[raft] Transitioning to LEADER state
[replication] Stream connected to primary
[failover] Promoting to PRIMARY role
[cluster] Health check failed for node X
```

## Deployment Patterns

### High Availability (3 Nodes)
- 1 Primary + 2 Replicas
- Survives 1 node failure
- Automatic failover

### Read Scaling (1+N)
- 1 Primary + N Read Replicas
- Linear read scaling
- Manual failover only

### Geographic Distribution
- Primary in Region A
- Replicas in Regions B, C
- Higher latency tolerance needed

## Future Architecture Plans

1. **Sharding Support**
   - Consistent hashing
   - Cross-shard transactions
   - Query routing layer

2. **Multi-Master**
   - Conflict resolution
   - Vector clocks
   - CRDTs for certain operations

3. **Cloud Native**
   - Kubernetes operators
   - Cloud storage backends
   - Serverless mode