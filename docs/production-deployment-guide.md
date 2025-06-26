# QuantaDB Production Deployment Guide

## Overview

This guide covers production deployment of QuantaDB clusters with enterprise-grade features including synchronous replication, automated failover, and comprehensive monitoring.

## Deployment Architecture

### Recommended Topologies

#### High Availability (3-Node Cluster)
```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Primary    │    │   Replica 1  │    │   Replica 2  │
│   Node 1     │───►│   Node 2     │    │   Node 3     │
│              │    │              │    │              │
│ Read/Write   │    │ Read Only    │    │ Read Only    │
└──────────────┘    └──────────────┘    └──────────────┘
```

**Benefits:**
- Survives 1 node failure
- Automatic failover in 30 seconds
- Read scaling across replicas
- Zero data loss with synchronous replication

#### Read Scaling (1+N Configuration)
```
              ┌──────────────┐
              │   Primary    │
              │   Node 1     │
              │              │
              │ Read/Write   │
              └──────┬───────┘
                     │
       ┌─────────────┼─────────────┐
       │             │             │
┌──────▼────┐ ┌──────▼────┐ ┌──────▼────┐
│ Replica 1 │ │ Replica 2 │ │ Replica N │
│ Read Only │ │ Read Only │ │ Read Only │
└───────────┘ └───────────┘ └───────────┘
```

**Benefits:**
- Linear read scaling
- Dedicated analytics replicas
- Geographic distribution

### Hardware Requirements

#### Minimum Production Setup
- **CPU**: 4 cores per node
- **RAM**: 8GB per node (4GB+ for buffer pool)
- **Storage**: SSD with 1000+ IOPS
- **Network**: 1Gbps between nodes

#### Recommended Production Setup
- **CPU**: 8+ cores per node
- **RAM**: 16GB+ per node (8GB+ for buffer pool)
- **Storage**: NVMe SSD with 10k+ IOPS
- **Network**: 10Gbps between nodes

## Configuration

### Primary Node Configuration

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 5432,
    "data_dir": "/var/lib/quantadb",
    "wal_dir": "/var/lib/quantadb/wal"
  },
  "cluster": {
    "mode": "primary",
    "node_id": "primary-01",
    "raft_port": 7432,
    "replication_port": 6432,
    "api_port": 8432
  },
  "replication": {
    "mode": "sync",
    "max_replicas": 2,
    "sync_timeout": "5s",
    "batch_size": 1000
  },
  "storage": {
    "buffer_pool_size": "8GB",
    "wal_segment_size": "16MB",
    "checkpoint_interval": "5m"
  }
}
```

### Replica Node Configuration

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 5432,
    "data_dir": "/var/lib/quantadb",
    "wal_dir": "/var/lib/quantadb/wal"
  },
  "cluster": {
    "mode": "replica",
    "node_id": "replica-01",
    "primary_host": "primary-01.example.com",
    "primary_replication_port": 6432,
    "raft_port": 7432,
    "api_port": 8432
  },
  "storage": {
    "buffer_pool_size": "8GB",
    "wal_segment_size": "16MB"
  }
}
```

## Network Configuration

### Port Requirements

| Port | Protocol | Purpose | Internal | External |
|------|----------|---------|----------|----------|
| 5432 | TCP | PostgreSQL SQL | Yes | Yes |
| 6432 | TCP | WAL Replication | Yes | No |
| 7432 | TCP | Raft Consensus | Yes | No |
| 8432 | HTTP | Management API | Yes | Limited |

### Firewall Rules

```bash
# SQL traffic (external)
iptables -A INPUT -p tcp --dport 5432 -j ACCEPT

# Cluster internal traffic (between nodes only)
iptables -A INPUT -p tcp --dport 6432 -s cluster_network/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 7432 -s cluster_network/24 -j ACCEPT

# Management API (restricted)
iptables -A INPUT -p tcp --dport 8432 -s management_network/24 -j ACCEPT
```

## Security Configuration

### TLS Encryption

```json
{
  "tls": {
    "enabled": true,
    "cert_file": "/etc/quantadb/certs/server.crt",
    "key_file": "/etc/quantadb/certs/server.key",
    "ca_file": "/etc/quantadb/certs/ca.crt",
    "require_client_auth": true
  }
}
```

### Authentication

```json
{
  "auth": {
    "method": "scram-sha-256",
    "password_encryption": "scram-sha-256",
    "require_ssl": true
  }
}
```

## Monitoring and Alerting

### Health Check Endpoints

```bash
# Node health
curl http://node:8432/health

# Cluster status
curl http://node:8432/cluster/status

# Replication lag
curl http://node:8432/cluster/replication
```

### Prometheus Metrics

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'quantadb'
    static_configs:
      - targets: ['node1:8432', 'node2:8432', 'node3:8432']
    metrics_path: /metrics
    scrape_interval: 15s
```

### Key Metrics to Monitor

#### Cluster Health
- `quantadb_cluster_nodes_total` - Number of cluster nodes
- `quantadb_cluster_leader_elections_total` - Raft leadership changes
- `quantadb_cluster_network_partitions_total` - Network partition events

#### Replication
- `quantadb_replication_lag_bytes` - Replication lag in bytes
- `quantadb_replication_lag_seconds` - Replication lag in time
- `quantadb_replication_connections_active` - Active replication connections

#### Performance
- `quantadb_queries_total` - Total queries executed
- `quantadb_query_duration_seconds` - Query execution time
- `quantadb_storage_buffer_hit_ratio` - Buffer pool hit ratio

### Alert Rules

```yaml
# alerts.yml
groups:
  - name: quantadb
    rules:
      - alert: QuantaDBNodeDown
        expr: up{job="quantadb"} == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "QuantaDB node is down"

      - alert: QuantaDBReplicationLag
        expr: quantadb_replication_lag_seconds > 10
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "High replication lag detected"

      - alert: QuantaDBHighQueryLatency
        expr: rate(quantadb_query_duration_seconds_sum[5m]) / rate(quantadb_query_duration_seconds_count[5m]) > 1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High query latency detected"
```

## Backup and Recovery

### Automated Backup Configuration

```json
{
  "backup": {
    "enabled": true,
    "schedule": "0 2 * * *",
    "retention_days": 30,
    "storage": {
      "type": "s3",
      "bucket": "quantadb-backups",
      "region": "us-west-2",
      "encryption": true
    },
    "compression": "gzip",
    "verify_after_backup": true
  }
}
```

### Point-in-Time Recovery

```bash
# Restore to specific timestamp
quantactl restore \
  --backup-location s3://quantadb-backups/cluster-01 \
  --target-time "2024-01-15 14:30:00" \
  --target-cluster cluster-02
```

## Load Balancing

### Application-Level Load Balancing

```go
// Connection configuration for applications
config := &pgxpool.Config{
    ConnConfig: &pgx.ConnConfig{
        Host:     "primary-01.example.com",
        Port:     5432,
        Database: "myapp",
        User:     "app_user",
    },
    MaxConns: 20,
}

// Read replica configuration
readConfig := &pgxpool.Config{
    ConnConfig: &pgx.ConnConfig{
        Host:     "replica-01.example.com,replica-02.example.com",
        Port:     5432,
        Database: "myapp",
        User:     "app_user",
    },
    MaxConns: 30,
}
```

### HAProxy Configuration

```
global
    daemon
    log stdout local0

defaults
    mode tcp
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms

frontend quantadb_write
    bind *:5432
    default_backend quantadb_primary

frontend quantadb_read
    bind *:5433
    default_backend quantadb_replicas

backend quantadb_primary
    server primary primary-01.example.com:5432 check

backend quantadb_replicas
    balance roundrobin
    server replica1 replica-01.example.com:5432 check
    server replica2 replica-02.example.com:5432 check
```

## Deployment Steps

### 1. Infrastructure Setup

```bash
# Create data directories
sudo mkdir -p /var/lib/quantadb/{data,wal,backups}
sudo chown quantadb:quantadb /var/lib/quantadb -R

# Create log directory
sudo mkdir -p /var/log/quantadb
sudo chown quantadb:quantadb /var/log/quantadb
```

### 2. Primary Node Deployment

```bash
# Start primary node
sudo systemctl enable quantadb
sudo systemctl start quantadb

# Verify startup
sudo systemctl status quantadb
curl http://localhost:8432/health
```

### 3. Replica Node Deployment

```bash
# Configure replica
sudo cp replica-config.json /etc/quantadb/config.json

# Start replica
sudo systemctl start quantadb

# Verify replication
curl http://localhost:8432/cluster/status
```

### 4. Verification

```bash
# Test write on primary
psql -h primary-01 -c "CREATE TABLE test (id int);"

# Verify read on replica
psql -h replica-01 -c "SELECT * FROM test;"

# Test failover
sudo systemctl stop quantadb  # On primary
# Verify automatic promotion of replica
```

## Maintenance

### Rolling Updates

```bash
# Update process (zero downtime)
1. Update replica nodes one by one
2. Promote a replica to primary
3. Update the old primary as a replica
4. Promote back if desired
```

### Adding New Replicas

```bash
# Add replica to running cluster
quantactl cluster add-node \
  --node-id new-replica \
  --host new-replica.example.com \
  --role replica
```

### Removing Nodes

```bash
# Graceful node removal
quantactl cluster remove-node --node-id replica-02 --graceful
```

## Troubleshooting

### Common Issues

#### Split-Brain Detection
```bash
# Check for split-brain
curl http://node:8432/cluster/partitions

# Manual recovery (if needed)
quantactl cluster heal-partition --force-leader node1
```

#### Replication Lag
```bash
# Check replication status
curl http://primary:8432/cluster/replication

# Restart replication if needed
quantactl replication restart --node replica-01
```

#### Performance Issues
```bash
# Check buffer pool hit ratio
curl http://node:8432/metrics | grep buffer_hit_ratio

# Analyze slow queries
psql -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10;"
```

## Performance Tuning

### Buffer Pool Sizing
```json
{
  "storage": {
    "buffer_pool_size": "75% of available RAM",
    "buffer_pool_policy": "lru"
  }
}
```

### WAL Configuration
```json
{
  "wal": {
    "segment_size": "16MB",
    "sync_method": "fsync",
    "checkpoint_completion_target": 0.9
  }
}
```

### Query Optimization
```json
{
  "query": {
    "vectorized_execution": true,
    "parallel_workers": 4,
    "result_cache_size": "1GB"
  }
}
```

This production deployment guide provides comprehensive coverage for deploying QuantaDB clusters in enterprise environments with high availability, monitoring, and security considerations.