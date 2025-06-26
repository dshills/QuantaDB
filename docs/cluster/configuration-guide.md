# QuantaDB Cluster Configuration Guide

This guide provides detailed configuration examples for various cluster deployment scenarios.

## Table of Contents
1. [Basic Two-Node Cluster](#basic-two-node-cluster)
2. [Three-Node High Availability](#three-node-high-availability)
3. [Read Replica Scaling](#read-replica-scaling)
4. [Custom Network Configuration](#custom-network-configuration)
5. [Performance Tuning](#performance-tuning)

## Basic Two-Node Cluster

The simplest cluster setup with one primary and one replica.

### Primary Configuration
```json
{
  "host": "0.0.0.0",
  "port": 5432,
  "data_dir": "/var/lib/quantadb/primary",
  "log_level": "info",
  "network": {
    "max_connections": 100,
    "connection_timeout": 30
  },
  "storage": {
    "buffer_pool_size": 256,
    "page_size": 8192
  },
  "wal": {
    "enabled": true,
    "directory": "wal",
    "segment_size": 16777216
  },
  "cluster": {
    "node_id": "primary-1",
    "mode": "primary",
    "data_dir": "/var/lib/quantadb/primary/cluster",
    "replication": {
      "stream_buffer_size": 1048576,
      "batch_size": 100,
      "flush_interval": 100,
      "heartbeat_interval": 10,
      "heartbeat_timeout": 30
    }
  }
}
```

### Replica Configuration
```json
{
  "host": "0.0.0.0",
  "port": 5433,
  "data_dir": "/var/lib/quantadb/replica",
  "log_level": "info",
  "network": {
    "max_connections": 200,
    "connection_timeout": 30
  },
  "storage": {
    "buffer_pool_size": 256,
    "page_size": 8192
  },
  "wal": {
    "enabled": true,
    "directory": "wal",
    "segment_size": 16777216
  },
  "cluster": {
    "node_id": "replica-1",
    "mode": "replica",
    "data_dir": "/var/lib/quantadb/replica/cluster",
    "replication": {
      "primary_address": "primary-host:6432",
      "stream_buffer_size": 1048576,
      "batch_size": 100,
      "flush_interval": 100,
      "heartbeat_interval": 10,
      "heartbeat_timeout": 30,
      "max_lag_bytes": 33554432,
      "max_lag_time": 300,
      "connect_timeout": 30,
      "reconnect_interval": 5,
      "max_reconnect_tries": 10
    }
  }
}
```

## Three-Node High Availability

A production-ready setup with automatic failover capability.

### Node 1 (Initial Primary)
```json
{
  "host": "node1.example.com",
  "port": 5432,
  "data_dir": "/data/quantadb/node1",
  "log_level": "info",
  "cluster": {
    "node_id": "node1",
    "mode": "primary",
    "data_dir": "/data/quantadb/node1/cluster"
  }
}
```

### Node 2 (Standby)
```json
{
  "host": "node2.example.com",
  "port": 5432,
  "data_dir": "/data/quantadb/node2",
  "log_level": "info",
  "cluster": {
    "node_id": "node2",
    "mode": "replica",
    "data_dir": "/data/quantadb/node2/cluster",
    "replication": {
      "primary_address": "node1.example.com:6432"
    }
  }
}
```

### Node 3 (Standby)
```json
{
  "host": "node3.example.com",
  "port": 5432,
  "data_dir": "/data/quantadb/node3",
  "log_level": "info",
  "cluster": {
    "node_id": "node3",
    "mode": "replica",
    "data_dir": "/data/quantadb/node3/cluster",
    "replication": {
      "primary_address": "node1.example.com:6432"
    }
  }
}
```

## Read Replica Scaling

Configuration for a primary with multiple read replicas for load distribution.

### Primary (Write Node)
```json
{
  "host": "write.example.com",
  "port": 5432,
  "data_dir": "/data/primary",
  "cluster": {
    "node_id": "write-primary",
    "mode": "primary",
    "replication": {
      "batch_size": 200,
      "flush_interval": 50
    }
  }
}
```

### Read Replica Template
```json
{
  "host": "read-${REPLICA_ID}.example.com",
  "port": 5432,
  "data_dir": "/data/replica-${REPLICA_ID}",
  "network": {
    "max_connections": 500
  },
  "storage": {
    "buffer_pool_size": 512
  },
  "cluster": {
    "node_id": "read-replica-${REPLICA_ID}",
    "mode": "replica",
    "replication": {
      "primary_address": "write.example.com:6432",
      "max_lag_bytes": 67108864,
      "max_lag_time": 600
    }
  }
}
```

## Custom Network Configuration

For deployments with specific network requirements.

### Multi-Interface Setup
```json
{
  "host": "10.0.1.100",
  "port": 5432,
  "cluster": {
    "node_id": "multi-if-node",
    "mode": "primary",
    "peers": [
      {
        "node_id": "peer1",
        "raft_address": "10.0.2.101:7432",
        "replication_address": "10.0.2.101:6432"
      },
      {
        "node_id": "peer2", 
        "raft_address": "10.0.2.102:7432",
        "replication_address": "10.0.2.102:6432"
      }
    ]
  }
}
```

### Behind Load Balancer
```json
{
  "host": "0.0.0.0",
  "port": 5432,
  "network": {
    "max_connections": 1000,
    "connection_timeout": 60,
    "read_buffer_size": 16384,
    "write_buffer_size": 16384
  },
  "cluster": {
    "node_id": "lb-node-1",
    "mode": "replica",
    "replication": {
      "primary_address": "internal-primary.local:6432",
      "connect_timeout": 60,
      "heartbeat_timeout": 60
    }
  }
}
```

## Performance Tuning

### High-Write Primary
```json
{
  "cluster": {
    "node_id": "write-optimized",
    "mode": "primary",
    "replication": {
      "stream_buffer_size": 4194304,
      "batch_size": 500,
      "flush_interval": 200,
      "heartbeat_interval": 30
    }
  },
  "wal": {
    "segment_size": 67108864,
    "retention_duration": "6h"
  },
  "storage": {
    "buffer_pool_size": 1024
  }
}
```

### Low-Latency Replica
```json
{
  "cluster": {
    "node_id": "low-latency-replica",
    "mode": "replica",
    "replication": {
      "stream_buffer_size": 524288,
      "batch_size": 50,
      "flush_interval": 10,
      "heartbeat_interval": 5,
      "max_lag_bytes": 8388608,
      "max_lag_time": 60,
      "reconnect_interval": 1
    }
  }
}
```

### Analytics Replica
```json
{
  "cluster": {
    "node_id": "analytics-replica",
    "mode": "replica",
    "replication": {
      "batch_size": 1000,
      "flush_interval": 1000,
      "max_lag_bytes": 268435456,
      "max_lag_time": 3600
    }
  },
  "storage": {
    "buffer_pool_size": 2048
  },
  "network": {
    "max_connections": 50
  }
}
```

## Environment-Specific Examples

### Docker Compose
```yaml
version: '3.8'

services:
  primary:
    image: quantadb:latest
    volumes:
      - ./configs/primary.json:/etc/quantadb/config.json
      - primary-data:/var/lib/quantadb
    ports:
      - "5432:5432"
      - "8432:8432"
    command: ["--config", "/etc/quantadb/config.json"]

  replica:
    image: quantadb:latest
    volumes:
      - ./configs/replica.json:/etc/quantadb/config.json
      - replica-data:/var/lib/quantadb
    ports:
      - "5433:5433"
      - "8433:8433"
    command: ["--config", "/etc/quantadb/config.json"]
    depends_on:
      - primary

volumes:
  primary-data:
  replica-data:
```

### Kubernetes ConfigMap
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: quantadb-config
data:
  primary.json: |
    {
      "host": "0.0.0.0",
      "port": 5432,
      "cluster": {
        "node_id": "${POD_NAME}",
        "mode": "primary",
        "data_dir": "/data/cluster"
      }
    }
  
  replica.json: |
    {
      "host": "0.0.0.0", 
      "port": 5432,
      "cluster": {
        "node_id": "${POD_NAME}",
        "mode": "replica",
        "replication": {
          "primary_address": "quantadb-primary:6432"
        }
      }
    }
```

## Configuration Best Practices

1. **Node IDs**: Use descriptive, unique identifiers
2. **Data Directories**: Use separate directories for each node
3. **Buffer Pool**: Set to 25-40% of available RAM
4. **Replication Lag**: Set based on business requirements
5. **Heartbeat**: Adjust based on network latency
6. **Connections**: Primary needs fewer connections than replicas

## Monitoring Configuration

Enable detailed metrics:
```json
{
  "log_level": "debug",
  "cluster": {
    "node_id": "monitored-node",
    "mode": "replica",
    "replication": {
      "heartbeat_interval": 5,
      "heartbeat_timeout": 15
    }
  }
}
```

Check configuration via API:
```bash
curl http://localhost:8432/cluster/status | jq .
```