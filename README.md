# TinyDB

A lightweight C/S architecture relational database system developed in C++17.

## Features

- **SQL Support**: Complete SQL parsing and execution

- **ACID Transactions**: WAL logging ensures data consistency

- **B+ Tree Index**: High-performance index for query acceleration

- **Query Optimization**: Cost-based query optimizer

- **Permission System**: Users, roles, and fine-grained access control

- **Backup & Recovery**: Full/incremental backup with PITR support

- **Master-Slave Replication**: WAL streaming replication architecture

- **Monitoring & Diagnostics**: tn_stat_* style system views

- **Command-line Tools**: psql-like interactive client

## Quick Start

### Build

```bash
mkdir build && cd build
cmake ..
make -j4
```

### Start Server

```bash
./bin/tinydb-server -p 5432
```

### Use Command-line Client

```bash
# Interactive mode
./bin/tinydb-cli -h localhost -p 5432

# Execute single SQL
./bin/tinydb-cli -c "SELECT * FROM users"

# Execute SQL script
./bin/tinydb-cli -f script.sql
```

### Sample SQL

```sql
-- Create table
CREATE TABLE users (id INT, name VARCHAR(32), age INT);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);

-- Create index
CREATE INDEX idx_users_id ON users(id);

-- Query
SELECT * FROM users WHERE age > 25;

-- Update
UPDATE users SET age = 26 WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 2;

-- Transaction
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 35);
COMMIT;
```

## System Views

```sql
-- View current activity
SELECT * FROM tn_stat_activity;

-- View table statistics
SELECT * FROM tn_stat_user_tables;

-- View index usage
SELECT * FROM tn_stat_user_indexes;

-- View lock information
SELECT * FROM tn_locks;

-- View replication status
SELECT * FROM tn_stat_replication;

-- View backup list
SELECT * FROM tn_backup_list;
```

## Testing

```bash
# Run all tests
ctest

# Run specific tests
./tests/test_phase5_integration
./tests/test_btree_index
./tests/test_statistics
```

## Tech Stack

- **Language**: C++17

- **Build**: CMake 3.14+

- **Testing**: Google Test

- **Protocol**: Custom binary protocol

- **Port**: 5432



