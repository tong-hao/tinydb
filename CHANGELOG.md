# Changelog

All notable changes to TinyDB will be documented in this file.

## [1.0.0] - 2024-03-20

### Phase 5: Enterprise Features

#### Added
- **Permission System**
  - User management (CREATE/DROP USER)
  - Role management with inheritance
  - GRANT/REVOKE permissions
  - Column-level permissions
  - Permission checking for all operations

- **Backup and Recovery**
  - Full backup support
  - Incremental backup support
  - Point-in-Time Recovery (PITR)
  - Restore points management
  - WAL archiving
  - Backup verification

- **Replication**
  - Master-Slave replication
  - WAL streaming protocol
  - Replication slots
  - Hot standby mode
  - Failover support
  - Replication lag monitoring

- **Row-Level Locking**
  - Shared/Exclusive row locks
  - Predicate locking for Serializable isolation
  - Lock compatibility matrix
  - Lock upgrade support

- **Monitoring and Diagnostics**
  - tn_stat_activity view
  - tn_stat_database view
  - tn_stat_user_tables view
  - tn_stat_user_indexes view
  - tn_locks view
  - tn_stat_replication view
  - Table/index statistics
  - Buffer statistics
  - WAL statistics

- **Command Line Client (tinydb-cli)**
  - Interactive REPL mode
  - Meta commands (\dt, \d, \du, \l, etc.)
  - Multi-line SQL input
  - Formatted table output
  - Extended output mode
  - Command history support

- **Import/Export Tools**
  - CSV format support
  - JSON format support
  - SQL INSERT format
  - Binary format support
  - Markdown table export
  - Bulk import

- **System Views**
  - tn_tables view
  - tn_indexes view
  - tn_user view
  - tn_database view
  - tn_backup_list view
  - tn_restore_points view

## [0.4.0] - 2024-03-19

### Phase 4: Index and Query Optimization

#### Added
- B+ Tree index implementation
- Index DDL (CREATE INDEX, DROP INDEX)
- Index scan support
- Query optimizer with cost model
- Nested Loop Join
- Statistics management (ANALYZE)
- Selectivity estimation
- EXPLAIN command

## [0.3.0] - 2024-03-18

### Phase 3: DML and Transactions

#### Added
- Full CRUD operations
- Expression evaluation
- Transaction support (ACID)
- WAL logging
- Table-level locking
- Deadlock detection framework

## [0.2.0] - 2024-03-17

### Phase 2: Storage Engine

#### Added
- Page-based storage (8KB pages)
- Heap table storage
- Buffer pool with LRU replacement
- WAL (Write-Ahead Logging)
- System tables (tn_class, tn_attribute)
- DDL support (CREATE TABLE, DROP TABLE)

## [0.1.0] - 2024-03-16

### Phase 1: Network and Protocol

#### Added
- TCP server with multi-threading
- Custom binary protocol
- SQL lexer (Flex)
- SQL parser (Bison)
- Client SDK
- Basic executor
