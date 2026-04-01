# 事务处理流程

```
InsertExecutor::execute
 StorageEngine::beginTransaction
  TransactionManager::beginTransaction// 创建一个事务对象，并分配事务ID
 Executor::acquireTableLock //为该事务创建某个表的表锁
 StorageEngine::insert //详见code_storage.md
 Transaction::addInsertRecord
  insert_records_.push_back //记录当前事务的insert记录
 Executor::releaseTableLock
  StorageEngine::unlockTable
   LockManager::unlockTable
 StorageEngine::abortTransaction
  LockManager::releaseAllLocks
  TransactionManager::abortTransaction
   WALManager::logRollback
  TransactionManager::cleanupCompletedTransactions
 StorageEngine::commitTransaction
  LockManager::releaseAllLocks
  TransactionManager::commitTransaction
   WALManager::logCommit
   BufferPoolManager::flushPage
   WALManager::flush
   clearRecords
   clearModifiedPages
  TransactionManager::cleanupCompletedTransactions
```

