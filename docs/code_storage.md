
## 插入数据流程
```
StorageEngine::insert
 TableManager::insertTuple
  TableManager::getTable //根据表名获取表的meta信息
  meta->last_page_id //根据meta信息拿到pageID
  Tuple::serialize //将tuple序列化为二进制
  BufferPoolManager::fetchPage //根据pageID获取Frame
  BufferPoolManager::unpinPage // Frame没有足够的空间
  BufferPoolManager::newPage
  Page::insertTuple // 插入元组
```




