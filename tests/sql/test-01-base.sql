-- create table
create table t1(id int, name varchar(100));

-- add data
insert into t1 values(1,'ht1');
insert into t1 values(2,'ht2');

-- query data
select * from t1;

-- update data
update t1 set name='ht11' where id=1;
select * from t1;

-- using index
CREATE INDEX idx_t1_id ON t1(id);
EXPLAIN select * from t1 where id=2;
update t1 set name='ht111' where id=1;
select * from t1;





