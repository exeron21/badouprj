# hive wordcount:

select t.word,count(1) as cnt from
(select explode(split(sentence,' ')) as word from allfiles) as t
group by t.word
order by cnt desc;


select count(*) from orders; -- 3421083

select count(*) from priors; -- 32434489



-- 导入数据需要设置set hive.enforce.bucketing=true;
-- 如果没有设置hive.enfoce.bucketing这个参数，那么需要设置和分桶个数相匹配的Reducer数目，set mapred.reduce.tasks=4，
-- 并且查询的时候需要添加CLSUTERBY子句。

-- 如果设置了我们查询的时候就不必设置Reducer数目，和查询的时候不必指定CLSUTRER BY子句。
-- orders_bucket：orders的分桶表
drop table orders_bucket;
create table orders_bucket(
order_id                int,
user_id                 int,
eval_set                string,
order_number            int,
order_dow               int,
order_hour_of_day       int,
days_since_prior_order  string
)
clustered by (order_id) sorted by (order_id asc) into 8 buckets
row format delimited fields terminated by ',';
set hive.enforce.bucketing=true;
insert overwrite table orders_bucket select * from orders;

drop table priors_bucket;
create table priors_bucket(
order_id                int,
product_id              int,
add_to_cart_order       int,
reordered               int
)
clustered by (order_id) sorted by (order_id asc) into 4 buckets
row format delimited fields terminated by ',';
set hive.enforce.bucketing=true;
insert overwrite table priors_bucket select * from priors;

select count(*) from orders o inner join priors p on o.order_id=p.order_id; -- 8 minutes 59 seconds 770 msec
select count(*) from orders_bucket o inner join priors_bucket p on o.order_id=p.order_id; -- 8 minutes 29 seconds 10 msec
set mapred.reduce.tasks=2;
set hive.enforce.bucketing=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.groupby.skewindata=true;

set mapred.map.tasks=12;

set hive.merge.size.per.task=256000000;


set hive.exec.reducers.bytes.per.reducer=100000000;
set hive.exec.reducers.max=4;
set mapreduce.job.reduces=4;
set hive.exec.reducers.bytes.per.reducer=2560000000;
select count(*) from orders_bucket o inner join priors_bucket p on o.order_id=p.order_id;




select count(1) from orders_part; -- 3421083
SET hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table orders_part partition(order_dow)
select order_id,user_id,eval_set,order_number,order_hour_of_day,days_since_prior_order,order_dow from orders;


drop table orders_part_bucket;
create table orders_part_bucket(
order_id                int,
user_id                 int,
eval_set                string,
order_number            int,
order_hour_of_day       int,
days_since_prior_order  string
)
partitioned by (order_dow int)
clustered by (order_id) sorted by (order_id asc) into 4 buckets
row format delimited fields terminated by ',';


drop table priors_part_bucket;
create table priors_part_bucket(
order_id                int,
product_id              int,
add_to_cart_order       int,
reordered               int
)
partitioned by ()
clustered by (order_id) sorted by (order_id asc) into 64 buckets
row format delimited fields terminated by ',';