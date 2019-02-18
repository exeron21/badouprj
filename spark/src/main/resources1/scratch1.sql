-- 查出每个用户最喜爱的10%的商品
select user_id,ceil(cast(count (1) as double) * 0.1) from badou.orders ord
join badou.order_prior pri
on ord.order_id = pri.order_id
group by user_id limit 100;

select user_id,ceil(cast(count(distinct pri.product_id) as double)*0.1) as total_prod_cnt
from orders join order_prior pri
on orders.order_id=pri.order_id
group by user_id
limit 10;


select user_id,collect_list(concat_ws('_',product_id,
cast(row_num as string),cast(total_prod_cnt as string))) as top_10_perc_prod
from
(select user_id,product_id,
row_number() over(distribute by user_id sort by usr_prod_cnt desc) as row_num,
ceil(cast((count(1) over(partition by user_id)) as double)*0.1) as total_prod_cnt
from
(select user_id,product_id,
count(1) as usr_prod_cnt
from orders join (select * from order_prior limit 3000) pri
on orders.order_id=pri.order_id
group by user_id,product_id)t)t1
where row_num<=total_prod_cnt
group by user_id
limit 10;


-- 查出每个客户一共买了多少种商品，并导入表中
create table user_prod_cnt as
select user_id,product_id,
count(1) as usr_prod_cnt
from orders join order_prior pri
on orders.order_id=pri.order_id
group by user_id,product_id;

-- 查出每个客户每种商品
select * from badou.user_prod_cnt limit 100;
select cnt.user_id,cnt.product_id,
ceil(cast (count(1) over(partition by user_id) as double) * 0.1) as total_prod_cnt
from badou.user_prod_cnt cnt
where cnt.user_id=1
group by cnt.user_id,cnt.product_id;

select cnt.user_id,cnt.product_id,
count(1) over(partition by user_id) as total_prod_cnt
from badou.user_prod_cnt cnt
where cnt.user_id=1
group by cnt.user_id,cnt.product_id;

select * from order_prior limit 10;

select * from user_prod_cnt where user_id=1;

select user_id,product_id,ceil(count(1) *0.1),count(1),
count(1) over(partition by user_id) as total_prod_cnt
from user_prod_cnt
group by user_id,product_id order by user_id,product_id;

select t.user_id,collect_list(concat_ws('_',cast(t.total_prod_cnt as string),cast(t.row_num as string))) as cnt from
(select user_id,
--ceil(cast((count(1) over(partition by user_id)) as double)*0.1) as total_prod_cnt
ceil(cast(count(1) over(partition by user_id) as double)*0.1) as total_prod_cnt,
row_number() over(partition by user_id order by cnt.usr_prod_cnt desc) as row_num,
cnt.usr_prod_cnt as cnt
from user_prod_cnt cnt) t
where t.total_prod_cnt>=row_num
group by t.user_id;


select * from allfiles limit 1;

drop table allfiles;
create table allfiles (
  sentence string,
  label string,
  sen string
) row format delimited fields terminated by ',';

select regexp_replace("hello world, micheal.", " ","");

load data local inpath('/home/hdp/data/allfiles') overwrite into table allfiles;
drop table if exists sen_noseg;
create table sen_noseg as select regexp_replace(sentence,' ', '') as sentence from allfiles;
delete file /home/hdp/sent.py;
add file /home/hdp/sent.py;
select transform(sentence) using 'python sent.py' as seg from sen_noseg limit 10;
select sentence as (seg) from sen_noseg limit 10;

select * from sen_noseg limit 2;

