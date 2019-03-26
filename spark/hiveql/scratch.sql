show databases;
SELECT word,count(1) AS count FROM (SELECT explode(split(,'[ \t]+') ) AS word FROM docs ) w GROUP BY word ORDER BY word;


SELECT explode(split(line,'\s')) AS word FROM docs;
SELECT word,count(1) FROM w GROUP BY word ORDER BY word;


delete file /home/hdp/sent.py;
add file /home/hdp/sent.py;

select word,count(1) from
  (select explode(split(w,' ')) from
    (select transform(sentence) using 'python sent.py' as w from sen_noseg)) as word
group by word;

select word,count(*) as cnt from(
  select explode(split(w,' ')) as word from (
    select transform(sentence) using 'python sent.py' as w from sen_noseg
  ) tt
) t group by word order by cnt desc limit 100;


select ord.user_id,concat_ws('_',collect_set(cast(pri.product_id as string))) from orders ord
inner join order_prior pri
on ord.order_id=pri.order_id
group by ord.user_id limit 10;
