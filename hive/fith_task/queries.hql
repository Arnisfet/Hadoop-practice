USE sberSm20232020_test;

SET hive.exec.parallel=true; 

SET mapreduce.job.reduces=12;
SET hive.auto.convert.join = False;

select r.region as region,
 SUM(r.male) as count_male, avg(r.male) as count_female
from
(SELECT r.region as region, CASE WHEN u.sex = 'male' then 1 else 0 end as male, CASE WHEN u.sex = 'female' then 1 else 0 end as female
FROM logs tablesample(5 percent) l left JOIN users u on l.ip=u.ip left JOIN ipregions r on r.ip = l.ip where r.region = 'Komi') r GROUP BY r.region;
