USE sberSm20232020_test;

SET hive.exec.parallel=true; 

SET mapreduce.job.reduces=15;
SET hive.auto.convert.join = False;

select r.region as region,
 SUM(r.male) as count_male,SUM(r.female) as count_female
from
(SELECT r.region as region,
    CASE WHEN u.sex = 'male' then 1 else 0 end as male,
    CASE WHEN u.sex = 'female' then 1 else 0 end as female
FROM logs l
left JOIN users u on l.ip=u.ip
left JOIN ipregions r on r.ip = l.ip) r
GROUP BY r.region;
