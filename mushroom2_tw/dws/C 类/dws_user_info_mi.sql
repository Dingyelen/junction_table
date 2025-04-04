###
create table if not exists hive.mushroom2_tw_w.dws_user_info_mi(
month date, 
role_id varchar, 
is_test bigint, 
login_days bigint, 
online_time bigint, 
login_times bigint, 
pay_count bigint, 
money double, 
pay_tag varchar, 
part_month varchar
)
with(partitioned_by = array['part_month']);

delete from hive.mushroom2_tw_w.dws_user_info_mi where part_month >= $start_date and part_month <= $end_date;

insert into hive.mushroom2_tw_w.dws_user_info_mi(
month, role_id, 
is_test, 
login_days, online_time, login_times, 
pay_count, money, pay_tag, 
part_month)

with user_daily as(
select *, date_trunc('month', date) as month
from hive.mushroom2_tw_w.dws_user_daily_di
where part_date >= $start_date
and  part_date <= $end_date
), 

user_info as(
select month, role_id, 
max(is_test) as is_test,
count(*) as login_days, 
sum(online_time) as online_time, 
sum(login_times) as login_times, 
sum(pay_count) as pay_count, 
sum(money) as money
from user_daily 
group by 1, 2
), 

tag_cal as(
select month, role_id, 
(case when money >= 20000/0.052102 then '超R'
when money >= 6000/0.052102 then '大R'
when money >= 1000/0.052102 then '中R'
when money >= 300/0.052102 then '小R'
when money > 0/0.052102 then '微R'
else '非R' end)  as pay_tag 
from user_info
)

select 
a.month, a.role_id, 
a.is_test, 
a.login_days, a.online_time, a.login_times, 
a.pay_count, a.money, b.pay_tag, 
date_format(a.month, '%Y-%m-%d') as part_month
from user_info a
left join tag_cal b
on a.role_id = b.role_id and a.month = b.month;
###