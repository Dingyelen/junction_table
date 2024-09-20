###
-- 【基础信息】
-- kpi
create table if not exists hive.dow_jpnew_w.ads_kpi_monthly_mi
(month date, 
zone_id varchar, 
channel varchar, 
os varchar, 
new_users bigint, 
active_users bigint, 
pay_users bigint, 
new_users_pay bigint, 
users_new_pay bigint,
pay_count bigint, 
money_rmb decimal(36, 2), 
new_users_moneyrmb decimal(36, 2), 
users_new_moneyrmb decimal(36, 2), 
new_users_ac bigint, 
moneyrmb_ac decimal(36, 2),
part_month varchar
)
with(partitioned_by = array['part_month']);

delete from hive.dow_jpnew_w.ads_kpi_monthly_mi
where part_month >= $start_date
and part_month <= $end_date;

insert into hive.dow_jpnew_w.ads_kpi_monthly_mi
(month, zone_id, channel, os, 
new_users, active_users, 
pay_users, new_users_pay, users_new_pay,
pay_count, money_rmb, 
new_users_moneyrmb, users_new_moneyrmb, 
new_users_ac, moneyrmb_ac,
part_month)

with user_daily as(
select 
date, part_date,
date_trunc('month', date) as month, 
role_id, 
level_min, level_max,
viplevel_min, viplevel_max,
exchange_rate, 
pay_count, money, money_rmb
from hive.dow_jpnew_w.dws_user_daily_di 
where part_date >= $start_date
and part_date <= $end_date
), 

user_monthly as(
select month, role_id, 
min(level_min) as level_min,
max(level_max) as level_max,
min(viplevel_min) as viplevel_min,
max(viplevel_max) as viplevel_max, 
sum(pay_count) as pay_count, 
sum(money) as money, 
sum(money_rmb) as money_rmb
from user_daily
group by 1, 2
), 

user_monthly_join as(
select 
a.month, 
a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max,
a.pay_count, a.money, a.money_rmb, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
date_diff('month', date_trunc('month', b.install_date), a.month) as retention_month,
date_diff('month', date_trunc('month', b.firstpay_date), a.month) as pay_retention_month
from user_monthly a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

monthly_info as(
select month, zone_id, channel, os, 
count(distinct (case when retention_month = 0 then role_id else null end)) as new_users,
count(distinct role_id) as active_users,
count(distinct (case when money > 0 then role_id else null end)) as pay_users,
count(distinct (case when money > 0 and retention_month = 0 and pay_retention_month = 0 then role_id else null end)) as new_users_pay,
count(distinct (case when money > 0 and pay_retention_month = 0 then role_id else null end)) as users_new_pay,
sum(pay_count) as pay_count, 
sum(money_rmb) as money_rmb, 
sum(case when money > 0 and retention_month = 0 and pay_retention_month = 0 then money_rmb else null end) as new_users_moneyrmb, 
sum(case when money > 0 and pay_retention_month = 0 then money_rmb else null end) as users_new_moneyrmb
from user_monthly_join
group by 1, 2, 3, 4
),

data_cube as
(select * from
(select distinct month from monthly_info)
cross join
(select distinct zone_id from monthly_info)
cross join
(select distinct channel from monthly_info)
cross join
(select distinct os from monthly_info)
),

monthly_info_cube as
(select a.*,
b.new_users, b.active_users, 
b.pay_users, b.new_users_pay, b.users_new_pay,
b.pay_count, b.money_rmb, b.new_users_moneyrmb, b.users_new_moneyrmb, 
sum(b.new_users) over (partition by a.zone_id, a.channel, a.os order by a.month rows between unbounded preceding and current row) as new_users_ac,
sum(b.money_rmb) over (partition by a.zone_id, a.channel, a.os order by a.month rows between unbounded preceding and current row) as moneyrmb_ac
from data_cube a
left join monthly_info b
on a.month = b.month 
and a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os
)

select month, zone_id, channel, os, 
new_users, active_users, 
pay_users, new_users_pay, users_new_pay,
pay_count, money_rmb, 
new_users_moneyrmb, users_new_moneyrmb, 
new_users_ac, moneyrmb_ac,
date_format(month, '%Y-%m-%d') as part_month
from monthly_info_cube
;
###
