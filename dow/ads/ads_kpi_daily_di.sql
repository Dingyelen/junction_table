###
-- 【基础信息】
-- kpi
create table if not exists hive.dow_jpnew_w.ads_kpi_daily_di
(date date,
zone_id varchar,
channel varchar,
os varchar, 
new_users bigint,
active_users bigint,
pay_users bigint, 
paid_users bigint,
new_users_pay bigint,
users_new_pay bigint,
online_time bigint,
pay_count_daily bigint,
money_rmb_daily decimal(36, 2),
new_users_moneyrmb decimal(36, 2), 
users_new_moneyrmb decimal(36, 2), 
new_users_ac bigint,
moneyrmb_ac decimal(36, 2),
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.ads_kpi_daily_di
(date, zone_id, channel, os, 
new_users, active_users, 
pay_users, paid_users, new_users_pay, users_new_pay,
online_time, 
pay_count_daily, money_rmb_daily,
new_users_moneyrmb, users_new_moneyrmb, 
new_users_ac, moneyrmb_ac,
part_date)

with user_daily as(
select 
date, part_date,
role_id, 
level_min as level_min_daily, level_max as level_max_daily,
viplevel_min as viplevel_min_daily, viplevel_max as viplevel_max_daily, 
online_time, 
exchange_rate, 
pay_count as pay_count_daily, 
money as money_daily, 
money_rmb as money_rmb_daily
from hive.dow_jpnew_w.dws_user_daily_di 
where part_date >= $start_date
and part_date <= $end_date
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min_daily, a.level_max_daily,
a.viplevel_min_daily, a.viplevel_max_daily, 
a.online_time, 
a.exchange_rate, a.pay_count_daily, a.money_daily, a.money_rmb_daily, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

daily_info as
(select date, part_date, zone_id, channel, os, 
count(distinct (case when retention_day = 0 then role_id else null end)) as new_users,
count(distinct role_id) as active_users,
count(distinct (case when money_daily > 0 then role_id else null end)) as pay_users,
count(distinct (case when moneyrmb_ac > 0 then role_id else null end)) as paid_users, 
count(distinct (case when money_daily > 0 and retention_day = 0 and pay_retention_day = 0 then role_id else null end)) as new_users_pay,
count(distinct (case when money_daily > 0 and pay_retention_day = 0 then role_id else null end)) as users_new_pay,
sum(online_time) as online_time, 
sum(pay_count_daily) as pay_count_daily, 
sum(money_rmb_daily) as money_rmb_daily, 
sum(case when money_daily > 0 and retention_day = 0 and pay_retention_day = 0 then money_rmb_daily else null end) as new_users_moneyrmb, 
sum(case when money_daily > 0 and pay_retention_day = 0 then money_rmb_daily else null end) as users_new_moneyrmb
from user_daily_join
group by 1, 2, 3, 4, 5
),

data_cube as
(select * from
(select distinct date, part_date from daily_info)
cross join
(select distinct zone_id from daily_info)
cross join
(select distinct channel from daily_info)
cross join
(select distinct os from daily_info)
),

daily_info_cube as
(select a.*,
b.new_users, b.active_users, 
b.pay_users, b.paid_users, b.new_users_pay, b.users_new_pay, 
b.online_time, 
b.pay_count_daily, b.money_rmb_daily, b.new_users_moneyrmb, b.users_new_moneyrmb, 
sum(b.new_users) over (partition by a.zone_id, a.channel, a.os order by a.date rows between unbounded preceding and current row) as new_users_ac,
sum(b.money_rmb_daily) over (partition by a.zone_id, a.channel, a.os order by a.date rows between unbounded preceding and current row) as moneyrmb_ac
from data_cube a
left join daily_info b
on a.date = b.date 
and a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os
)

select date, zone_id, channel, os, 
new_users, active_users, 
pay_users, paid_users, new_users_pay, users_new_pay, 
online_time, 
pay_count_daily, money_rmb_daily, 
new_users_moneyrmb, users_new_moneyrmb, 
new_users_ac, moneyrmb_ac,
part_date
from daily_info_cube
;
###
