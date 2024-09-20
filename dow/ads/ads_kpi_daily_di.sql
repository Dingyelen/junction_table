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
moneyrmb_ac decimal(36, 2),
new_users_ac bigint,
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
web_rmb_daily, web_rmb_users, moneyrmb_ac, newuser_ac, 
part_date)

with user_daily as(
select 
date, part_date,
role_id, 
level_min as level_min_daily, level_max as level_max_daily,
viplevel_min as viplevel_min_daily, viplevel_max as viplevel_max_daily, 
online_time, 
exchange_rate, pay_count as pay_count_daily, money as money_daily, money_rmb as money_rmb_daily, web_rmb as web_rmb_daily
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
a.exchange_rate, a.pay_count_daily, a.money_daily, a.money_rmb_daily, a.web_rmb_daily, 
b.moneyrmb_ac, b.newuser_ac, 
c.install_date, date(c.lastlogin_ts) as lastlogin_date, 
c.firstpay_date, c.firstpay_goodid, c.firstpay_level,
c.zone_id, c.channel, c.os, 
date_diff('day', c.install_date, a.date) as retention_day,
date_diff('day', c.firstpay_date, a.date) as pay_retention_day,
date_diff('day', c.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.dow_jpnew_w.dws_user_daily_derive_df b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.dow_jpnew_w.dws_user_info_di c
on a.role_id = c.role_id
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
sum(case when money_daily > 0 and pay_retention_day = 0 then money_rmb_daily else null end) as users_new_moneyrmb, 
sum(web_rmb_daily) as web_rmb_daily, 
count(distinct (case when web_rmb_daily > 0  then role_id else null end)) as web_rmb_users, 
sum(moneyrmb_ac) as moneyrmb_ac, 
sum(newuser_ac) as newuser_ac
from user_daily_join
group by 1, 2, 3, 4, 5
),

data_cube as
(select distinct zone_id, channel, os, date from daily_info
cross join unnest(sequence($start_date, current_date, interval '1' day)) as t(date)
),

daily_info_cube as
(select a.*,
b.new_users, b.active_users, 
b.pay_users, b.paid_users, b.new_users_pay, b.users_new_pay, 
b.online_time, 
b.pay_count_daily, b.money_rmb_daily, b.new_users_moneyrmb, b.users_new_moneyrmb, 
b.web_rmb_daily, b.web_rmb_users, b.moneyrmb_ac, b.newuser_ac
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
web_rmb_daily, web_rmb_users, moneyrmb_ac, newuser_ac, 
part_date
from daily_info_cube
;
###
