/*
* @Author: dingyelen
* @Date:   2024-10-16 17:13:44
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-10-16 18:02:22
*/


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
install_pay bigint,
newpay_users bigint,
online_time bigint,
pay_count bigint,
money_rmb decimal(36, 2),
install_moneyrmb decimal(36, 2), 
newpay_moneyrmb decimal(36, 2), 
web_rmb decimal(36, 2), 
webpay_users bigint, 
moneyrmb_ac decimal(36, 2),
newuser_ac bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.ads_kpi_daily_di
(date, zone_id, channel, os, 
new_users, active_users, 
pay_users, paid_users, install_pay, newpay_users, 
online_time, 
pay_count, money_rmb, 
install_moneyrmb, newpay_moneyrmb, 
web_rmb, webpay_users, moneyrmb_ac, newuser_ac, 
part_date)

with user_daily as(
select 
date, part_date, role_id, 
level_min, level_max,
viplevel_min, viplevel_max, 
online_time, exchange_rate, 
pay_count, money, money_rmb, web_rmb
from hive.dow_jpnew_w.dws_user_daily_di 
where part_date >= $start_date
and part_date <= $end_date
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max, 
a.online_time, 
a.exchange_rate, a.pay_count, a.money, a.money_rmb, a.web_rmb, 
b.moneyrmb_ac, b.is_new, 
c.install_date, date(c.lastlogin_ts) as lastlogin_date, 
c.firstpay_date, c.firstpay_goodid, c.firstpay_level,
c.zone_id, c.channel, c.os, 
date_diff('day', c.install_date, a.date) as retention_day,
date_diff('day', c.firstpay_date, a.date) as pay_retention_day,
date_diff('day', c.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.dow_jpnew_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.dow_jpnew_w.dws_user_info_di c
on a.role_id = c.role_id
where c.is_test is null
),

daily_info as
(select date, part_date, zone_id, channel, os, 
count(distinct (case when retention_day = 0 then role_id else null end)) as new_users,
count(distinct role_id) as active_users,
count(distinct (case when money_rmb > 0 then role_id else null end)) as pay_users,
count(distinct (case when moneyrmb_ac > 0 then role_id else null end)) as paid_users, 
count(distinct (case when money_rmb > 0 and retention_day = 0 and pay_retention_day = 0 then role_id else null end)) as install_pay,
count(distinct (case when money_rmb > 0 and pay_retention_day = 0 then role_id else null end)) as newpay_users,
sum(online_time) as online_time, 
sum(pay_count) as pay_count, sum(money_rmb) as money_rmb, 
sum(case when money_rmb > 0 and retention_day = 0 and pay_retention_day = 0 then money_rmb else null end) as install_moneyrmb, 
sum(case when money_rmb > 0 and pay_retention_day = 0 then money_rmb else null end) as newpay_moneyrmb, 
sum(web_rmb) as web_rmb, 
count(distinct (case when web_rmb > 0  then role_id else null end)) as webpay_users, 
sum(moneyrmb_ac) as moneyrmb_ac
from user_daily_join
group by 1, 2, 3, 4, 5
),

data_cube as
(select distinct zone_id, channel, os, t.date from daily_info
cross join unnest(sequence(date $start_date, date $end_date, interval '1' day)) as t(date)
),

daily_info_cube as
(select a.*,
b.new_users, b.active_users, 
b.pay_users, b.paid_users, b.install_pay, b.newpay_users, 
b.online_time, 
b.pay_count, b.money_rmb, b.install_moneyrmb, b.newpay_moneyrmb, 
b.web_rmb, b.webpay_users, b.moneyrmb_ac, 
sum(b.new_users) over(partition by a.zone_id, a.channel, a.os order by a.date rows between unbounded preceding and current row) as newuser_ac
from data_cube a
left join daily_info b
on a.date = b.date 
and a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os
), 

-- 历史新增人数
his_new as(
select zone_id, channel, os, 
max(newuser_ac) as newuser_ac
from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date < $start_date
group by 1, 2, 3
), 

new_user_fit as(
select a.date, a.zone_id, a.channel, a.os, 
a.new_users, a.active_users, 
a.pay_users, a.paid_users, a.install_pay, a.newpay_users, 
a.online_time, 
a.pay_count, a.money_rmb, a.install_moneyrmb, a.newpay_moneyrmb, 
a.web_rmb, a.webpay_users, a.moneyrmb_ac, 
coalesce(a.newuser_ac, 0) + coalesce(b.newuser_ac, 0) as newuser_ac
from daily_info_cube a
left join his_new b
on a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os
)

select date, zone_id, channel, os, 
new_users, active_users, 
pay_users, paid_users, install_pay, newpay_users, 
online_time, 
pay_count, money_rmb, 
install_moneyrmb, newpay_moneyrmb, 
web_rmb, webpay_users, moneyrmb_ac, newuser_ac, 
date_format(date, '%Y-%m-%d') as part_date
from daily_info_cube
;