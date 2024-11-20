/*
* @Author: dingyelen
* @Date:   2024-10-16 17:13:44
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-11-20 16:06:35
*/


###
-- 【基础信息】
-- kpi
-- 本报表 money_ac 和 newuser_ac 为全服不是活跃

create table if not exists hive.dow_jpnew_w.ads_kpi_daily_di
(date date, 
zone_id varchar, 
channel varchar, 
os varchar, 
new_users bigint, 
active_users bigint, 
pay_users bigint, 
paid_users bigint, 
webpay_users bigint, 
install_pay bigint, 
newpay_users bigint, 
online_time bigint, 
money decimal(36, 2), 
app_money decimal(36, 2), 
web_money decimal(36, 2), 
money_ac decimal(36, 2), 
install_money decimal(36, 2), 
newpay_money decimal(36, 2), 
pay_count bigint, 
app_count bigint, 
web_count bigint, 
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
pay_users, paid_users, webpay_users, install_pay, newpay_users, 
online_time, money, app_money, web_money, money_ac, 
install_money, newpay_money, 
pay_count, app_count, web_count, newuser_ac, part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max, viplevel_min, viplevel_max, 
online_time, money, web_money, app_money, 
pay_count, web_count, app_count
from hive.dow_jpnew_w.dws_user_daily_di 
where part_date <= $end_date
), 

user_daily_join as(
select a.date, a.part_date, a.role_id, 
a.level_min, a.level_max, a.viplevel_min, a.viplevel_max, 
a.online_time, a.money, a.web_money, a.app_money, b.money_ac, 
a.pay_count, a.web_count, a.app_count,  
b.is_new, c.install_date, date(c.lastlogin_ts) as lastlogin_date, 
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
count(distinct (case when money > 0 then role_id else null end)) as pay_users,
count(distinct (case when money_ac > 0 then role_id else null end)) as paid_users, 
count(distinct (case when money > 0 and retention_day = 0 and pay_retention_day = 0 then role_id else null end)) as install_pay,
count(distinct (case when money > 0 and pay_retention_day = 0 then role_id else null end)) as newpay_users,
sum(online_time) as online_time, 
sum(money) as money, sum(app_money) as app_money, sum(web_money) as web_money, sum(money_ac) as money_ac, 
sum(case when money > 0 and retention_day = 0 and pay_retention_day = 0 then money else null end) as install_money, 
sum(case when money > 0 and pay_retention_day = 0 then money else null end) as newpay_money, 
sum(pay_count) as pay_count, sum(app_count) as app_count, sum(web_count) as web_count, 
count(distinct (case when web_money > 0  then role_id else null end)) as webpay_users
from user_daily_join
where part_date >= $start_date
and part_date <= $end_date
group by 1, 2, 3, 4, 5
),

-- 历史新增人数
his_new as(
select zone_id, channel, os
from hive.dow_jpnew_w.dws_user_info_di
), 

data_cube as(
select distinct zone_id, channel, os, t.date 
from his_new
cross join unnest(sequence(date_add('day', -30, date $start_date), date $end_date, interval '1' day)) as t(date)
), 

life_agg as(
select date, part_date, zone_id, channel, os, 
sum(money) as money, 
count(distinct case when retention_day = 0 then role_id else null end) as new_users
from user_daily_join
group by 1, 2, 3, 4, 5
), 

life_ac_agg as(
select date, part_date, zone_id, channel, os, 
sum(money) over(partition by zone_id, channel, os order by date rows between unbounded preceding and current row) as money_ac, 
sum(new_users) over(partition by zone_id, channel, os order by date rows between unbounded preceding and current row) as newuser_ac 
from life_agg
), 

daily_info_cube as
(select a.*,
b.new_users, b.active_users, 
b.pay_users, b.paid_users, b.webpay_users, b.install_pay, b.newpay_users, 
b.online_time, b.money, b.app_money, b.web_money, 
-- c.money_ac, 
coalesce(c.money_ac, lag(c.money_ac, 1) ignore nulls over(partition by a.zone_id, a.channel, a.os order by a.date)) as money_ac, 
b.install_money, b.newpay_money, 
b.pay_count, b.app_count, b.web_count,
-- c.newuser_ac
coalesce(c.newuser_ac, lag(c.newuser_ac, 1) ignore nulls over(partition by a.zone_id, a.channel, a.os order by a.date)) as newuser_ac
from data_cube a
left join daily_info b
on a.date = b.date 
and a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os
left join life_ac_agg c
on a.date = c.date 
and a.zone_id = c.zone_id 
and a.channel = c.channel 
and a.os = c.os
)

select date, zone_id, channel, os, 
new_users, active_users, 
pay_users, paid_users, webpay_users, install_pay, newpay_users, 
online_time, money, app_money, web_money, money_ac, 
install_money, newpay_money, 
pay_count, app_count, web_count, 
newuser_ac, date_format(date, '%Y-%m-%d') as part_date
from daily_info_cube
where date >= date $start_date
and date <= date $end_date
;