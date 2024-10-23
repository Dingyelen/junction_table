###
-- 【基础信息】
-- kpi
drop table if exists hive.mushroom_tw_w.ads_kpi_hourly_hf;

create table if not exists hive.mushroom_tw_w.ads_kpi_hourly_hf
(date date,
hour timestamp,
zone_id varchar,
channel varchar,
os varchar, 
exchange_rate double,
dau bigint, 
paycount_daily bigint, 
moneyrmb_daily decimal(36, 2), 
moneyrmb_lasthour decimal(36, 2), 
new_users bigint,
active_users_error bigint,
active_users bigint,
pay_users bigint,
install_pay bigint,
newpay_users bigint,
pay_count bigint,
money_rmb decimal(36, 2),
newusers_ac bigint, 
moneyrmb_ac decimal(36, 2), 
part_date varchar
)
with(partitioned_by = array['part_date']);

insert into hive.mushroom_tw_w.ads_kpi_hourly_hf
(date, hour, zone_id, channel, os, 
exchange_rate, dau, paycount_daily, moneyrmb_daily, moneyrmb_lasthour, 
new_users, active_users_error, active_users, 
pay_users, install_pay, newpay_users,
pay_count, money_rmb, 
newusers_ac, moneyrmb_ac, 
part_date)

with user_hourly as
(select date, part_date, hour, 
date_format(hour, '%H') as hour_pure, 
exchange_rate, 
role_id, pay_count, money, money_rmb, last_event
from hive.mushroom_tw_w.dws_user_hourly_hi
where part_date >= cast(date_add('month', -6, date(current_date)) as varchar)
and part_date <= cast(current_date as varchar)
),

user_hourly_join as
(select a.date, a.hour, a.hour_pure, a.part_date,
a.role_id, a.exchange_rate, 
a.pay_count, a.money, a.money_rmb, 
a.last_event,
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_ts, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
date_diff('hour', date_trunc('hour', b.install_ts), a.hour) as retention_hour,
date_diff('hour', date_trunc('hour', b.firstpay_ts), a.hour) as pay_retention_hour
from user_hourly a
left join hive.mushroom_tw_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

hourly_info as
(select date, part_date, hour, hour_pure, 
zone_id, channel, os, exchange_rate, 
count(distinct (case when retention_hour = 0 then role_id else null end)) as new_users,
count(distinct role_id) as active_users_error,
count(distinct (case when money_rmb > 0 then role_id else null end)) as pay_users,
count(distinct (case when money_rmb > 0 and retention_hour = 0 and pay_retention_hour = 0 then role_id else null end)) as install_pay,
count(distinct (case when money_rmb > 0 and pay_retention_hour = 0 then role_id else null end)) as newpay_users, 
sum(pay_count) as pay_count, 
sum(money_rmb) as money_rmb
from user_hourly_join
group by 1, 2, 3, 4, 5, 6, 7, 8
), 

daily_info as(
select date, zone_id, channel, os, 
count(distinct role_id) as dau, 
sum(pay_count) as paycount_daily, 
sum(money_rmb) as moneyrmb_daily, 
sum(case when cast(hour_pure as bigint) <= cast(date_format(current_timestamp, '%H') as bigint) then money_rmb else null end) as moneyrmb_lasthour 
from user_hourly_join 
group by 1, 2, 3, 4
), 

active_data_cube_base as
(select role_id, hour, hour_agg
from user_hourly_join
cross join unnest(sequence(hour, date_add('day', $interval_day, hour), interval '1' hour)) as t(hour_agg)
order by 1, 2, 3
), 

-- 这里修改时注意自己项目的登出事件写法
active_data_cube as
(select a.hour, a.role_id, a.zone_id, a.channel, a.os, a.last_event,
b.hour_agg,
(case when b.hour_agg != a.hour and a.last_event = 'logout' then 'nonact' else 'act' end) as isact
from user_hourly_join a
left join active_data_cube_base b
on a.hour = b.hour and a.role_id = b.role_id
),

active_data_cube_last as
(select hour_agg, role_id, zone_id, channel, os, 
map_agg(hour, last_event) as last_events,
element_at(array_agg(isact order by hour), -1) as isact_final
from active_data_cube
group by 1, 2, 3, 4, 5
),

hourly_act as
(select hour_agg, zone_id, channel, os, 
count(distinct (case when isact_final = 'act' then role_id else null end)) as active_users
from active_data_cube_last
group by 1, 2, 3, 4
),

-- data_cube as(
-- select *, cast(concat(part_date, ' ', hour_pure, ':00:00') as timestamp) as hour from
-- (select distinct date, part_date, zone_id, channel, os, exchange_rate from hourly_info)
-- cross join
-- (select distinct hour_pure from hourly_info)
-- ), 

data_cube as(
select distinct date, part_date, zone_id, channel, os, exchange_rate, t.hour, date_format(t.hour, '%H') as hour_pure
from hourly_info a
cross join unnest(sequence(hour, date_add('day', $interval_day, hour), interval '1' hour)) as t(hour)
), 

hourly_info_final as(
select a.date, a.part_date, a.hour, a.hour_pure, a.zone_id, a.channel, a.os, 
a.exchange_rate, d.dau, d.paycount_daily, d.moneyrmb_daily, d.moneyrmb_lasthour, 
b.new_users, b.active_users_error, c.active_users, 
b.pay_users, b.install_pay, b.newpay_users, 
b.pay_count, b.money_rmb, 
sum(b.new_users) over (partition by a.zone_id, a.channel, a.os, a.date order by a.hour rows between unbounded preceding and current row) as newusers_ac,
sum(b.money_rmb) over (partition by a.zone_id, a.channel, a.os, a.date order by a.hour rows between unbounded preceding and current row) as moneyrmb_ac
from data_cube a
left join hourly_info b
on a.hour = b.hour
and a.zone_id = b.zone_id 
and a.channel = b.channel
and a.os = b.os
left join hourly_act c
on a.hour = c.hour_agg
and a.zone_id = c.zone_id 
and a.channel = c.channel
and a.os = c.os
left join daily_info d
on a.date = d.date
and a.zone_id = d.zone_id 
and a.channel = d.channel
and a.os = d.os
and a.hour_pure = '00'
)

select date, hour, zone_id, channel, os, 
exchange_rate, dau, paycount_daily, moneyrmb_daily, moneyrmb_lasthour,  
new_users, active_users_error, active_users, 
pay_users, install_pay, newpay_users,
pay_count, money_rmb, 
newusers_ac, moneyrmb_ac, 
part_date
from hourly_info_final
;
###