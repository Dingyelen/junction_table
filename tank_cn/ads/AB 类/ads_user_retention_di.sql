create table if not exists hive.tank_cn_w.ads_user_retention_di
(date date, 
install_date date,
zone_id varchar,
channel varchar,
os varchar, 
break_type varchar, 
retention_day bigint,
users bigint,
pay_users bigint,
users_new_pay bigint,
money_rmb_daily decimal(36, 2), 
online_time_daily bigint,
pay_users_ac bigint,
moneyrmb_ac decimal(36, 2),
new_users bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.tank_cn_w.ads_user_retention_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.tank_cn_w.ads_user_retention_di
(date, install_date, zone_id, channel, os, break_type, retention_day, 
users, pay_users, users_new_pay, money_rmb_daily, online_time_daily, 
pay_users_ac, moneyrmb_ac,
new_users, 
part_date)

with user_daily as(
select 
date, part_date,
role_id, 
level_min as level_min_daily, level_max as level_max_daily,
viplevel_min as viplevel_min_daily, viplevel_max as viplevel_max_daily,
money as money_daily, 
money_rmb as money_rmb_daily, exchange_rate, 
online_time
from hive.tank_cn_w.dws_user_daily_di 
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min_daily, a.level_max_daily,
a.viplevel_min_daily, a.viplevel_max_daily,
a.money_daily, a.money_rmb_daily, a.exchange_rate, a.online_time, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
(case when b.install_date=b.firstpay_date then 'firstdate_break' 
when b.firstpay_date is not null then 'other_break'
else 'not_break' end) as break_type,  
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.tank_cn_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
and b.install_date >= date($start_date)
and b.install_date <= date($end_date)
),

retention_info as
(select date, install_date, zone_id, channel, os, break_type, retention_day,
count(distinct role_id) as users,
count(distinct (case when money_daily > 0 then role_id else null end)) as pay_users,
count(distinct (case when money_daily > 0 and pay_retention_day = 0 then role_id else null end)) as users_new_pay,
sum(money_rmb_daily) as money_rmb_daily, 
sum(online_time) as online_time_daily
from user_daily_join
group by 1, 2, 3, 4, 5, 6, 7
),

retention_all as
(select install_date, zone_id, channel, os, break_type, 
sum(case when retention_day = 0 then users else null end) as new_users
from retention_info
group by 1, 2, 3, 4, 5
),

data_cube as
(select * from
(select distinct install_date, zone_id, channel, os, break_type from retention_info)
cross join unnest(sequence(0, 30, 1)) as t(retention_day)
),

retenion_info_cube as
(select date_add('day', a.retention_day, a.install_date) as date, 
a.install_date, a.zone_id, a.channel, a.os, a.break_type, a.retention_day,
b.users, b.pay_users, b.users_new_pay, b.money_rmb_daily, b.online_time_daily, 
c.new_users
from data_cube a
left join retention_info b
on a.install_date = b.install_date and a.retention_day = b.retention_day
and a.zone_id = b.zone_id and a.channel = b.channel and a.os = b.os 
and a.break_type = b.break_type
left join retention_all c
on a.install_date = c.install_date
and a.zone_id = c.zone_id and a.channel = c.channel and a.os = c.os 
and a.break_type = c.break_type
)

select date, install_date, zone_id, channel, os, break_type, retention_day, 
users, pay_users, users_new_pay, money_rmb_daily, online_time_daily, 
sum(users_new_pay) over (partition by install_date, zone_id, channel, break_type order by retention_day
rows between unbounded preceding and current row) as pay_users_ac,
sum(money_rmb_daily) over (partition by install_date, zone_id, channel, break_type order by retention_day
rows between unbounded preceding and current row) as moneyrmb_ac,
new_users, 
date_format(install_date, '%Y-%m-%d') as part_date
from retenion_info_cube
;