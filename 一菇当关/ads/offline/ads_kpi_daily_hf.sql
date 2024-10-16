drop table if exists hive.mushroom_tw_w.ads_kpi_daily_hf;

create table if not exists hive.mushroom_tw_w.ads_kpi_daily_hf
(date date,
zone_id varchar,
channel varchar,
os varchar,
active_users bigint,
new_users bigint,
retention1_newusers bigint,
retention1_users bigint,
paid_users bigint,
pay_users bigint,
firstpay_users bigint,
firstdaypay_users bigint,
money_rmb decimal(36, 2),
firstpay_rmb decimal(36, 2), 
firstdaypay_rmb decimal(36, 2),
part_date varchar
)
with(partitioned_by = array['part_date']);

insert into hive.mushroom_tw_w.ads_kpi_daily_hf
(date, zone_id, channel, os,
active_users, new_users, retention1_newusers, retention1_users, 
paid_users, pay_users, firstpay_users, firstdaypay_users, 
firstpay_rmb, firstdaypay_rmb, money_rmb, 
part_date)

with user_hourly as(
select 
date, part_date,
role_id, 
money, money_rmb, exchange_rate
from hive.mushroom_tw_w.dws_user_hourly_hi 
where part_date >= date_format(date_add('day', -15, date($end_date)), '%Y-%m-%d')
and part_date <= $end_date
), 

user_daily as(
select date, part_date,
role_id, 
sum(money) as money, 
sum(money_rmb) as money_rmb, 
min(exchange_rate) as exchange_rate
from user_hourly 
group by 1, 2, 3
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.money, a.money_rmb, a.exchange_rate, 
b.is_firstpay, b.is_pay, b.is_paid, 
c.install_date, date(c.lastlogin_ts) as lastlogin_date, 
c.moneyrmb_ac, c.firstpay_date, c.firstpay_goodid, c.firstpay_level,
c.zone_id, c.channel, c.os, 
(case when c.install_date=c.firstpay_date then 'firstdate_break' 
when c.firstpay_date is not null then 'other_break'
else 'not_break' end) as break_type,  
date_diff('day', c.install_date, a.date) as retention_day,
date_diff('day', c.firstpay_date, a.date) as pay_retention_day,
date_diff('day', c.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.mushroom_tw_w.dws_user_daily_derive_df b
on a.date = b.date and a.role_id = b.role_id
left join hive.mushroom_tw_w.dws_user_info_di c
on a.role_id = c.role_id
where c.is_test is null
),

daily_info as
(select date, zone_id, channel, os, 
count(distinct role_id) as active_users,
count(distinct case when retention_day = 0 then role_id else null end) as new_users, 
count(distinct case when retention_day = 1 then role_id else null end) as retention1_users, 
sum(is_paid) as paid_users, 
count(distinct case when money_rmb > 0 then role_id else null end) as pay_users, 
count(distinct case when money_rmb > 0 and firstpay_date = date then role_id else null end) as firstpay_users, 
count(distinct case when firstpay_date = date and retention_day = 0 then role_id else null end) as firstdaypay_users, 
sum(money_rmb) as money_rmb, 
sum(case when firstpay_date = date then money_rmb else null end) as firstpay_rmb, 
sum(case when firstpay_date = date and retention_day = 0 then money_rmb else null end) as firstdaypay_rmb
from user_daily_join
group by 1, 2, 3, 4
),

data_cube as
(select * from
(select distinct zone_id, channel, os from daily_info)
cross join
(select distinct date from daily_info)
),

retenion_info_cube as
(select a.date, a.zone_id, a.channel, a.os,
b.active_users, b.new_users, c.new_users as retention1_newusers, b.retention1_users, 
b.paid_users, b.pay_users, b.firstpay_users, b.firstdaypay_users, 
b.firstpay_rmb, b.firstdaypay_rmb, b.money_rmb
from data_cube a
left join daily_info b
on a.date = b.date and a.zone_id = b.zone_id 
and a.channel = b.channel and a.os = b.os 
left join daily_info c
on a.date = date_add('day', 1, c.date) and a.zone_id = c.zone_id 
and a.channel = c.channel and a.os = c.os 
)

select date, zone_id, channel, os,
active_users, new_users, retention1_newusers, retention1_users, 
paid_users, pay_users, firstpay_users, firstdaypay_users, 
firstpay_rmb, firstdaypay_rmb, money_rmb, 
date_format(date, '%Y-%m-%d') as part_date
from retenion_info_cube
;