drop table if exists hive.dow_jpnew_w.ads_kpi_life_df;

create table if not exists hive.dow_jpnew_w.ads_kpi_life_df
(date date,
zone_id varchar, 
channel varchar, 
os varchar,
moneyrmb_ac decimal(36, 2), 
new_users_ac bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

insert into hive.dow_jpnew_w.ads_kpi_life_df
(date, zone_id, channel, os,
moneyrmb_ac, new_users_ac, 
part_date)

with user_daily as(
select 
date, part_date, role_id, 
money_rmb
from hive.dow_jpnew_w.dws_user_daily_di
), 

new_user as(
select install_date, zone_id, channel, os, 
count(distinct role_id) as new_users
from hive.dow_jpnew_w.dws_user_info_di 
group by 1, 2, 3, 4
), 

-- new_user_info as(
-- select install_date, zone_id, channel, os, new_users, 
-- sum(new_users) over(partition by zone_id, channel, os order by install_date rows between unbounded preceding and current row) as new_users_ac
-- from new_user
-- ), 

user_daily_join as
(select 
a.date, a.part_date, a.role_id, 
a.money_rmb, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, b.firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

daily_info as
(select date, zone_id, channel, os, 
sum(money_rmb) as money_rmb
from user_daily_join
group by 1, 2, 3, 4
),

data_cube as
(select * from
(select distinct zone_id, channel, os from new_user)
cross join
(select distinct install_date as date from new_user)
),

cube_info as(
select a.date, a.zone_id, a.channel, a.os,
b.money_rmb, c.new_users
from data_cube a
left join daily_info b
on a.date = b.date and a.zone_id = b.zone_id 
and a.channel = b.channel and a.os = b.os 
left join new_user c
on a.date = c.install_date and a.zone_id = c.zone_id 
and a.channel = c.channel and a.os = c.os 
), 

cube_cal as(
select date, zone_id, channel, os, 
sum(money_rmb) over(partition by zone_id, channel, os order by date rows between unbounded preceding and current row) as moneyrmb_ac, 
sum(new_users) over(partition by zone_id, channel, os order by date rows between unbounded preceding and current row) as new_users_ac, 
date_format(date, '%Y-%m-%d') as part_date
from cube_info
)

select date, zone_id, channel, os, moneyrmb_ac, new_users_ac, 
part_date
from cube_cal
where date >= date_add('day', -15, date($end_date))
;