create table if not exists hive.dow_jpnew_w.ads_top_life_di
(date date,
role_id varchar, 
zone_id varchar, 
channel varchar, 
level_max bigint, 
viplevel_max bigint, 
lastpay_date date, 
install_date date, 
lastlogin_date date, 
pay_rank bigint, 
login_day bigint, 
money_ac decimal(36, 2), 
money decimal(36, 2), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.ads_top_life_di where part_date >= $start_date and part_date <= $end_date;

insert into hive.dow_jpnew_w.ads_top_life_di
(date, role_id, zone_id, channel, 
level_max, viplevel_max, lastpay_date, 
install_date, lastlogin_date, 
pay_rank, login_day, money_ac, money, part_date)

with data_cube as(
select date_cube, cast(date_cube as varchar) as part_date
from unnest(sequence(date $start_date, date $end_date, interval '1' day)) as t(date_cube)
), 

user_daily_join as
(select 
a.date, 
a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max, 
a.online_time, 
a.pay_count, a.money, a.web_money, b.money_ac, b.is_new, 
c.install_date, date(c.lastlogin_ts) as lastlogin_date, date(c.lastpay_ts) as lastpay_date, 
c.firstpay_date, c.firstpay_goodid, c.firstpay_level,
c.zone_id, c.channel, c.os
from hive.dow_jpnew_w.dws_user_daily_di a
left join hive.dow_jpnew_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.dow_jpnew_w.dws_user_info_di c
on a.role_id = c.role_id
where a.part_date <= $end_date
and c.is_test is null
and c.money_ac > 0
-- and a.money > 0
), 

data_cube_join as(
select a.date_cube, a.part_date, b.*
from data_cube a
left join user_daily_join b
on a.date_cube >= b.date
), 

data_cube_agg as(
select date_cube, part_date, role_id, zone_id, 
channel, lastpay_date, install_date, 
max(level_max) as level_max, 
max(viplevel_max) as viplevel_max, 
max(date) as lastlogin_date, 
max(money_ac) as money_ac, 
sum(case when date_cube = date then money else null end) as money, 
count(case when date >= date_add('day', -6, date_cube) then date_cube else null end) as login_day
from data_cube_join 
group by 1, 2, 3, 4, 5, 6, 7
), 

data_cube_rank as(
select date_cube, part_date, 
role_id, zone_id, channel, 
level_max, viplevel_max, 
lastpay_date, install_date, lastlogin_date,
money_ac, money, login_day, 
row_number() over(partition by date_cube order by money_ac desc, level_max desc, money desc) as pay_rank
from data_cube_agg
)

select date_cube as date, 
role_id, zone_id, channel, 
level_max, viplevel_max, 
lastpay_date, install_date, lastlogin_date,
money_ac, money, login_day, pay_rank, 
part_date
from data_cube_rank
where pay_rank <= 200;