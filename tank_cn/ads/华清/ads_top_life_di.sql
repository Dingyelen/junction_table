create table if not exists hive.tank_cn_w.ads_top_life_di
(date date,
role_id varchar,
zone_id varchar,
channel varchar,
install_date date,
lastlogin_date date,
lastpay_date date, 
level_max bigint,
viplevel_max bigint,
money_rmb decimal(36, 2),
moneyrmb_ac decimal(36, 2),
pay_rank bigint,
login_time bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.tank_cn_w.ads_top_life_di where part_date >= $start_date and part_date <= $end_date;

insert into hive.tank_cn_w.ads_top_life_di
(date, role_id, zone_id, 
channel, install_date, lastlogin_date, lastpay_date, 
level_max, viplevel_max, 
money_rmb, moneyrmb_ac, pay_rank, login_time, 
part_date)

with user_daily as(
select 
date, part_date, role_id, 
level_min, level_max,
viplevel_min, viplevel_max, 
online_time, exchange_rate, 
pay_count, money, money_rmb, web_rmb
from hive.tank_cn_w.dws_user_daily_di 
where part_date >= date_format(date_add('day', -7, date $start_date), '%Y-%m-%d')
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
c.install_date, date(c.lastlogin_ts) as lastlogin_date, date(c.lastpay_ts) as lastpay_date, 
c.firstpay_date, c.firstpay_goodid, c.firstpay_level,
c.zone_id, c.channel, c.os, 
rank() over(partition by a.date order by b.moneyrmb_ac desc) as pay_rank
from user_daily a
left join hive.tank_cn_w.dws_user_daily_derive_di b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.tank_cn_w.dws_user_info_di c
on a.role_id = c.role_id
where c.is_test is null
), 

date_cube as(
select date, part_date, role_id, date_explore
from user_daily_join
cross join unnest(sequence(date_add('day', -6, date), date, interval '1' day)) as t(date_explore)
), 

date_cube_agg as(
select a.date, a.part_date, a.role_id, 
sum(case when b.role_id is not null then 1 else null end) as login_time
from date_cube a
left join user_daily b
on a.date_explore = b.date and a.role_id = b.role_id
group by 1, 2, 3
)

select
a.date, a.role_id, a.zone_id, 
a.channel, a.install_date, a.lastlogin_date, a.lastpay_date, 
a.level_max, a.viplevel_max, 
a.money_rmb, a.moneyrmb_ac, a.pay_rank, b.login_time, a.part_date
from user_daily_join a
left join date_cube_agg b
on a.role_id = b.role_id and a.date = b.date
where pay_rank <= 500
and a.part_date >= $start_date
and a.part_date <= $end_date