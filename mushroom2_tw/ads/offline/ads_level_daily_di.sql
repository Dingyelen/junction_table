###
-- 【基础信息】
-- kpi
create table if not exists hive.mushroom2_tw_w.ads_level_daily_di
(date date,
zone_id varchar,
channel varchar,
level_max_daily bigint,
level_hs bigint, 
level_hist varchar,
users bigint,
pay_users bigint, 
paid_users bigint, 
pay_count_daily bigint, 
money_rmb_daily decimal(36, 2), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.ads_level_daily_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.ads_level_daily_di
(date, zone_id, channel, 
level_max_daily, level_hs, level_hist,
users, pay_users, paid_users, 
pay_count_daily, money_rmb_daily, 
part_date)

with user_daily as(
select 
date, part_date,
role_id, 
level_min as level_min_daily, level_max as level_max_daily,
viplevel_min as viplevel_min_daily, viplevel_max as viplevel_max_daily,
money as money_daily, 
money_rmb as money_rmb_daily, exchange_rate, pay_count as pay_count_daily
from hive.mushroom2_tw_w.dws_user_daily_di 
where part_date >= $start_date
and part_date <= $end_date
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min_daily, a.level_max_daily,
a.viplevel_min_daily, a.viplevel_max_daily,
a.money_daily, a.money_rmb_daily, a.exchange_rate, a.pay_count_daily, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel,
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.mushroom2_tw_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

level_daily as
(select date, part_date, zone_id, channel, 
level_max_daily, (level_max_daily / 5 * 5) as level_hs,
count(distinct role_id) as users, 
count(distinct (case when money_daily > 0 then role_id else null end)) as pay_users,
count(distinct (case when moneyrmb_ac > 0 then role_id else null end)) as paid_users, 
sum(pay_count_daily) as pay_count_daily, 
sum(money_rmb_daily) as money_rmb_daily
from user_daily_join
group by 1, 2, 3, 4, 5, 6
),

level_daily_hist as
(select date, part_date, zone_id, channel,
level_max_daily, level_hs,
concat('[', cast(level_hs as varchar), ',', cast(((level_hs / 5 + 1) * 5) as varchar), ')') as level_hist,
users, pay_users, paid_users, 
pay_count_daily, money_rmb_daily
from level_daily
)

select date, zone_id, channel, 
level_max_daily, level_hs, level_hist,
users, pay_users, paid_users, 
pay_count_daily, money_rmb_daily, 
part_date
from level_daily_hist
;
###·