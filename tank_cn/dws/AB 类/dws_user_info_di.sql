###
create table if not exists hive.tank_cn_w.dws_user_info_di(
role_id varchar,
device_id varchar,
open_id varchar,
adid varchar,
zone_id varchar,
alliance_id varchar,
os varchar, 
channel varchar, 
ip varchar,
country varchar,
network varchar,
campaign varchar,
creative varchar,
adgroup varchar,
campaign_id varchar,
creative_id varchar,
adgroup_id varchar,
adcost varchar,
is_test bigint,
install_ts timestamp(3),
install_date date,
lastlogin_ts timestamp(3),
firstpay_ts timestamp(3),
firstpay_date date,
firstpay_level bigint,
firstpay_goodid varchar,
firstpay_money decimal(36, 2),
lastpay_ts timestamp(3),
lastpay_level bigint,
lastpay_goodid varchar,
lastpay_money decimal(36, 2),
is_paid bigint,
pay_count bigint,
money_ac decimal(36, 2),
moneyrmb_ac decimal(36, 2),
webrmb_ac decimal(36, 2),
sincetimes_gain bigint,
sincetimes_cost bigint,
sincetimes_end bigint,
core_gain bigint,
core_cost bigint,
core_end bigint,
free_gain bigint,
free_cost bigint,
free_end bigint,
paid_gain bigint,
paid_cost bigint,
paid_end bigint,
vip_level bigint,
level bigint,
rank bigint,
power bigint, 
login_days bigint,
login_times bigint,
online_time bigint
)
with(
format = 'ORC',
transactional = true
);

delete from hive.tank_cn_w.dws_user_info_di 
where exists(
select 1
from hive.tank_cn_w.dws_user_daily_di
where dws_user_daily_di.role_id = dws_user_info_di.role_id
and dws_user_daily_di.part_date >= '$start_date'
and dws_user_daily_di.part_date <= '$end_date'
);

insert into hive.tank_cn_w.dws_user_info_di(
role_id, device_id, open_id, adid, 
zone_id, alliance_id, 
os, channel, 
ip, country, 
network, campaign, creative, adgroup, 
campaign_id, creative_id, adgroup_id, 
adcost, is_test, 
install_ts, install_date, 
lastlogin_ts, 
firstpay_ts, firstpay_date, 
firstpay_level, firstpay_goodid, firstpay_money, 
lastpay_ts, lastpay_level, lastpay_goodid, lastpay_money, 
is_paid, pay_count, money_ac, moneyrmb_ac, webrmb_ac, 
sincetimes_gain, sincetimes_cost, sincetimes_end, 
core_gain, core_cost, core_end, 
free_gain, free_cost, free_end, 
paid_gain, paid_cost, paid_end, 
vip_level, level, rank, power, 
login_days, login_times, online_time
)

with user_daily as(
select *
from hive.tank_cn_w.dws_user_daily_di
where role_id in 
(select distinct role_id 
from hive.tank_cn_w.dws_user_daily_di 
where part_date >= $start_date
and  part_date <= $end_date)
), 

user_info as(
select role_id, 
max(is_test) as is_test,
min(first_ts) as install_ts,
max(last_ts) as lastlogin_ts,
max(viplevel_max) as vip_level, 
max(level_max) as level, 
max(rank_max) as rank, 
max(power_max) as power, 
count(*) as login_days, 
sum(online_time) as online_time, 
sum(login_times) as login_times, 
sum(pay_count) as pay_count, 
sum(money) as money_ac, 
sum(money_rmb) as moneyrmb_ac, 
sum(web_rmb) as webrmb_ac, 
sum(sincetimes_gain) as sincetimes_gain, 
sum(sincetimes_cost) as sincetimes_cost, 
sum(core_gain) as core_gain, 
sum(core_cost) as core_cost, 
sum(free_gain) as free_gain, 
sum(free_cost) as free_cost, 
sum(paid_gain) as paid_gain, 
sum(paid_cost) as paid_cost, 
min(firstpay_ts) as firstpay_ts, 
min(firstpay_level) as firstpay_level, 
max(lastpay_ts) as lastpay_ts, 
max(lastpay_level) as lastpay_level
from user_daily 
group by 1
), 

user_first_info as(
select distinct role_id, 
first_value(device_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_id,
first_value(open_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as open_id,
first_value(adid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adid,
first_value(zone_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as zone_id,
last_value(alliance_id) over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as alliance_id,
first_value(os) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as os,
first_value(channel) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as channel,
first_value(ip) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as ip,
first_value(country) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as country,
first_value(network) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as network,
first_value(campaign) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as campaign,
first_value(creative) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as creative,
first_value(adgroup) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adgroup,
first_value(campaign_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as campaign_id,
first_value(creative_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as creative_id,
first_value(adgroup_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adgroup_id, 
first_value(firstpay_goodid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_goodid,
first_value(firstpay_money) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_money,
last_value(lastpay_goodid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as lastpay_goodid,
last_value(lastpay_money) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as lastpay_money,
last_value(sincetimes_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as sincetimes_end,
last_value(core_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as core_end,
last_value(free_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as free_end,
last_value(paid_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as paid_end
from user_daily 
)

select 
a.role_id, coalesce(b.device_id, '') as device_id, coalesce(b.open_id, '') as open_id, coalesce(b.adid, '') as adid, 
coalesce(b.zone_id, '') as zone_id, coalesce(b.alliance_id, '') as alliance_id, coalesce(b.os, '') as os, coalesce(b.channel, '') as channel, 
coalesce(b.ip, '') as ip, coalesce(b.country, '') as country, 
coalesce(b.network, '') as network, coalesce(b.campaign, '') as campaign, coalesce(b.creative, '') as creative, coalesce(b.adgroup, '') as adgroup, 
coalesce(b.campaign_id, '') as campaign_id, coalesce(b.creative_id, '') as creative_id, coalesce(b.adgroup_id, '') as adgroup_id, 
null as adcost, 
a.is_test, 
a.install_ts, 
date(a.install_ts) as install_date, 
a.lastlogin_ts, 
a.firstpay_ts, date(a.firstpay_ts) as firstpay_date, 
a.firstpay_level, b.firstpay_goodid, b.firstpay_money, 
a.lastpay_ts, a.lastpay_level, b.lastpay_goodid, b.lastpay_money, 
(case when money_ac > 0 then 1 else 0 end) as is_paid, 
a.pay_count, a.money_ac, a.moneyrmb_ac, a.webrmb_ac, 
a.sincetimes_gain, a.sincetimes_cost, b.sincetimes_end, 
a.core_gain, a.core_cost, b.core_end, 
a.free_gain, a.free_cost, b.free_end, 
a.paid_gain, a.paid_cost, b.paid_end, 
a.vip_level, a.level, a.rank, a.power, 
a.login_days, a.login_times, a.online_time
from user_info a
left join user_first_info b
on a.role_id = b.role_id;
###