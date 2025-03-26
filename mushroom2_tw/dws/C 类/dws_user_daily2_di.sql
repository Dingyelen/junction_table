###
create table if not exists hive.mushroom2_tw_w.dws_user_daily2_di(
date date,
role_id varchar,
android_id varchar, 
gaid varchar, 
device_id varchar, 
device_modelid varchar, 
device_detail varchar, 
money decimal(36, 2),
is_pay bigint, 
summon_num bigint, 
summon_continue bigint, 
summon_corecost bigint, 
summon_list array(varchar), 
normal_count bigint,
normal_win bigint,
adv_count bigint,
adv_win bigint,
ad_click bigint,
ad_skipcount bigint,
ad_success bigint,
ad_revenue decimal(36, 2),
ad_duration bigint,
tech_upgrade bigint, 
equip_upgrade bigint, 
equip_exchange bigint, 
gem_exchange bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.dws_user_daily2_di 
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.dws_user_daily2_di
(date, role_id, 
android_id, gaid, device_id, device_modelid, device_detail, 
money, is_pay, summon_num, summon_continue, summon_corecost, summon_list, 
normal_count, normal_win, adv_count, adv_win, 
ad_click, ad_skipcount, ad_success, ad_revenue, ad_duration, 
tech_upgrade, equip_upgrade, equip_exchange, gem_exchange, 
part_date)
 
with daily_info as(
select date, part_date, role_id, money
from hive.mushroom2_tw_w.dws_user_daily_di
where part_date >= $start_date
and part_date <= $end_date
), 

rolelogin_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
channel, zone_id, alliance_id, app_id, 
vip_level, level, rank_level, power, 
app_id, android_id, gaid, 
device_id, device_modelid, 
ip, country, device_detail
from hive.mushroom2_tw_r.dwd_gserver_rolelogin_live
where part_date >= $start_date
and part_date <= $end_date
), 

rolelogin_first_info as(
select distinct role_id, date, 
first_value(android_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as android_id,
first_value(gaid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as gaid, 
first_value(device_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_id, 
first_value(device_modelid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_modelid, 
first_value(device_detail) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_detail
from rolelogin_log 
), 

summon_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
summon_id, summon_num, core_cost, cost_detail
from hive.mushroom2_tw_r.dwd_gserver_summon_live
where part_date >= $start_date
and part_date <= $end_date
), 

summon_cal01 as(
select date, part_date, role_id, 
sum(summon_num) as summon_num, 
sum(case when summon_num = 10 then 10 else null end) as summon_continue, 
sum(core_cost) as summon_corecost
from summon_log
group by 1, 2, 3
), 

summon_group as(
select date, part_date, role_id, summon_id
from summon_log
group by 1, 2, 3, 4
), 

summon_cal02 as(
select date, part_date, role_id, 
array_agg(summon_id) as summon_list
from summon_group
group by 1, 2, 3
), 

battle_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
event_type, map_id, 
battle_id, step_id, teammate_id, hero_detail, skill_list, 
result, result_detail, duration, monster_end, add_detail, cost_detail
from hive.mushroom2_tw_r.dwd_gserver_battle_live
where part_date >= $start_date
and part_date <= $end_date
), 

battle_cal as(
select date, part_date, role_id, 
sum(case when step_id = 'start' and event_type = '1' then 1 else null end) as normal_count, 
sum(case when step_id = 'end' and event_type = '1' and result = 'success' then 1 else null end) as normal_win,
sum(case when step_id = 'start' and event_type = '3' then 1 else null end) as adv_count,
sum(case when step_id = 'end' and event_type = '3' and result = 'success' then 1 else null end) as adv_win
from battle_log
group by 1, 2, 3
), 

adv_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
icon_id, acv_id, ad_channel, 
result, limit_num, is_skip, 
duration, currency, revenue, add_detail
from hive.mushroom2_tw_r.dwd_gserver_adv_live
where part_date >= $start_date
and part_date <= $end_date
), 

adv_cal as(
select date, part_date, role_id, 
sum(case when result = 'click' then 1 else null end) as ad_click, 
sum(case when result = 'privilege_skip' then 1 else null end) as ad_skipcount,
sum(case when result = 'finish' then 1 else null end) as ad_success,
sum(revenue) as ad_revenue, 
sum(duration) as ad_duration 
from adv_log
group by 1, 2, 3
), 

equip_log as(
select date(event_time) as date, part_date, event_time, 
role_id, event_type, page_id, object_id
from hive.mushroom2_tw_r.dwd_gserver_equip_live
where part_date >= $start_date
and part_date <= $end_date
and event_type in ('equipexchange', 'gemexchange')
), 

equip_cal as(
select date, part_date, role_id, 
sum(case when event_type = 'equipexchange' then 1 else null end) as equip_exchange, 
sum(case when event_type = 'gemexchange' then 1 else null end) as gem_exchange
from equip_log
group by 1, 2, 3
), 

cultivation_log as(
select date(event_time) as date, part_date, event_time, 
role_id, event_type, item_did, object_id, 
upgrade_id, upgrade_step, target_level, target_sublevel, cost_detail
from hive.mushroom2_tw_r.dwd_gserver_cultivation_live
where part_date >= $start_date
and part_date <= $end_date
and upgrade_step = 'complete'
), 

cultivation_cal as(
select date, part_date, role_id, 
sum(case when event_type = 'tech' then 1 else null end) as tech_upgrade, 
sum(case when event_type = 'equipstar' then 1 else null end) as equip_upgrade
from cultivation_log
group by 1, 2, 3
), 

daily_res as(
select a.date, a.part_date, a.role_id, 
d.android_id, d.gaid, d.device_id, d.device_modelid, d.device_detail, 
a.money, (case when a.money > 0 then 1 else null end) as is_pay, 
g.summon_num, g.summon_continue, g.summon_corecost, h.summon_list, 
b.normal_count, b.normal_win, b.adv_count, b.adv_win, 
c.ad_click, c.ad_skipcount, c.ad_success, c.ad_revenue, c.ad_duration, 
f.tech_upgrade, f.equip_upgrade, e.equip_exchange, e.gem_exchange
from daily_info a
left join battle_cal b
on a.role_id = b.role_id and a.date = b.date
left join adv_cal c
on a.role_id = c.role_id and a.date = c.date
left join rolelogin_first_info d
on a.role_id = d.role_id and a.date = d.date
left join equip_cal e
on a.role_id = e.role_id and a.date = e.date 
left join cultivation_cal f
on a.role_id = f.role_id and a.date = f.date 
left join summon_cal01 g
on a.role_id = g.role_id and a.date = g.date 
left join summon_cal02 h
on a.role_id = h.role_id and a.date = h.date 
)

select date, role_id, 
android_id, gaid, device_id, device_modelid, device_detail, 
money, is_pay, summon_num, summon_continue, summon_corecost, summon_list, 
normal_count, normal_win, adv_count, adv_win, 
ad_click, ad_skipcount, ad_success, ad_revenue, ad_duration, 
tech_upgrade, equip_upgrade, equip_exchange, gem_exchange, 
part_date
from daily_res
;
###