###
create table if not exists hive.mushroom_tw_w.dws_user_daily2_di(
date date,
role_id varchar,
android_id varchar, 
gaid varchar, 
device_id varchar, 
device_modelid varchar, 
device_detail varchar, 
money_rmb decimal(36, 2),
is_pay bigint,
normal_count bigint,
normal_win bigint,
adv_count bigint,
adv_win bigint,
adv_click bigint,
adv_success bigint,
adv_duration bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom_tw_w.dws_user_daily2_di 
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom_tw_w.dws_user_daily2_di
(date, role_id, 
android_id, gaid, device_id, device_modelid, device_detail, 
money_rmb, is_pay, normal_count, normal_win, adv_count, adv_win, 
adv_click, adv_success, adv_duration, 
part_date)
 
with daily_info as(
select date, part_date, role_id, money_rmb
from hive.mushroom_tw_w.dws_user_daily_di
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
from hive.mushroom_tw_r.dwd_gserver_rolelogin_live
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

-- summon_log as(
-- select part_date, event_name, event_time, 
-- date(event_time) as date, 
-- role_id, open_id, adid, 
-- zone_id, alliance_id, 
-- vip_level, level, rank_level, 
-- cast(recruitid as varchar) as summon_id, 
-- count as summon_count, 
-- free as is_free, costid as currency_id, costcount as currency_num
-- from hive.mushroom_tw_r.dwd_gserver_recruitcard_live
-- where part_date >= $start_date
-- and part_date <= $end_date
-- ), 

-- summon_cal as(
-- select date, part_date, role_id, 
-- sum(case when is_free = 1 then summon_count else null end) as summon_free,
-- sum(case when is_free = 0 then summon_count else null end) as summon_valid, 
-- sum(summon_count) as summon_count, 
-- sum(case when summon_count = 10 then 10 else null end) as summon_continue, 
-- sum(case when currency_id = 1 then currency_num else null end) as core_cost
-- from summon_log
-- group by 1, 2, 3
-- ), 

-- summon_group as(
-- select date, part_date, role_id, summon_id
-- from summon_log
-- group by 1, 2, 3, 4
-- ), 

-- summon_cal02 as(
-- select date, part_date, role_id, 
-- array_agg(summon_id) as summon_list
-- from summon_group
-- group by 1, 2, 3
-- ), 

battle_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
event_type, map_id, 
battle_id, step_id, teammate_id, hero_detail, skill_list, 
result, result_detail, duration, monster_end, add_detail, cost_detail
from hive.mushroom_tw_r.dwd_gserver_battle_live
where part_date >= $start_date
and part_date <= $end_date
), 

battle_cal as(
select date, part_date, role_id, 
sum(case when step_id = 'start' and event_type = '1' then 1 else null end) as normal_count, 
sum(case when step_id = 'end' and event_type = '1' and result = 'success' then 1 else null end) as normal_win,
sum(case when step_id = 'start' and event_type = '2' then 1 else null end) as adv_count,
sum(case when step_id = 'end' and event_type = '2' and result = 'success' then 1 else null end) as adv_win
from battle_log
group by 1, 2, 3
), 

adv_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
icon_id, ad_channel, limit_num, 
ad_result, ad_jump, duration, add_detail
from hive.mushroom_tw_r.dwd_gserver_adv_live
where part_date >= $start_date
and part_date <= $end_date
), 

adv_cal as(
select date, part_date, role_id, 
count(*) as adv_click, 
sum(case when ad_result = '1' then 1 else null end) as adv_success,
sum(duration) as adv_duration 
from adv_log
group by 1, 2, 3
), 

daily_res as(
select a.date, a.part_date, a.role_id, 
d.android_id, d.gaid, d.device_id, d.device_modelid, d.device_detail, 
a.money_rmb, (case when a.money_rmb > 0 then 1 else null end) as is_pay, 
b.normal_count, b.normal_win, b.adv_count, b.adv_win, 
c.adv_click, c.adv_success, c.adv_duration
from daily_info a
left join battle_cal b
on a.role_id = b.role_id and a.date = b.date
left join adv_cal c
on a.role_id = c.role_id and a.date = c.date
left join rolelogin_first_info d
on a.role_id = d.role_id and a.date = d.date
)

select date, role_id, 
android_id, gaid, device_id, device_modelid, device_detail, 
money_rmb, is_pay, normal_count, normal_win, adv_count, adv_win, 
adv_click, adv_success, adv_duration, 
part_date
from daily_res
;
###